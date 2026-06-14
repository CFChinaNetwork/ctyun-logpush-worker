/**
 * ─────────────────────────────────────────────────────────────────────────────
 *  ctyun-logpush-worker
 * ─────────────────────────────────────────────────────────────────────────────
 *
 *  作用：
 *    将 Cloudflare Logpush 的 http_requests 原始日志（NDJSON gzip，落 R2）转换为
 *    CDN partner v3.0 的 145 字段格式，gzip 压缩后 POST 推送到客户接收端。
 *
 *  Pipeline：
 *    Logpush → R2 logs/ → R2 Event Notification → parse-queue
 *      → Parser Worker → R2 processed/*.txt
 *      → send-queue → Sender Worker → gzip + auth_key POST → customer endpoint
 *
 *  发送实现（流式管道）：
 *    object.body → CompressionStream('gzip') → fetch body
 *    压缩与 HTTP 发送可流水线化，无需把整个压缩副本缓存到内存。
 *
 *  接收端前提：
 *    ⚠️  必须支持 HTTP/1.1 Transfer-Encoding: chunked（无 Content-Length）。
 *        nginx / ATS / IIS / Caddy 等现代服务器默认都支持。
 *        若接收端只接受 Content-Length 固定长度请求体，会返回 400/411/415。
 *
 *  超时防御：
 *    fetch() 设置 AbortSignal.timeout(60_000)。客户端 hang 时单次 fetch 最多 60s
 *    会被中断 → 抛 AbortError → 外层 catch 触发 msg.retry()。
 *    防止单 invocation 撑满 Queue consumer 的 15 分钟 wall time 上限。
 *
 *  关键机制：
 *    - send-queue 同 batch 内串行 + .done 标记保证 at-least-once 幂等
 *    - Queue send 失败回滚 R2 临时文件
 *    - resp.body.cancel() 防 stalled HTTP response
 *    - delete 失败不抛异常（R2 lifecycle 兜底）
 *    - DLQ 自动重驱动：worker 同时消费 parse-dlq / send-dlq，把死信消息以
 *      指数退避+抖动的延迟回灌到各自源队列，全自动恢复，无需人工/告警。
 *      依赖确定性 batchKey + .done 标记保证重驱动不产生重复（适配无去重接收端）。
 *
 *  Env Secrets : CTYUN_ENDPOINT, CTYUN_PRIVATE_KEY, CTYUN_URI_EDGE
 *  Env Vars    : BATCH_SIZE, LOG_LEVEL, PARSE_QUEUE_NAME, SEND_QUEUE_NAME,
 *                PARSE_DLQ_NAME, SEND_DLQ_NAME, R2_BUCKET_NAME,
 *                FIELD11_SERVER_IP, SEND_PARALLELISM, SEND_FETCH_TIMEOUT_MS,
 *                RETRY_DELAY_SECONDS, REDRIVE_BASE_DELAY_SECONDS,
 *                REDRIVE_MAX_DELAY_SECONDS, REDRIVE_JITTER_SECONDS
 */
'use strict';
// ─── IATA机场三字码 → 国家两字码（CDN节点所在国家，用于#45 country字段）─────────
const IATA_TO_COUNTRY = Object.freeze({
  'HGH':'CN','SHA':'CN','PEK':'CN','PVG':'CN','CAN':'CN','SZX':'CN',
  'CTU':'CN','CKG':'CN','XIY':'CN','WUH':'CN','NKG':'CN','TSN':'CN',
  'TAO':'CN','CGO':'CN','CSX':'CN','HRB':'CN','DLC':'CN','URC':'CN',
  'KMG':'CN','FOC':'CN','HAK':'CN','SHE':'CN','TNA':'CN','XMN':'CN',
  'NNG':'CN','INC':'CN','LHW':'CN','TYN':'CN','CGQ':'CN','HET':'CN',
  'HKG':'HK','MFM':'MO',
  'TPE':'TW','TSA':'TW','KHH':'TW','RMQ':'TW',
  'NRT':'JP','HND':'JP','KIX':'JP','ITM':'JP','NGO':'JP','FUK':'JP',
  'CTS':'JP','OKA':'JP','HIJ':'JP','KOJ':'JP','SDJ':'JP',
  'ICN':'KR','GMP':'KR','PUS':'KR','CJU':'KR','CJJ':'KR',
  'SIN':'SG',
  'KUL':'MY','PEN':'MY','BKI':'MY','KCH':'MY',
  'BKK':'TH','DMK':'TH','HKT':'TH','CNX':'TH',
  'CGK':'ID','DPS':'ID','SUB':'ID','MDC':'ID','UPG':'ID',
  'MNL':'PH','CEB':'PH','DVO':'PH',
  'SGN':'VN','HAN':'VN','DAD':'VN',
  'BOM':'IN','DEL':'IN','MAA':'IN','BLR':'IN','CCU':'IN','HYD':'IN',
  'AMD':'IN','COK':'IN','PNQ':'IN','GAU':'IN','JAI':'IN','LKO':'IN',
  'KHI':'PK','LHE':'PK','ISB':'PK',
  'DAC':'BD','CMB':'LK','RGN':'MM','PNH':'KH','KTM':'NP',
  'SYD':'AU','MEL':'AU','BNE':'AU','PER':'AU','ADL':'AU','CBR':'AU',
  'AKL':'NZ','CHC':'NZ','WLG':'NZ',
  'LAX':'US','SFO':'US','SEA':'US','ORD':'US','DFW':'US','JFK':'US',
  'EWR':'US','MIA':'US','ATL':'US','IAD':'US','DEN':'US','PHX':'US',
  'MSP':'US','DTW':'US','BOS':'US','CLT':'US','LAS':'US','SLC':'US',
  'PDX':'US','SAN':'US','AUS':'US','CMH':'US','IND':'US','MCI':'US',
  'STL':'US','RIC':'US','BUF':'US','HNL':'US','OMA':'US','TUL':'US',
  'OKC':'US','ELP':'US','ABQ':'US','BHM':'US','LIT':'US','GRR':'US',
  'ICT':'US','CID':'US','DSM':'US','FAR':'US','RAP':'US','BIS':'US',
  'SMF':'US','BUR':'US','LGB':'US','ONT':'US','TUS':'US',
  'YYZ':'CA','YVR':'CA','YUL':'CA','YYC':'CA','YEG':'CA','YOW':'CA',
  'MEX':'MX','GDL':'MX','MTY':'MX','CUN':'MX',
  'GRU':'BR','GIG':'BR','SSA':'BR','FOR':'BR','REC':'BR','POA':'BR',
  'EZE':'AR','SCL':'CL','BOG':'CO','MDE':'CO','LIM':'PE',
  'UIO':'EC','CCS':'VE','PTY':'PA','SJO':'CR','GUA':'GT','SDQ':'DO',
  'ASU':'PY','MVD':'UY','VVI':'BO',
  'LHR':'GB','LGW':'GB','MAN':'GB','EDI':'GB','BHX':'GB','STN':'GB',
  'CDG':'FR','ORY':'FR','LYS':'FR','NCE':'FR','MRS':'FR',
  'FRA':'DE','MUC':'DE','DUS':'DE','BER':'DE','HAM':'DE','STR':'DE',
  'CGN':'DE','NUE':'DE','LEJ':'DE',
  'AMS':'NL','BRU':'BE',
  'MAD':'ES','BCN':'ES','VLC':'ES','AGP':'ES','PMI':'ES',
  'LIS':'PT','OPO':'PT',
  'FCO':'IT','MXP':'IT','LIN':'IT','NAP':'IT','VCE':'IT',
  'ZRH':'CH','GVA':'CH','VIE':'AT',
  'WAW':'PL','KRK':'PL','PRG':'CZ','BUD':'HU',
  'OTP':'RO','SOF':'BG','ATH':'GR','SKG':'GR',
  'IST':'TR','SAW':'TR','ESB':'TR','ADB':'TR',
  'TLV':'IL',
  'DXB':'AE','AUH':'AE','SHJ':'AE',
  'RUH':'SA','JED':'SA','DMM':'SA',
  'KWI':'KW','DOH':'QA','BAH':'BH','MCT':'OM','AMM':'JO',
  'CAI':'EG','JNB':'ZA','CPT':'ZA','DUR':'ZA',
  'LOS':'NG','NBO':'KE','ADD':'ET','DAR':'TZ','ACC':'GH','DKR':'SN',
  'CMN':'MA','TUN':'TN','ALG':'DZ',
  'SVO':'RU','DME':'RU','LED':'RU','OVB':'RU','SVX':'RU',
  'KBP':'UA','ARN':'SE','OSL':'NO','CPH':'DK','HEL':'FI',
  'DUB':'IE','KEF':'IS','LUX':'LU',
  'RIX':'LV','VNO':'LT','TLL':'EE',
  'ZAG':'HR','BEG':'RS','BTS':'SK',
  'ALA':'KZ','TAS':'UZ','GYD':'AZ','TBS':'GE','EVN':'AM',
  'MLA':'MT','LCA':'CY','TGD':'ME',
  'GUM':'GU','NAN':'FJ','POM':'PG','MLE':'MV',
  // Additional active POP airport codes verified against Zinc production data (2026-05)
  'AAE':'DZ','ABJ':'CI','ACX':'CN','AGR':'IN','AKX':'KZ','ANC':'US',
  'AQG':'CN','ARI':'CL','ARU':'BR','ASK':'CI','AVA':'CN','BAQ':'CO',
  'BBI':'IN','BDQ':'IN','BEL':'BR','BEY':'LB','BGI':'BB','BGR':'US',
  'BGW':'IQ','BHY':'CN','BNA':'US','BNU':'BR','BOD':'FR','BPE':'CN',
  'BSB':'BR','BSR':'IQ','BWN':'BN','CAW':'BR','CCP':'CL','CFC':'BR',
  'CGB':'BR','CGD':'CN','CGP':'BD','CGY':'PH','CJB':'IN','CLE':'US',
  'CLO':'CO','CNF':'BR','CNN':'IN','COR':'AR','CRK':'PH','CUR':'CW',
  'CWB':'BR','CZL':'DZ','CZX':'CN','DLA':'CM','EBB':'UG','EBL':'IQ',
  'FIH':'CD','FLN':'BR','FRU':'KG','FSD':'US','FUO':'CN','GBE':'BW',
  'GEO':'GY','GND':'GD','GOT':'SE','GYE':'EC','GYN':'BR','HBA':'AU',
  'HFA':'IL','HFE':'CN','HRE':'ZW','HUZ':'CN','HYN':'CN','IAH':'US',
  'ISU':'IQ','ITJ':'BR','IXC':'IN','JAX':'US','JDO':'BR','JHB':'MY',
  'JIB':'DJ','JJN':'CN','JOG':'ID','JOI':'BR','JRG':'IN','JSR':'BD',
  'JUZ':'CN','JXG':'CN','KGL':'RW','KHN':'CN','KHV':'RU','KIN':'JM',
  'KIV':'MD','KJA':'RU','KLD':'RU','KNU':'IN','KWE':'CN','LAD':'AO',
  'LAP':'MX','LJU':'SI','LLK':'AZ','LLW':'MW','LPB':'BO','LUH':'IN',
  'LUN':'ZM','LYA':'CN','MAO':'BR','MBA':'KE','MDL':'MM','MEM':'US',
  'MFE':'US','MGM':'US','MLG':'ID','MPM':'MZ','MRU':'MU','MSQ':'BY',
  'NAG':'IN','NJF':'IQ','NOU':'NC','NQN':'AR','NQZ':'KZ','NTG':'CN',
  'NVT':'BR','ORF':'US','ORK':'IE','ORN':'DZ','OUA':'BF','PAP':'HT',
  'PAT':'IN','PBH':'BT','PBM':'SR','PHL':'US','PIT':'US','PKX':'CN',
  'PMO':'IT','PMW':'BR','POS':'TT','PPT':'PF','QRO':'MX','QWJ':'BR',
  'RAO':'BR','RDU':'US','ROB':'LR','RUN':'RE','SAP':'HN','SAT':'US',
  'SJC':'US','SJK':'BR','SJP':'BR','SJU':'PR','SJW':'CN','SKP':'MK',
  'SOD':'BR','STI':'DO','SUV':'FJ','TEN':'CN','TGU':'HN','TIA':'AL',
  'TLH':'US','TNR':'MG','TPA':'US','TXL':'DE','UDI':'BR','UDR':'IN',
  'ULN':'MN','URT':'TH','VCP':'BR','VIX':'BR','VTE':'LA','WDH':'NA',
  'WDS':'CN','WHU':'CN','WNZ':'CN','WRO':'PL','WUX':'CN','XAP':'BR',
  'XFN':'CN','XNH':'IQ','XNN':'CN','YHZ':'CA','YIH':'CN','YNJ':'CN',
  'YTY':'CN','YWG':'CA','YXE':'CA','ZDM':'PS','ZGN':'CN',
});
function coloToCountry(coloCode) {
  if (coloCode) {
    const c = IATA_TO_COUNTRY[coloCode.toUpperCase()];
    if (c) return c;
  }
  return 'SG';
}
// ─── 常量 ──────────────────────────────────────────────────────────────────
const SEP = '\u0001';
const MONTH_ABBR = Object.freeze([
  'Jan','Feb','Mar','Apr','May','Jun',
  'Jul','Aug','Sep','Oct','Nov','Dec',
]);
const VERSION_EDGE = 'cf_vod_v3.0';
// 字段占位组（严格保证145字段总数）
// 1-45(45) + 46-54(9) + 55(1) + 56-59(4) + 60(1) + 61(1) + 62(1)
// + 63-64(2) + 65-80(16) + 81-95(15) + 96-145(50) = 145
const DASHES_9  = Object.freeze(Array(9).fill('-'));
const DASHES_4  = Object.freeze(Array(4).fill('-'));
const DASHES_2  = Object.freeze(Array(2).fill('-'));
const DASHES_16 = Object.freeze(Array(16).fill('-'));
const DASHES_15 = Object.freeze(Array(15).fill('-'));
const DASHES_50 = Object.freeze(Array(50).fill('-'));
const MAX_URL_LEN = 4096;
const MAX_UA_LEN  = 1024;
const MAX_REF_LEN = 2048;
const BATCH_PREFIX = 'processed/';
const RAW_LOG_PREFIX = 'logs/';
const RAW_LOG_SUFFIX = '.log.gz';
const DEFAULT_BATCH_SIZE = 1000;
const MAX_BATCH_SIZE = 2000;
// 单次 R2 流式读的【空闲超时】默认值（ms）。健康读每几百毫秒出块，永不触发；只有读卡死才触发。
// 与对象大小无关 → 全域统一一个值即可，不需要按域名调。0=不限。
const DEFAULT_PARSE_READ_IDLE_MS = 45000;
// 补扫（reconciliation）默认参数：只兜底「事件通知漏/晚」的对象，正常路径仍是实时事件驱动。
const RECONCILE_DEFAULT_LOOKBACK_MIN = 120; // 回看窗口（分钟）：只考虑最近这段时间落桶的对象
const RECONCILE_DEFAULT_GRACE_SEC    = 120; // 宽限（秒）：落桶不足此时长的对象先交给事件路径，避免与之竞争
const RECONCILE_DEFAULT_MAX_ENQUEUE  = 500; // 单次补扫最多回灌对象数（防一次性回灌过多）
const LOG_LEVELS   = Object.freeze({ debug:0, info:1, warn:2, error:3 });
// ─── 主入口 ────────────────────────────────────────────────────────────────
// 实时性主路径：R2 Event Notification（对新建对象触发）→ queue 消费者，~2min 端到端。
// 兜底安全网：cron 每分钟触发 scheduled() 补扫，捕捉「事件通知漏/晚」的对象（R2 通知 best-effort，
//   实测见过晚 53min）。两者经同一套 .done 幂等去重，互不产生重复发送。
export default {
  async queue(batch, env, ctx) {
    if      (batch.queue === env.PARSE_QUEUE_NAME) await handleParseQueue(batch, env);
    else if (batch.queue === env.SEND_QUEUE_NAME)  await handleSendQueue(batch, env);
    // DLQ 自动重驱动：死信回灌到各自源队列（无人工、无告警）。绑定 binding 沿用现有 producer。
    else if (env.PARSE_DLQ_NAME && batch.queue === env.PARSE_DLQ_NAME) await handleDlqRedrive(batch, env, env.PARSE_QUEUE, env.PARSE_QUEUE_NAME, 'parse');
    else if (env.SEND_DLQ_NAME  && batch.queue === env.SEND_DLQ_NAME)  await handleDlqRedrive(batch, env, env.SEND_QUEUE,  env.SEND_QUEUE_NAME,  'send');
    else throw new Error(`Unknown queue: ${batch.queue}; check PARSE_QUEUE_NAME/SEND_QUEUE_NAME/PARSE_DLQ_NAME/SEND_DLQ_NAME`);
  },
  // Cron（每分钟）触发的【补扫 / reconciliation】——纯事件驱动的兜底安全网。
  // 背景：R2 Event Notification 是 best-effort，瞬时投递失败时由 R2 内部 CronJob 重试，
  //   可能延迟数十分钟（实测见过 53min）才把消息投到 parse-queue → 对象早已落 R2 却长时间无人处理
  //   = 客户侧 gap。事件路径无法自愈这一类（worker 根本没被触发）。
  // 补扫做法：扫描最近窗口内「已落桶超过 grace、却没有源级 .done 标记」的原始对象，回灌 parse-queue。
  // 幂等：源级 .done 早退 + 批次级 .done 保证回灌不会重复发送（见 processFile / writeBatchAndEnqueue）。
  // 安全：handleReconcile 内部全程 try/catch，永不抛出 → 不会出现 scriptThrewException；
  //   ctx.waitUntil 让补扫在响应返回后继续完成。
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(handleReconcile(env));
  },
};

// ─── Parser: R2原始文件 → 流式解析转换 → R2临时文件 → send-queue ───────────
async function handleParseQueue(batch, env) {
  // 单次 invocation 资源控制（关键修复，2026-06 全量实测）：
  // gap 有两类，共性是「单次 invocation 同时扛 batch 内多个文件」，单次资源占用过高：
  //   1) 内存型 OOM（已实测确认）：大行域名(id-upload/update, ~4KB/行)，旧版 batch=3 并行 → 峰值内存
  //      = batch × 多份拷贝(2000 行数组 + join 串 + R2 put body)，多 invocation 撞同 isolate → 超 128MB。
  //   2) 大文件/资源密集型：asia-vcode-od/img/magazine(单文件 7万~10万行)，CPU 重(success cpuP99≈29-31s)，
  //      DLQ 是 6/2-6/5 一次性事件、现已平息；⚠️确切触发未锁定(exceededResources 实测 cpuTime 仅 35-38s，
  //      未撞 60s；日志被采样、metrics 过期)。故 batch=1 是【降风险】(降单次 CPU/内存/子请求至 ~1/3，对任何
  //      资源都增余量)，非已证唯一根因解。两类都【不是并发问题】(大域名 ~200 并发零 OOM)。
  // 本处串行循环是【双保险】：即使将来有人调大 batch_size，单次峰值仍只取决于「同一时刻一个文件」。
  // 吞吐不受影响：invocation 数量由 autoscaler 横向扩（每队列上限 250，远够），大域名实时转发照常。
  // processFile 内部自 ack/retry，循环不因单文件失败中断。
  for (const msg of batch.messages) {
    await processFile(msg, env);
  }
}

// ─── DLQ 自动重驱动消费者 ────────────────────────────────────────────────────
// 设计目标（全自动、无人工、无告警）：
//   进入 parse-dlq / send-dlq 的死信消息不再永久滞留，而是以「指数退避 + 抖动」的延迟
//   回灌到各自源队列（parse-queue / send-queue），交给正常的幂等管线重新处理。
// 为什么安全（不产生重复，适配无去重接收端）：
//   - Parser 的 batchKey 是确定性的（processed/<safeKey>-<index>.txt），Sender 成功后写 .done 标记。
//   - 重驱动 → 重解析 → 已发送批次撞 .done 跳过 → 只补发从未发出的尾部 → 零重复。
//   - 前提：BATCH_SIZE 不变（否则 index→行区间 错位会破坏 .done 匹配）。修改 BATCH_SIZE 须先清空 DLQ。
// 行为：
//   - 瞬时型失败（并发 OOM、端点抖动）：延迟后再试通常即成功，自愈。
//   - 真正的「毒消息」：__redrive 计数驱动退避逐次拉长（封顶 REDRIVE_MAX_DELAY_SECONDS），
//     退化为低频无害循环，不烧资源、不需人工介入。
//   - 抖动避免一次性大量死信同时回灌造成 thundering herd。
// 普适：随模板部署到所有域名后，自动清空各自的 DLQ，无需逐域名打补丁。
async function handleDlqRedrive(batch, env, targetQueue, targetName, label) {
  // 绑定缺失时（误配置）：不丢消息，整批重试，等待修复
  if (!targetQueue) {
    log(env, 'error', `[REDRIVE:${label}] target queue binding missing; retrying batch`);
    for (const m of batch.messages) m.retry({ delaySeconds: retryDelaySeconds(env) });
    return;
  }
  const base = parseIntegerVar(env, 'REDRIVE_BASE_DELAY_SECONDS', 60, 0, 86400);
  const max  = parseIntegerVar(env, 'REDRIVE_MAX_DELAY_SECONDS', 3600, 0, 86400);
  const jit  = parseIntegerVar(env, 'REDRIVE_JITTER_SECONDS', 30, 0, 3600);
  for (const msg of batch.messages) {
    try {
      // 保留原始消息体（Parser 读 body.object.key；Sender 读 body.key），仅附加 __redrive 计数
      const orig = (msg.body && typeof msg.body === 'object') ? msg.body : { __rawBody: msg.body };
      const attempt = (Number(orig.__redrive) || 0) + 1;
      // 指数退避（封顶）+ 抖动；delaySeconds 上限 86400（24h）
      const backoff = Math.min(base * Math.pow(2, attempt - 1), max);
      const delaySeconds = Math.min(backoff + Math.floor(Math.random() * (jit + 1)), 86400);
      await targetQueue.send({ ...orig, __redrive: attempt }, { delaySeconds });
      msg.ack();
      log(env, 'info', `[REDRIVE:${label}] re-enqueued → ${targetName} (attempt #${attempt}, delay ${delaySeconds}s)`);
    } catch (e) {
      // 回灌失败（源队列暂时不可用等）：重试该死信消息，绝不丢弃
      log(env, 'warn', `[REDRIVE:${label}] re-enqueue failed, will retry: ${e.message || e}`);
      msg.retry({ delaySeconds: retryDelaySeconds(env) });
    }
  }
}

// 失败重试延迟（秒）：仅延迟「失败的少数」的重试，不影响成功路径的实时性。
// 用于 parser/sender 的 msg.retry({delaySeconds})，给瞬时型故障（并发压力、端点抖动）恢复时间。
function retryDelaySeconds(env) {
  return parseIntegerVar(env, 'RETRY_DELAY_SECONDS', 30, 0, 43200);
}

// ─── 补扫 / reconciliation（cron 每分钟触发）────────────────────────────────────
// 列出最近窗口内的原始对象与源级 .done 标记，挑出「落桶超过 grace 却没有 .done」的对象，回灌 parse-queue。
// 全程 try/catch，永不抛出（避免 scheduled scriptThrewException）。
async function handleReconcile(env) {
  try {
    if (!env.PARSE_QUEUE) { log(env, 'error', '[RECONCILE] PARSE_QUEUE binding missing; skip'); return; }
    const lookbackMs = parseIntegerVar(env, 'RECONCILE_LOOKBACK_MINUTES', RECONCILE_DEFAULT_LOOKBACK_MIN, 1, 1440) * 60000;
    const graceMs    = parseIntegerVar(env, 'RECONCILE_GRACE_SECONDS',   RECONCILE_DEFAULT_GRACE_SEC,    30, 3600) * 1000;
    const maxEnqueue = parseIntegerVar(env, 'RECONCILE_MAX_ENQUEUE',     RECONCILE_DEFAULT_MAX_ENQUEUE,  1, 10000);
    const now = Date.now();
    const rawSuffix  = env?.RAW_LOG_SUFFIX || RAW_LOG_SUFFIX;
    const doneSuffix = `${rawSuffix}.done`;
    const prefixes = genDayPrefixes(now - lookbackMs, now, env);

    const rawUploaded = new Map(); // rawKey -> uploadedMs
    const doneSet     = new Set(); // rawKey（去掉 .done 后缀）
    for (const prefix of prefixes) {
      let cursor;
      do {
        const page = await env.RAW_BUCKET.list({ prefix, limit: 1000, cursor });
        for (const o of (page.objects || [])) {
          const k = o.key;
          const up = o.uploaded ? new Date(o.uploaded).getTime() : 0;
          if (k.endsWith(doneSuffix)) {
            doneSet.add(k.slice(0, -('.done'.length))); // 源级完成标记
          } else if (isRawLogKey(k, env)) {
            rawUploaded.set(k, up);                       // 原始 Logpush 对象
          }
          // 其余（processed/、running marker 等）忽略
        }
        cursor = page.truncated ? page.cursor : undefined;
      } while (cursor);
    }

    const candidates = selectReconcileCandidates(rawUploaded, doneSet, now, lookbackMs, graceMs, maxEnqueue);
    if (candidates.length === 0) { log(env, 'debug', '[RECONCILE] no stale objects'); return; }
    log(env, 'warn', `[RECONCILE] re-enqueueing ${candidates.length} stale object(s) (lookback=${lookbackMs / 60000}min grace=${graceMs / 1000}s, scanned=${rawUploaded.size})`);

    const bucketName = env.R2_BUCKET_NAME || 'cdn-logs-raw';
    for (let i = 0; i < candidates.length; i += 100) { // Queue sendBatch 上限 100
      const batch = candidates.slice(i, i + 100).map((key) => ({
        body: { bucket: bucketName, object: { key }, __reconcile: true },
      }));
      try {
        await env.PARSE_QUEUE.sendBatch(batch);
      } catch (e) {
        log(env, 'warn', `[RECONCILE] enqueue batch failed (${batch.length} msgs): ${e.message || e}`);
      }
    }
  } catch (e) {
    // 补扫失败必须非致命：退回纯事件驱动（gap 可能回来但不新增丢失），下一次 cron 再试。
    log(env, 'error', `[RECONCILE] non-fatal error: ${e?.message || e}`);
  }
}

// 纯函数（便于单测）：从「原始对象 uploaded 时间表 + 已完成集合」挑出需要回灌的对象。
//   - 跳过已有源级 .done 的（已处理）
//   - 跳过落桶不足 grace 的（太新，先让事件路径处理，避免竞争重复）
//   - 跳过落桶早于 lookback 的（窗口外，避免无限重试坏对象）
//   - uploaded 缺失(=0) 的对象视为需要处理（保守：宁可重投也不漏）
function selectReconcileCandidates(rawUploaded, doneSet, now, lookbackMs, graceMs, maxEnqueue) {
  const out = [];
  const freshAfter = now - graceMs;   // uploaded 晚于此 = 太新
  const windowStart = now - lookbackMs; // uploaded 早于此 = 窗口外
  for (const [key, up] of rawUploaded) {
    if (doneSet.has(key)) continue;
    if (up && up > freshAfter) continue;
    if (up && up < windowStart) continue;
    out.push(key);
    if (out.length >= maxEnqueue) break;
  }
  return out;
}

// 生成补扫所需的按 UTC 日期 prefix 列表（logs/YYYYMMDD/）。lookback ≤ 24h，故最多跨 2~3 个 UTC 日。
function genDayPrefixes(startMs, endMs, env) {
  const base = env?.RAW_LOG_PREFIX || RAW_LOG_PREFIX;
  const prefixBase = base.endsWith('/') ? base : `${base}/`;
  const prefixes = [];
  const d = new Date(startMs); d.setUTCHours(0, 0, 0, 0);
  const end = new Date(endMs); end.setUTCHours(0, 0, 0, 0);
  let iter = 0;
  while (d.getTime() <= end.getTime() && iter++ < 3) {
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    prefixes.push(`${prefixBase}${yyyy}${mm}${dd}/`);
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return prefixes;
}

async function processFile(msg, env) {
  const key = msg.body?.object?.key;
  if (!key) {
    log(env, 'warn', `No object.key: ${JSON.stringify(msg.body)}`);
    msg.ack();
    return;
  }

  if (!isRawLogKey(key, env)) {
    log(env, 'warn', `Skipped non-raw-log R2 object: ${key}`);
    msg.ack();
    return;
  }

  // 源级幂等早退：若该原始对象已被完整处理过（写过源级 .done 标记），直接跳过。
  // 应对「补扫回灌」与「迟到数十分钟的事件通知」对同一对象的重复投递 —— 避免无谓重解析，
  // 并把潜在并发重发窗口压到最小（批次级 .done 仍是不重发的最终保证）。
  const sourceDoneKey = `${key}.done`;
  if (await env.RAW_BUCKET.head(sourceDoneKey).catch(() => null)) {
    log(env, 'info', `Source already processed (skip): ${key}`);
    msg.ack();
    return;
  }

  log(env, 'info', `Parsing: ${key}`);
  try {
    const object = await env.RAW_BUCKET.get(key);
    if (!object) { log(env, 'warn', `Not in R2: ${key}`); msg.ack(); return; }
    const batchSize = parseIntegerVar(env, 'BATCH_SIZE', DEFAULT_BATCH_SIZE, 1, MAX_BATCH_SIZE);
    // PARSE_READ_IDLE_TIMEOUT_MS：给【每一次 R2 read()】设空闲超时（非整文件总时限）。健康读每几百毫秒
    // 出块，永不触发；只有单次读卡死（连接挂住，实测纯 I/O 等待 900~1922s）才触发 → 抛错 → 下方 catch 对
    // 【这一个文件】msg.retry（只重试该文件、不挡同批后续、不丢数据）。阈值与对象大小无关 → 全域统一一个值、
    // 不会误杀大文件。默认 45000ms；0=不限。卡住文件不再堵住队列（06-12 那种级联的根因修复）。
    const idleTimeoutMs = parseIntegerVar(env, 'PARSE_READ_IDLE_TIMEOUT_MS', DEFAULT_PARSE_READ_IDLE_MS, 0, 600000);
    let lines = [], batchIdx = 0, lineCount = 0, errCount = 0, parseErrCount = 0;
    await streamParseNdjsonGzip(object.body, async (record) => {
      lineCount++;
      try {
        lines.push(transformEdge(record, env));
      } catch (e) {
        errCount++;
        log(env, 'warn', `Transform err line ${lineCount}: ${e.message}`);
        return;
      }
      if (lines.length >= batchSize) {
        await writeBatchAndEnqueue(lines, key, batchIdx++, env);
        lines = [];
      }
    }, (line) => {
      parseErrCount++;
      if (parseErrCount <= 5) log(env, 'warn', `JSON parse failed in ${key}: ${line.substring(0, 100)}`);
    }, idleTimeoutMs);
    if (lineCount === 0 && parseErrCount > 0) throw new Error(`No valid JSON records in ${key}; parseErrors=${parseErrCount}`);
    if (lines.length > 0) await writeBatchAndEnqueue(lines, key, batchIdx++, env);
    // 写源级完成标记（供补扫判定「已处理」+ 重复投递早退）。复用 writeDoneMarker：写 `${key}.done`。
    // 失败仅告警不抛（批次级 .done 仍保证不重发；下次补扫会再处理一次，靠 .done 去重）。
    await writeDoneMarker(env, key);
    log(env, 'info', `Done: ${key} | lines=${lineCount} batches=${batchIdx} errors=${errCount} parseErrors=${parseErrCount}`);
    msg.ack();
  } catch (err) {
    log(env, 'error', `Failed: ${key}: ${err.message}`);
    // 延迟重试：给「并发型瞬时 OOM / R2 抖动」等留出恢复时间；耗尽 max_retries 后进 parse-dlq，
    // 再由 handleDlqRedrive 自动延迟回灌，最终自愈（不会永久丢失）。
    msg.retry({ delaySeconds: retryDelaySeconds(env) });
  }
}
async function writeBatchAndEnqueue(lines, sourceKey, index, env) {
  const safeKey  = sourceKey.replace(/[^a-zA-Z0-9_-]/g, '_');
  // 确定性 batchKey（不带时间戳）：Parser 重试时同一个 (sourceKey, index) 始终对应同一个文件
  const batchKey = `${BATCH_PREFIX}${safeKey}-${index}.txt`;
  // 幂等检查：如果该 batch 已被 Sender 成功发送过（存在 .done 标记），直接跳过
  // 这避免了 Parser 中途失败重试时，已发送的 batch 被重复发送，导致数据翻倍
  const doneMarker = await env.RAW_BUCKET.head(`${batchKey}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'debug', `Batch already sent (skip): ${batchKey}`);
    return;
  }
  const body = lines.join('\n') + '\n';
  await env.RAW_BUCKET.put(batchKey, body, {
    httpMetadata: { contentType: 'text/plain; charset=utf-8' },
  });
  try {
    await env.SEND_QUEUE.send({ key: batchKey });
  } catch (e) {
    // Queue入队失败，立即回滚删除R2临时文件
    // 避免产生无人处理的孤立文件，让parse-queue的retry机制干净地重新处理
    await env.RAW_BUCKET.delete(batchKey).catch(() => {});
    throw e;
  }
  log(env, 'debug', `Queued: ${batchKey} (${lines.length} lines)`);
}
// ─── Sender: R2临时文件 → Gzip → MD5鉴权 → POST to customer endpoint → 删除临时文件 ──────
async function handleSendQueue(batch, env) {
  // 小规模并行发送（可控并发池），放大单次 invocation 的有效并发。
  // 默认串行（SEND_PARALLELISM 未设置或为 1），推荐设置为 2~3。
  const pRaw = env?.SEND_PARALLELISM;
  const pool = (() => {
    const n = Number(pRaw);
    if (!Number.isFinite(n) || n < 1) return 1;
    // 上限做个软限制，防止误填过大值导致客户侧被压垮
    return Math.min(Math.floor(n), 8);
  })();

  // 任务分配游标（单线程 JS，原子性足够；无需锁）
  let cursor = 0;
  const total = batch.messages.length;

  const worker = async () => {
    while (true) {
      const i = cursor++;
      if (i >= total) break;
      const msg = batch.messages[i];
      try {
        await sendBatch(msg, env);
        msg.ack();
      } catch (err) {
        log(env, 'warn', `Send failed, retry: ${err.message || err}`);
        // 延迟重试：端点慢/抖动时退避；耗尽 max_retries 后进 send-dlq，再由 handleDlqRedrive 自动回灌。
        msg.retry({ delaySeconds: retryDelaySeconds(env) });
      }
    }
  };

  // 并发启动 pool 个 worker，跑完整个 batch。
  const runners = Array.from({ length: pool }, () => worker());
  await Promise.allSettled(runners);
}
async function sendBatch(msg, env) {
  const { key } = msg.body || {};
  if (!key) throw new Error(`Invalid message: ${JSON.stringify(msg.body)}`);
  await sendBatchUnlocked(key, env);
}
async function sendBatchUnlocked(key, env) {
  // 幂等检查：如果已存在 .done 标记，说明该 batch 曾成功发送过（Queue 重复投递场景）
  // 直接静默 ack，避免重复发送导致数据翻倍
  const doneMarker = await env.RAW_BUCKET.head(`${key}.done`).catch(() => null);
  if (doneMarker) {
    log(env, 'info', `Already sent (skip duplicate): ${key}`);
    return;
  }
  const object = await env.RAW_BUCKET.get(key);
  if (!object) { log(env, 'warn', `Batch not found (may be sent or rolled back): ${key}`); return; }
  const uri        = env.CTYUN_URI_EDGE;
  const endpoint   = env.CTYUN_ENDPOINT;
  const privateKey = env.CTYUN_PRIVATE_KEY;
  if (!endpoint || !privateKey || !uri) throw new Error('Missing CTYUN_ENDPOINT, CTYUN_PRIVATE_KEY or CTYUN_URI_EDGE');

  // 流式管道：R2 stream → CompressionStream(gzip) → fetch body
  //   - 压缩与 HTTP 发送可流水线化，无 ArrayBuffer 中间缓冲
  //   - 内存占用约 ~10 KB/请求（只有 stream 小缓冲）
  //
  // 接收端要求：
  //   HTTP/1.1 Transfer-Encoding: chunked（请求体无 Content-Length）。
  //   现代服务器（nginx / ATS / IIS / Caddy）默认支持；若收到 400/411/415，
  //   说明接收端不支持 chunked 请求体，需协调客户端启用。
  const compressedStream = object.body.pipeThrough(new CompressionStream('gzip'));

  // 单次发送 fetch 超时（默认 120s，可配 1000–120000ms）：仅用于 abort 完全 hang 的接收端 → 外层 catch
  // → msg.retry()。实测 invocation P999≈82s，远低于 15min wall 上限，120s 安全且给慢接收端留足余量。
  const sendTimeoutMs = parseIntegerVar(env, 'SEND_FETCH_TIMEOUT_MS', 120000, 1000, 120000);
  const fetchInit = {
    method: 'POST',
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
      'Content-Encoding': 'gzip',
    },
    body: compressedStream,
    signal: AbortSignal.timeout(sendTimeoutMs),
  };
  const resp = await fetch(buildAuthUrl(endpoint, uri, privateKey), fetchInit);
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`HTTP ${resp.status} ${resp.statusText} | ${text.substring(0, 200)}`);
  }
  // 必须消费 response body，否则并发场景下 CF 会触发 "stalled HTTP response" 保护
  await resp.body?.cancel().catch(() => {});
  log(env, 'info', `Sent ${object.size ?? '?'} bytes (uncompressed) → HTTP ${resp.status} | ${key}`);
  // 先写入幂等标记，确保即使后续 delete 失败，重复消息也能被识别
  await writeDoneMarker(env, key);
  // delete 失败不能触发重发（会导致翻倍），只记警告，R2 lifecycle 会兜底清理
  await env.RAW_BUCKET.delete(key).catch((e) => {
    log(env, 'warn', `Delete failed (will be cleaned by lifecycle): ${key}: ${e.message}`);
  });
  log(env, 'debug', `Deleted: ${key}`);
}
// ─── 流式解析: gzip ndjson → 逐行回调 ─────────────────────────────────────
async function streamParseNdjsonGzip(inputStream, onRecord, onParseError, idleTimeoutMs = 0) {
  const reader  = inputStream.pipeThrough(new DecompressionStream('gzip')).getReader();
  const decoder = new TextDecoder('utf-8');
  let   buffer  = '';
  // idleTimeoutMs>0：给【每一次 reader.read()】设空闲超时（不是整文件总时限）。
  // 健康的 R2 流每几百毫秒就产出一块数据，永远不会触发；只有「单次读卡死」（连接挂住，
  // 实测纯 I/O 等待 900~1922s）才会触发 → 抛错 → 上层对该文件 msg.retry。
  // 关键：阈值与对象大小【无关】（大文件也是连续出块），可全域统一一个值、不会误杀大文件。
  // 每次循环重新计时（空闲口径），区别于旧版「整文件总时限」需按域名调。0=不限（旧默认行为）。
  try {
    while (true) {
      let res;
      if (idleTimeoutMs > 0) {
        const rp = reader.read();
        rp.catch(() => {});   // 若超时赢得 race，吞掉 read() 的滞后 rejection，避免 unhandled rejection
        let timer;
        try {
          res = await Promise.race([
            rp,
            new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Parse read idle timeout after ${idleTimeoutMs}ms`)), idleTimeoutMs); }),
          ]);
        } finally { clearTimeout(timer); }
      } else {
        res = await reader.read();
      }
      const { done, value } = res;
      if (done) {
        const last = buffer.trim();
        if (last) await tryParse(last, onRecord, onParseError);
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        const t = line.trim();
        if (t) await tryParse(t, onRecord, onParseError);
      }
    }
  } finally {
    // cancel() 释放锁并（超时时）中断底层 R2 流；正常读完时为无害 no-op。不 await，避免 finally 卡住。
    reader.cancel().catch(() => {});
  }
}
async function tryParse(line, onRecord, onParseError) {
  try { await onRecord(JSON.parse(line)); }
  catch (e) { onParseError?.(line, e); }
}
// ─── 格式转换: CF http_requests → CDN partner log format v3.0（145字段）─────────
//
// 字段说明:
//   #11 server_ip:         FIELD11_SERVER_IP（wrangler 填入该域名的解析/anycast IP，如 172.65.90.64），必填。
//                          ⚠️不取 EdgeServerIP：按 CF 文档它是「边缘→源站」的内部 IP（仅回源请求才有值、
//                          cache 命中为空），并非客户 server_ip($server_addr) 语义，会污染该字段。
//   #6  request_time:      (EdgeEndTimestamp - EdgeStartTimestamp) / 1000
//   #7  rwt_time:          OriginResponseHeaderReceiveDurationMs / 1000
//   #8  wwt_time:          OriginRequestHeaderSendDurationMs / 1000
//   #9  fbt_time:          EdgeTimeToFirstByteMs / 1000，秒格式 0.999
//   #10 finalize_error:    nginx/ATS特有字段，CF无对应，固定'-'
//   #12 server_port:       ClientRequestScheme→https:443 / http:80
//   #19 server_protocol:   ClientRequestProtocol完整值，如 HTTP/1.1
//   #21 sent_http_content_length: ResponseHeaders['content-length']（需配置Custom Fields），否则'-'
//   #27 cache_status:      CacheCacheStatus: hit/stale/revalidated/updating→HIT, miss/expired/bypass/dynamic/none→MISS
//   #28 cache_status2:     同#27
//   #36 http_x_forwarded_for: CF无XFF header，用ClientIP近似
//   #42 dysta:             CacheCacheStatus: hit→static, dynamic→dynamic, 其他→-
//   #44 ssl_connect_time:  OriginTLSHandshakeDurationMs / 1000
//   #45 country:           EdgeColoCode→IATA映射→国家码，未命中→SG
//   #55 request_start_time: 无方括号的北京时间
//   #60 servername:        ClientRequestHost
//   #62 ssl_protocol:      ClientSSLProtocol
function sf(val, maxLen) {
  if (val == null || val === '') return '-';
  const s = String(val).replace(/[\u0000-\u001f\u007f]/g, ' ');
  if (s.trim() === '') return '-';
  return (maxLen && s.length > maxLen) ? s.substring(0, maxLen) : s;
}
function transformEdge(r, env) {
  return [
    /* 1  */ VERSION_EDGE,
    /* 2  */ fmtTimeLocal(r.EdgeStartTimestamp),
    /* 3  */ sf(r.RayID),
    /* 4  */ sf(r.EdgeResponseStatus),
    /* 5  */ fmtMsec(r.EdgeStartTimestamp),
    /* 6  */ fmtDurationSec(r.EdgeStartTimestamp, r.EdgeEndTimestamp),
    /* 7  */ fmtSec(r.OriginResponseHeaderReceiveDurationMs),
    /* 8  */ fmtSec(r.OriginRequestHeaderSendDurationMs),
    /* 9  */ fmtSec(r.EdgeTimeToFirstByteMs),
    /* 10 */ finalizeErrorCode(r),
    /* 11 */ sf(env.FIELD11_SERVER_IP),
    /* 12 */ schemeToPort(r.ClientRequestScheme),
    /* 13 */ sf(r.ClientIP),
    /* 14 */ sf(r.ClientSrcPort),
    /* 15 */ sf(r.ClientRequestMethod),
    /* 16 */ sf(r.ClientRequestScheme),
    /* 17 */ sf(r.ClientRequestHost),
    /* 18 */ sf(buildFullUrl(r), MAX_URL_LEN),
    /* 19 */ sf(r.ClientRequestProtocol),
    /* 20 */ sf(r.ClientRequestBytes),
    /* 21 */ responseContentLength(r),
    /* 22 */ sf(r.EdgeResponseBytes),
    /* 23 */ sf(r.EdgeResponseBodyBytes),
    /* 24 */ sf(r.OriginIP),
    /* 25 */ sf(r.OriginResponseStatus),
    /* 26 */ fmtSec(r.OriginResponseDurationMs),
    /* 27 */ mapCache(r.CacheCacheStatus),
    /* 28 */ mapCache(r.CacheCacheStatus),
    /* 29 */ sf(r.OriginIP),
    /* 30 */ sf(r.OriginResponseStatus),
    /* 31 */ '-',
    /* 32 */ '-',
    /* 33 */ sf(r.EdgeResponseContentType),
    /* 34 */ sf(r.ClientRequestReferer, MAX_REF_LEN),
    /* 35 */ sf(r.ClientRequestUserAgent, MAX_UA_LEN),
    /* 36 */ sf(r.ClientIP),
    /* 37 */ '-',
    /* 38 */ '-',
    /* 39 */ '-',
    /* 40 */ sf(r.ClientIP),
    /* 41 */ '-',
    /* 42 */ mapDysta(r.CacheCacheStatus),
    /* 43 */ '-',
    /* 44 */ fmtSec(r.OriginTLSHandshakeDurationMs),
    /* 45 */ coloToCountry(r.EdgeColoCode),
    /* 46-54 */ ...DASHES_9,
    /* 55 */ fmtTimeLocalSimple(r.EdgeStartTimestamp),
    /* 56 */ '-',
    /* 57 */ '-',
    /* 58 */ '2cee6ba6ff8247a385902ddf5686df0c',
    /* 59 */ '-',
    /* 60 */ sf(r.ClientRequestHost),
    /* 61 */ '-',
    /* 62 */ sf(r.ClientSSLProtocol),
    /* 63-64 */ ...DASHES_2,
    /* 65-80 */ ...DASHES_16,
    /* 81-95 */ ...DASHES_15,
    /* 96-145 */ ...DASHES_50,
  ].join(SEP);
}
// ─── 鉴权: auth_key={ts}-{rand}-md5({uri}-{ts}-{rand}-{key}) ──────────────
function buildAuthUrl(endpoint, uri, privateKey) {
  const ts   = Math.floor(Date.now() / 1000) + 300;
  const rand = Math.floor(Math.random() * 99999);
  const base = endpoint.endsWith('/') && uri.startsWith('/') ? endpoint.slice(0, -1) : endpoint;
  const path = !endpoint.endsWith('/') && !uri.startsWith('/') ? `/${uri}` : uri;
  const target = `${base}${path}`;
  const sep = target.includes('?') ? '&' : '?';
  return `${target}${sep}auth_key=${ts}-${rand}-${md5(`${uri}-${ts}-${rand}-${privateKey}`)}`;
}
// ─── 工具函数 ──────────────────────────────────────────────────────────────
// 兼容秒整数、毫秒整数、RFC3339字符串三种时间戳格式
function parseTimestamp(ts) {
  if (ts == null) return null;
  if (typeof ts === 'number') return ts > 1e12 ? ts : ts * 1000;
  if (typeof ts === 'string') {
    const n = Number(ts);
    if (!isNaN(n) && n > 0) return n > 1e12 ? n : n * 1000;
    const d = new Date(ts).getTime();
    return isNaN(d) ? null : d;
  }
  return null;
}
function fmtTimeLocal(ts) {
  const ms = parseTimestamp(ts);
  if (ms == null) return '-';
  const d  = new Date(ms + 8 * 3600 * 1000);
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const mo = MONTH_ABBR[d.getUTCMonth()];
  const yy = d.getUTCFullYear();
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mi = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `[${dd}/${mo}/${yy}:${hh}:${mi}:${ss} +0800]`;
}
function fmtTimeLocalSimple(ts) {
  const s = fmtTimeLocal(ts);
  return s === '-' ? '-' : s.slice(1, -1);
}
function fmtMsec(ts) {
  const ms = parseTimestamp(ts);
  if (ms == null) return '-';
  return `${Math.floor(ms / 1000)}.${String(Math.floor(ms) % 1000).padStart(3, '0')}`;
}
function fmtSec(ms) {
  if (ms == null) return '-';
  return (ms / 1000).toFixed(3);
}
function fmtDurationSec(startTs, endTs) {
  const startMs = parseTimestamp(startTs);
  const endMs = parseTimestamp(endTs);
  if (startMs == null || endMs == null || endMs < startMs) return '-';
  return ((endMs - startMs) / 1000).toFixed(3);
}
function buildFullUrl(r) {
  return `${r.ClientRequestScheme || 'http'}://${r.ClientRequestHost || ''}${r.ClientRequestURI || '/'}`;
}
function schemeToPort(scheme) {
  if (!scheme) return '-';
  return scheme.toLowerCase() === 'https' ? '443' : '80';
}

function isRawLogKey(key, env) {
  const prefix = env?.RAW_LOG_PREFIX || RAW_LOG_PREFIX;
  const suffix = env?.RAW_LOG_SUFFIX || RAW_LOG_SUFFIX;
  return typeof key === 'string' && key.startsWith(prefix) && key.endsWith(suffix);
}

function parseIntegerVar(env, name, defaultValue, min, max) {
  const raw = env?.[name];
  if (raw == null || raw === '') return defaultValue;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < min || n > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}; got "${raw}"`);
  }
  return n;
}

async function writeDoneMarker(env, key) {
  let lastErr;
  for (let i = 0; i < 3; i++) {
    try {
      await env.RAW_BUCKET.put(`${key}.done`, '1', {
        httpMetadata: { contentType: 'text/plain' },
      });
      return;
    } catch (e) {
      lastErr = e;
      await sleep(100 * (i + 1));
    }
  }
  log(env, 'error', `Done marker write failed after retries: ${key}: ${lastErr?.message || lastErr}`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// #10 finalize_error_code: 该字段为nginx/ATS架构特有的连接中断错误码
// CF边缘架构无对应字段，无法准确映射，固定返回'-'
function finalizeErrorCode(r) {
  return '-';
}

// #21 sent_http_content_length: 响应头Content-Length
// 需配置Logpush Custom Fields捕获ResponseHeaders，否则无数据返回'-'
function responseContentLength(r) {
  if (r.ResponseHeaders && r.ResponseHeaders['content-length']) {
    return sf(r.ResponseHeaders['content-length']);
  }
  return '-';
}
function mapCache(s) {
  if (!s) return '-';
  const l = s.toLowerCase();
  // CF CacheCacheStatus values that represent a cache hit
  if (['hit','stale','revalidated','updating'].includes(l)) return 'HIT';
  // CF CacheCacheStatus values that represent a cache miss
  if (['miss','expired','bypass','dynamic','none'].includes(l)) return 'MISS';
  return '-';
}
function mapDysta(s) {
  if (!s) return '-';
  const l = s.toLowerCase();
  return l === 'hit' ? 'static' : l === 'dynamic' ? 'dynamic' : '-';
}
function log(env, level, msg) {
  if ((LOG_LEVELS[level] ?? 1) >= (LOG_LEVELS[env?.LOG_LEVEL] ?? 1)) {
    const fn = level === 'error' ? console.error : level === 'warn' ? console.warn : console.log;
    fn(`[${level.toUpperCase()}] ${new Date().toISOString()} ${msg}`);
  }
}
// ─── MD5 (RFC 1321, Workers SubtleCrypto不支持MD5) ─────────────────────────
function md5(str) {
  const add = (x,y)=>{const l=(x&0xffff)+(y&0xffff);return(((x>>16)+(y>>16)+(l>>16))<<16)|(l&0xffff);};
  const rol  = (n,c)=>(n<<c)|(n>>>(32-c));
  const cmn  = (q,a,b,x,s,t)=>add(rol(add(add(a,q),add(x,t)),s),b);
  const ff   = (a,b,c,d,x,s,t)=>cmn((b&c)|(~b&d),a,b,x,s,t);
  const gg   = (a,b,c,d,x,s,t)=>cmn((b&d)|(c&~d),a,b,x,s,t);
  const hh   = (a,b,c,d,x,s,t)=>cmn(b^c^d,a,b,x,s,t);
  const ii   = (a,b,c,d,x,s,t)=>cmn(c^(b|~d),a,b,x,s,t);
  const utf8 = unescape(encodeURIComponent(str));
  const len  = utf8.length;
  const nb   = ((len+8)>>>6)+1;
  const blk  = new Array(nb*16).fill(0);
  for(let i=0;i<len;i++) blk[i>>2]|=utf8.charCodeAt(i)<<(i%4*8);
  blk[len>>2]|=0x80<<(len%4*8);
  blk[nb*16-2]=len*8;
  let a=1732584193,b=-271733879,c=-1732584194,d=271733878;
  for(let i=0;i<blk.length;i+=16){
    const[pa,pb,pc,pd]=[a,b,c,d];
    a=ff(a,b,c,d,blk[i],7,-680876936);      d=ff(d,a,b,c,blk[i+1],12,-389564586);
    c=ff(c,d,a,b,blk[i+2],17,606105819);    b=ff(b,c,d,a,blk[i+3],22,-1044525330);
    a=ff(a,b,c,d,blk[i+4],7,-176418897);    d=ff(d,a,b,c,blk[i+5],12,1200080426);
    c=ff(c,d,a,b,blk[i+6],17,-1473231341);  b=ff(b,c,d,a,blk[i+7],22,-45705983);
    a=ff(a,b,c,d,blk[i+8],7,1770035416);    d=ff(d,a,b,c,blk[i+9],12,-1958414417);
    c=ff(c,d,a,b,blk[i+10],17,-42063);      b=ff(b,c,d,a,blk[i+11],22,-1990404162);
    a=ff(a,b,c,d,blk[i+12],7,1804603682);   d=ff(d,a,b,c,blk[i+13],12,-40341101);
    c=ff(c,d,a,b,blk[i+14],17,-1502002290); b=ff(b,c,d,a,blk[i+15],22,1236535329);
    a=gg(a,b,c,d,blk[i+1],5,-165796510);    d=gg(d,a,b,c,blk[i+6],9,-1069501632);
    c=gg(c,d,a,b,blk[i+11],14,643717713);   b=gg(b,c,d,a,blk[i],20,-373897302);
    a=gg(a,b,c,d,blk[i+5],5,-701558691);    d=gg(d,a,b,c,blk[i+10],9,38016083);
    c=gg(c,d,a,b,blk[i+15],14,-660478335);  b=gg(b,c,d,a,blk[i+4],20,-405537848);
    a=gg(a,b,c,d,blk[i+9],5,568446438);     d=gg(d,a,b,c,blk[i+14],9,-1019803690);
    c=gg(c,d,a,b,blk[i+3],14,-187363961);   b=gg(b,c,d,a,blk[i+8],20,1163531501);
    a=gg(a,b,c,d,blk[i+13],5,-1444681467);  d=gg(d,a,b,c,blk[i+2],9,-51403784);
    c=gg(c,d,a,b,blk[i+7],14,1735328473);   b=gg(b,c,d,a,blk[i+12],20,-1926607734);
    a=hh(a,b,c,d,blk[i+5],4,-378558);       d=hh(d,a,b,c,blk[i+8],11,-2022574463);
    c=hh(c,d,a,b,blk[i+11],16,1839030562);  b=hh(b,c,d,a,blk[i+14],23,-35309556);
    a=hh(a,b,c,d,blk[i+1],4,-1530992060);   d=hh(d,a,b,c,blk[i+4],11,1272893353);
    c=hh(c,d,a,b,blk[i+7],16,-155497632);   b=hh(b,c,d,a,blk[i+10],23,-1094730640);
    a=hh(a,b,c,d,blk[i+13],4,681279174);    d=hh(d,a,b,c,blk[i],11,-358537222);
    c=hh(c,d,a,b,blk[i+3],16,-722521979);   b=hh(b,c,d,a,blk[i+6],23,76029189);
    a=hh(a,b,c,d,blk[i+9],4,-640364487);    d=hh(d,a,b,c,blk[i+12],11,-421815835);
    c=hh(c,d,a,b,blk[i+15],16,530742520);   b=hh(b,c,d,a,blk[i+2],23,-995338651);
    a=ii(a,b,c,d,blk[i],6,-198630844);      d=ii(d,a,b,c,blk[i+7],10,1126891415);
    c=ii(c,d,a,b,blk[i+14],15,-1416354905); b=ii(b,c,d,a,blk[i+5],21,-57434055);
    a=ii(a,b,c,d,blk[i+12],6,1700485571);   d=ii(d,a,b,c,blk[i+3],10,-1894986606);
    c=ii(c,d,a,b,blk[i+10],15,-1051523);    b=ii(b,c,d,a,blk[i+1],21,-2054922799);
    a=ii(a,b,c,d,blk[i+8],6,1873313359);    d=ii(d,a,b,c,blk[i+15],10,-30611744);
    c=ii(c,d,a,b,blk[i+6],15,-1560198380);  b=ii(b,c,d,a,blk[i+13],21,1309151649);
    a=ii(a,b,c,d,blk[i+4],6,-145523070);    d=ii(d,a,b,c,blk[i+11],10,-1120210379);
    c=ii(c,d,a,b,blk[i+2],15,718787259);    b=ii(b,c,d,a,blk[i+9],21,-343485551);
    a=add(a,pa);b=add(b,pb);c=add(c,pc);d=add(d,pd);
  }
  return[a,b,c,d].map(n=>[0,1,2,3].map(j=>
    ((n>>(j*8+4))&0xf).toString(16)+((n>>(j*8))&0xf).toString(16)
  ).join('')).join('');
}
