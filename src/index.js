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
 *    每次发送 fetch() 设置 SEND_FETCH_TIMEOUT_MS 超时；接收端 hang 时单次 fetch 被中断 →
 *    抛 AbortError → 外层 catch 触发 msg.retry()，避免单 invocation 撑满 Queue 的 15 分钟上限。
 *
 *  关键机制：
 *    - 幂等：确定性 batchKey + .done 标记 → 重复投递不会重复 POST（接收端无需去重）；
 *      源级 .done 让已完整处理过的原始对象直接跳过、不重解析。
 *    - parse 有界并发池：单次 invocation 内最多 PARSE_PARALLELISM 个文件并行解析，每个文件流式 +
 *      每 BATCH_SIZE 行 flush → 峰值内存受 BATCH_SIZE 限、与文件大小无关。
 *    - 单次 R2 read() 空闲超时（PARSE_READ_IDLE_TIMEOUT_MS）：卡死的读在超时后中断、只重试该文件，
 *      不挡同批其他文件；空闲口径、与对象大小无关。
 *    - DLQ 自动重驱动：worker 同时消费 parse-dlq / send-dlq，把死信以指数退避+抖动回灌源队列，
 *      靠 .done 幂等保证不重复，全自动恢复、无需人工/告警。
 *    - 发送：resp.body.cancel() 防 stalled HTTP response；delete 失败不抛异常（R2 lifecycle 兜底）；
 *      Queue send 失败回滚 R2 临时文件。
 *    - PUSH_START_TIME 时间过滤（未来/过去双模式）+ Scheduled handler 历史补传（一次性 + 幂等 marker）。
 *
 *  Env Secrets : CTYUN_ENDPOINT, CTYUN_PRIVATE_KEY, CTYUN_URI_EDGE
 *  Env Vars    : BATCH_SIZE, LOG_LEVEL, PARSE_QUEUE_NAME, SEND_QUEUE_NAME,
 *                PARSE_DLQ_NAME, SEND_DLQ_NAME, R2_BUCKET_NAME, PUSH_START_TIME,
 *                FIELD11_SERVER_IP, SEND_PARALLELISM, PARSE_PARALLELISM,
 *                PARSE_READ_IDLE_TIMEOUT_MS, SEND_FETCH_TIMEOUT_MS, RETRY_DELAY_SECONDS,
 *                REDRIVE_BASE_DELAY_SECONDS, REDRIVE_MAX_DELAY_SECONDS,
 *                REDRIVE_JITTER_SECONDS
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
  // Additional active POP airport codes
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
const MAX_RECOVERY_DAYS = 62;
const DEFAULT_PARSE_READ_IDLE_MS = 45000;  // 单次 R2 read() 空闲超时默认(ms)：卡死才触发，与对象大小无关
const LOG_LEVELS   = Object.freeze({ debug:0, info:1, warn:2, error:3 });
// ─── 主入口 ────────────────────────────────────────────────────────────────
export default {
  async queue(batch, env, ctx) {
    if      (batch.queue === env.PARSE_QUEUE_NAME) await handleParseQueue(batch, env);
    else if (batch.queue === env.SEND_QUEUE_NAME)  await handleSendQueue(batch, env);
    // DLQ 自动重驱动：死信回灌到各自源队列（无人工、无告警）。绑定 binding 沿用现有 producer。
    else if (env.PARSE_DLQ_NAME && batch.queue === env.PARSE_DLQ_NAME) await handleDlqRedrive(batch, env, env.PARSE_QUEUE, env.PARSE_QUEUE_NAME, 'parse');
    else if (env.SEND_DLQ_NAME  && batch.queue === env.SEND_DLQ_NAME)  await handleDlqRedrive(batch, env, env.SEND_QUEUE,  env.SEND_QUEUE_NAME,  'send');
    else throw new Error(`Unknown queue: ${batch.queue}; check PARSE_QUEUE_NAME/SEND_QUEUE_NAME/PARSE_DLQ_NAME/SEND_DLQ_NAME`);
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(handleScheduled(env));
  },
};

// ─── Parser: R2原始文件 → 流式解析转换 → R2临时文件 → send-queue ───────────
async function handleParseQueue(batch, env) {
  // 有界并发池：单次 invocation 内最多 PARSE_PARALLELISM 个文件并行解析（默认 1=串行；[vars] 设 2）。
  // 内存安全：同一时刻最多 P 个文件在解析，每个仍是流式 + 每 BATCH_SIZE 行 flush →
  //   峰值内存 ≈ P × 单文件批次，与对象大小无关，远低于 128MB。
  // CPU 安全：单 invocation 总 CPU ≈ max_batch_size × 单文件 CPU，须 < limits.cpu_ms。
  // 横向扩：invocation 数量由 autoscaler 按 backlog 扩（每队列上限 250）。
  // ⚠️ wrangler 的 parse consumer max_batch_size 须 ≥ PARSE_PARALLELISM，池才有多文件可并行（=1 退化串行）。
  // processFile 内部自行 ack/retry，单文件失败不影响同批其他文件。
  const pool = (() => {
    const n = Number(env?.PARSE_PARALLELISM);
    if (!Number.isFinite(n) || n < 1) return 1;
    return Math.min(Math.floor(n), 8); // 软上限，防误填过大导致 OOM
  })();
  let cursor = 0;
  const total = batch.messages.length;
  const worker = async () => {
    while (true) {
      const i = cursor++;
      if (i >= total) break;
      await processFile(batch.messages[i], env);
    }
  };
  await Promise.allSettled(Array.from({ length: pool }, () => worker()));
}

// ─── DLQ 自动重驱动消费者 ────────────────────────────────────────────────────
// 进入 parse-dlq / send-dlq 的死信，以「指数退避 + 抖动」的延迟回灌到各自源队列
//   （parse-queue / send-queue），交给正常的幂等管线重新处理，全自动、无需人工/告警。
// 为什么不产生重复（适配无去重接收端）：
//   - Parser 的 batchKey 是确定性的（processed/<safeKey>-<index>.txt），Sender 成功后写 .done 标记。
//   - 重驱动 → 重解析 → 已发送批次撞 .done 跳过 → 只补发从未发出的尾部 → 零重复。
//   - 前提：BATCH_SIZE 不变（否则 index→行区间错位会破坏 .done 匹配）。修改 BATCH_SIZE 须先清空 DLQ。
// 行为：
//   - 瞬时失败：延迟后再试通常即成功，自愈。
//   - 毒消息：__redrive 计数驱动退避逐次拉长（封顶 REDRIVE_MAX_DELAY_SECONDS），退化为低频无害循环。
//   - 抖动避免大量死信同时回灌造成 thundering herd。
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

  // ─── 源级 .done 幂等早退 ───────────────────────────────────────────────────
  // 该原始对象已被完整处理过（写过源级 .done 标记）→ 一次 HEAD 命中即 ack 跳过，不重解析。
  //   （应对重复投递：同一对象被多次入队时，只有第一份做完整解析，其余秒级跳过。）
  // 标记键 `${key}.done` 落在 logs/ 下但以 .done 结尾（非 .log.gz），不会被当作原始日志再次触发/解析。
  // 安全边界：源级 .done 仅在【所有批次都已成功入 send-queue】之后才写（见下方）；
  //   即便 .done 漏写，批次级 .done 仍是「不重复发送」的最终保证。
  const sourceDoneKey = `${key}.done`;
  if (await env.RAW_BUCKET.head(sourceDoneKey).catch(() => null)) {
    log(env, 'info', `Source already processed (skip): ${key}`);
    msg.ack();
    return;
  }

  // ─── PUSH_START_TIME 文件级过滤 ───────────────────────────────────────────
  // 环境变量未设置或为空时，跳过过滤，正常处理所有文件（默认行为）
  // 设置后，根据文件名中的时间戳对整个文件做预判断，避免不必要的 R2 读取
  // 文件名格式: logs/YYYYMMDD/YYYYMMDDTHHmmssZ_YYYYMMDDTHHmmssZ_xxxx.log.gz
  // 一次性逻辑：当所有新文件时间都 >= startMs 时，此过滤永远不触发，无性能损耗
  const startMs = getPushStartMs(env);
  if (startMs !== null) {
    const fileEndMs = parseFileEndTime(key);
    if (fileEndMs !== null && fileEndMs < startMs) {
      log(env, 'info', `Skipped (before PUSH_START_TIME): ${key}`);
      msg.ack();
      return;
    }
  }
  // ─────────────────────────────────────────────────────────────────────────

  log(env, 'info', `Parsing: ${key}`);
  try {
    const object = await env.RAW_BUCKET.get(key);
    if (!object) { log(env, 'warn', `Not in R2: ${key}`); msg.ack(); return; }
    const batchSize = parseIntegerVar(env, 'BATCH_SIZE', DEFAULT_BATCH_SIZE, 1, MAX_BATCH_SIZE);
    // 单次 R2 read() 空闲超时：健康流每几百 ms 出块、永不触发；只有单次读卡死才超时 → 抛错 →
    // 下方 catch 对【这一个文件】msg.retry（只重试该文件、不挡同批后续、不丢数据）。
    // 空闲口径（每次 read 重新计时），与对象大小无关 → 全域统一一个值，不会误杀大文件。
    const idleTimeoutMs = parseIntegerVar(env, 'PARSE_READ_IDLE_TIMEOUT_MS', DEFAULT_PARSE_READ_IDLE_MS, 0, 600000);
    let lines = [], batchIdx = 0, lineCount = 0, errCount = 0, parseErrCount = 0, skipped = 0;
    await streamParseNdjsonGzip(object.body, async (record) => {
      lineCount++;
      // 逐行时间过滤：仅用于文件跨越 startMs 的边界情况
      if (startMs !== null) {
        const recMs = parseTimestamp(record.EdgeStartTimestamp);
        if (recMs !== null && recMs < startMs) {
          skipped++;
          return;
        }
      }
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
    // 源级完成标记：仅在【所有批次都已成功入 send-queue】之后才写（投递由持久化的 send-queue + 批次级 .done 保证）。
    // 写在 msg.ack() 之前；若崩在中途则不会写 .done → 重试/重投会重解析，已发批次撞批次级 .done 跳过、
    //   未发尾部补发 → 绝不丢日志、也不重复。writeDoneMarker 失败仅告警不抛：丢的只是「下次重复投递的快速跳过」优化。
    await writeDoneMarker(env, key);
    log(env, 'info', `Done: ${key} | lines=${lineCount} batches=${batchIdx} errors=${errCount} parseErrors=${parseErrCount} skipped=${skipped}`);
    msg.ack();
  } catch (err) {
    log(env, 'error', `Failed: ${key}: ${err.message}`);
    // 延迟重试：给瞬时失败（R2 抖动等）留出恢复时间；耗尽 max_retries 后进 parse-dlq，
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

  // 单次 fetch 超时（SEND_FETCH_TIMEOUT_MS）：接收端 hang 时尽快 abort → 外层 catch → msg.retry() 接管。
  // 作用：缩短慢发送滞留时间、降低并发发送的内存堆积，并保证单 invocation wall 不撑满 Queue 的 15min 上限。
  const sendTimeoutMs = parseIntegerVar(env, 'SEND_FETCH_TIMEOUT_MS', 30000, 1000, 60000);
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
  // 健康的 R2 流每几百毫秒就产出一块数据，永远不会触发；只有「单次读卡死」（连接挂住）
  // 才会触发 → 抛错 → 上层对该文件 msg.retry。
  // 阈值与对象大小【无关】（大文件也是连续出块），可全域统一一个值、不会误杀大文件。
  // 每次循环重新计时（空闲口径）。0=不限。
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
//   #11 server_ip:         EdgeServerIP；为空时使用 FIELD11_SERVER_IP 兜底
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
    /* 11 */ sf(r.EdgeServerIP || env.FIELD11_SERVER_IP),
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

// ─── PUSH_START_TIME 辅助函数 ─────────────────────────────────────────────
// 解析环境变量 PUSH_START_TIME，返回毫秒时间戳，未设置时返回 null
// 支持 ISO 8601 格式，如 "2026-04-15T10:00:00Z" 或 "2026-04-15T18:00:00+08:00"
function getPushStartMs(env) {
  const v = env.PUSH_START_TIME;
  if (!v || !v.trim()) return null;
  const ms = new Date(v.trim()).getTime();
  if (isNaN(ms)) {
    console.warn(`[WARN] Invalid PUSH_START_TIME: "${v}", filtering disabled`);
    return null;
  }
  return ms;
}

// 从 R2 文件名中解析文件结束时间（毫秒）
// 文件名格式: logs/20260415/20260415T100000Z_20260415T100060Z_xxxx.log.gz
// 第二个时间戳为文件结束时间，用于文件级快速预判断
// 解析失败时返回 null，退化为逐行过滤
function parseFileEndTime(key) {
  // 匹配文件名中的第二个时间戳（ISO基本格式：YYYYMMDDTHHmmssZ）
  const m = key.match(/\d{8}T\d{6}Z_(\d{8}T\d{6}Z)/);
  if (!m) return null;
  // 转换为 ISO 8601 扩展格式让 Date 可以解析
  const s = m[1]; // e.g. "20260415T100060Z"
  const iso = `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}T${s.slice(9,11)}:${s.slice(11,13)}:${s.slice(13,15)}Z`;
  const ms = new Date(iso).getTime();
  return isNaN(ms) ? null : ms;
}

// 从 R2 文件名中解析文件开始时间（毫秒）
// 第一个时间戳为文件开始时间，用于补救恢复时判断文件是否在目标时间范围内
function parseFileStartTime(key) {
  const m = key.match(/(\d{8}T\d{6}Z)_\d{8}T\d{6}Z/);
  if (!m) return null;
  const s = m[1];
  const iso = `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}T${s.slice(9,11)}:${s.slice(11,13)}:${s.slice(13,15)}Z`;
  const ms = new Date(iso).getTime();
  return isNaN(ms) ? null : ms;
}

// ─── Scheduled 处理：Cron 每分钟触发一次，检查是否需要补传历史日志 ─────────
// 触发补救的条件（全部满足）：
//   1. PUSH_START_TIME 已设置
//   2. 其值 < 当前时间（过去时间）
//   3. 该时间值对应的完成标记 .recover-done-<时间> 在 R2 中不存在
// 满足条件时：扫描 R2 logs/ 目录 → 筛选时间范围匹配的文件 → 批量入队 parse-queue
// 执行成功后写入完成标记；失败不写 done，下一次 Cron 自动重试
const RECOVER_MARKER_PREFIX = '.recover-done-';
const RECOVER_RUNNING_PREFIX = '.recover-running-';
const RECOVER_RUNNING_STALE_MS = 15 * 60 * 1000;

async function handleScheduled(env) {
  const startMs = getPushStartMs(env);
  if (startMs === null) {
    // 未设置或格式错误，秒级返回
    return;
  }
  const now = Date.now();
  if (startMs > now) {
    // 未来时间，无需恢复
    return;
  }
  const recoveryDays = getRecoveryDayCount(startMs, now);
  if (recoveryDays > MAX_RECOVERY_DAYS) {
    log(env, 'error', `[SCHEDULED] Recovery range ${recoveryDays} days exceeds ${MAX_RECOVERY_DAYS}-day safety limit; use dedicated backfill worker`);
    return;
  }
  // 过去时间，检查幂等标记
  const v = env.PUSH_START_TIME.trim();
  const encoded = encodeURIComponent(v);
  const markerKey = `${RECOVER_MARKER_PREFIX}${encoded}`;
  const runningKey = `${RECOVER_RUNNING_PREFIX}${encoded}`;
  const existing = await env.RAW_BUCKET.head(markerKey).catch(() => null);
  if (existing) {
    // 已执行过，跳过
    return;
  }

  const running = await env.RAW_BUCKET.head(runningKey).catch(() => null);
  const runningUploadedMs = running?.uploaded ? new Date(running.uploaded).getTime() : 0;
  if (running && runningUploadedMs && now - runningUploadedMs < RECOVER_RUNNING_STALE_MS) {
    log(env, 'info', `[SCHEDULED] Recovery already running: PUSH_START_TIME=${v}`);
    return;
  }

  log(env, 'info', `[SCHEDULED] Recovery started: PUSH_START_TIME=${v}, scanning R2 for files from that time to now`);

  // 先写 running 标记，降低重复 Cron 并发；失败时删除，成功后写 done。
  try {
    await env.RAW_BUCKET.put(runningKey, JSON.stringify({
      pushStartTime: v,
      startedAt: new Date().toISOString(),
    }), {
      httpMetadata: { contentType: 'application/json' },
    });
  } catch (e) {
    log(env, 'error', `[SCHEDULED] Failed to write marker, aborting: ${e.message}`);
    return;
  }

  // 执行恢复
  let result;
  try {
    result = await recoverLogs(env, startMs, now);
    if (result.errors > 0 || result.enqueued !== result.matched) {
      throw new Error(`Recovery incomplete: ${JSON.stringify(result)}`);
    }
  } catch (e) {
    log(env, 'error', `[SCHEDULED] Recovery failed: ${e.message}`);
    await env.RAW_BUCKET.delete(runningKey).catch(() => {});
    return;
  }

  // 更新标记，记录完成结果
  try {
    await env.RAW_BUCKET.put(markerKey, JSON.stringify({
      pushStartTime: v,
      startedAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      result,
    }), {
      httpMetadata: { contentType: 'application/json' },
    });
  } catch (e) {
    log(env, 'error', `[SCHEDULED] Recovery done marker write failed; will retry after running marker becomes stale: ${e.message}`);
    return;
  }
  await env.RAW_BUCKET.delete(runningKey).catch(() => {});

  log(env, 'info', `[SCHEDULED] Recovery done: ${JSON.stringify(result)}`);
}

// 扫描 R2 目录，把时间范围内的文件批量入队到 parse-queue
// 为避免全桶扫描，按日期拆分 prefix（logs/YYYYMMDD/）
async function recoverLogs(env, startMs, endMs) {
  const prefixes = getR2PrefixesByDay(startMs, endMs, env);
  let scanned = 0;
  let matched = 0;
  let enqueued = 0;
  let errors = 0;

  for (const prefix of prefixes) {
    let cursor;
    do {
      const page = await env.RAW_BUCKET.list({ prefix, limit: 1000, cursor });

      const toEnqueue = [];
      for (const obj of page.objects) {
        scanned++;
        const key = obj.key;
        // 仅恢复原始 Logpush 文件，避免 processed/ 和 marker 被误处理。
        if (!isRawLogKey(key, env)) continue;

        const fileStartMs = parseFileStartTime(key);
        const fileEndMs = parseFileEndTime(key);
        if (fileStartMs === null || fileEndMs === null) continue;

        // 文件时间 [fileStartMs, fileEndMs] 与目标 [startMs, endMs] 有重叠
        if (fileStartMs <= endMs && fileEndMs >= startMs) {
          matched++;
          // bucket 字段仅为与 R2 Event Notification 原生消息格式保持一致
          // Parser 实际通过 env.RAW_BUCKET binding 访问，不读 bucket 字段
          toEnqueue.push({
            body: { bucket: env.R2_BUCKET_NAME || 'cdn-logs-raw', object: { key } },
          });
        }
      }

      // 批量入队，每批最多 100（Queue sendBatch 限制）
      for (let i = 0; i < toEnqueue.length; i += 100) {
        const batch = toEnqueue.slice(i, i + 100);
        try {
          await env.PARSE_QUEUE.sendBatch(batch);
          enqueued += batch.length;
        } catch (e) {
          errors += batch.length;
          log(env, 'warn', `[RECOVER] Batch enqueue failed (${batch.length} msgs): ${e.message}`);
        }
      }

      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor);
  }

  return { prefixes, scanned, matched, enqueued, errors };
}

// 根据时间范围生成 R2 list 所需的日期 prefix 列表（避免全桶扫描）
// 以 UTC 日期为边界（R2 文件名中的时间戳是 UTC）
function getR2PrefixesByDay(startMs, endMs, env) {
  const prefixes = [];
  const rawPrefix = env?.RAW_LOG_PREFIX || RAW_LOG_PREFIX;
  const prefixBase = rawPrefix.endsWith('/') ? rawPrefix : `${rawPrefix}/`;
  const d = new Date(startMs);
  d.setUTCHours(0, 0, 0, 0);
  const endDay = new Date(endMs);
  endDay.setUTCHours(0, 0, 0, 0);
  // 最多遍历 62 天，防止误配置导致过量扫描
  let iter = 0;
  while (d.getTime() <= endDay.getTime() && iter++ < 62) {
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    prefixes.push(`${prefixBase}${yyyy}${mm}${dd}/`);
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return prefixes;
}

function getRecoveryDayCount(startMs, endMs) {
  const start = new Date(startMs);
  start.setUTCHours(0, 0, 0, 0);
  const end = new Date(endMs);
  end.setUTCHours(0, 0, 0, 0);
  return Math.floor((end.getTime() - start.getTime()) / 86400000) + 1;
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
