// 自验证单测：直接拷贝 src/index.js 字节 + 追加命名导出后作为 ESM 载入，测真实代码。
// 运行：node --test test/    （Node 20+，已内置 DecompressionStream/CompressionStream/node:test）
// 覆盖本次三处改动：①单次读空闲超时 ②补扫候选挑选(纯函数) ③补扫日期前缀生成。
import { test } from 'node:test';
import assert from 'node:assert';
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

// —— 载入真实源码（拷贝 + 追加命名导出；Workers 运行时只用 default export，命名导出无副作用）——
const srcUrl = new URL('../src/index.js', import.meta.url);
const src = readFileSync(srcUrl, 'utf8');
const dir = mkdtempSync(join(tmpdir(), 'ctyun-verify-'));
const modPath = join(dir, 'mod.mjs');
writeFileSync(modPath, src + '\nexport { streamParseNdjsonGzip, tryParse, selectReconcileCandidates, genDayPrefixes };\n');
const mod = await import(pathToFileURL(modPath).href);

// —— 测试工具 ——
async function gzipBytes(str) {
  const cs = new CompressionStream('gzip');
  const w = cs.writable.getWriter();
  w.write(new TextEncoder().encode(str));
  w.close();
  const r = cs.readable.getReader();
  const chunks = [];
  for (;;) { const { done, value } = await r.read(); if (done) break; chunks.push(value); }
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total); let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.length; }
  return out;
}
function streamFromBytes(bytes) {
  return new ReadableStream({ start(c) { c.enqueue(bytes); c.close(); } });
}
function stalledStream() { // 永不产出、永不关闭 → reader.read() 永久挂起（模拟 R2 读卡死）
  return new ReadableStream({ start() {} });
}

// ── ① 单次读空闲超时 ──────────────────────────────────────────────────────────
test('idle timeout fires on a stalled read (~timeout ms)', async () => {
  const t0 = Date.now();
  let err = null;
  try { await mod.streamParseNdjsonGzip(stalledStream(), async () => {}, () => {}, 300); }
  catch (e) { err = e; }
  const dt = Date.now() - t0;
  assert.ok(err, 'should throw on stalled read');
  assert.match(String(err.message), /idle timeout/i);
  assert.ok(dt >= 250 && dt < 3000, `expected ~300ms, got ${dt}ms`);
});

test('healthy stream parses all records, no false timeout', async () => {
  const gz = await gzipBytes('{"a":1}\n{"a":2}\n{"a":3}\n');
  const recs = [];
  await mod.streamParseNdjsonGzip(streamFromBytes(gz), async (r) => recs.push(r), () => {}, 2000);
  assert.equal(recs.length, 3);
  assert.equal(recs[2].a, 3);
});

test('idleTimeoutMs=0 disables timeout (backward compatible)', async () => {
  const gz = await gzipBytes('{"x":true}\n{"y":5}\n');
  const recs = [];
  await mod.streamParseNdjsonGzip(streamFromBytes(gz), async (r) => recs.push(r), () => {}, 0);
  assert.equal(recs.length, 2);
  assert.equal(recs[0].x, true);
});

test('stalled read produces no unhandled rejection (rp.catch + cancel)', async () => {
  let unhandled = 0;
  const h = () => { unhandled++; };
  process.on('unhandledRejection', h);
  try { await mod.streamParseNdjsonGzip(stalledStream(), async () => {}, () => {}, 200); } catch {}
  await new Promise((r) => setTimeout(r, 600)); // 给滞后 rejection 浮现的时间
  process.off('unhandledRejection', h);
  assert.equal(unhandled, 0, 'no unhandled rejection expected');
});

test('malformed JSON line goes to onParseError, valid lines still parsed', async () => {
  const gz = await gzipBytes('{"ok":1}\nNOT_JSON\n{"ok":2}\n');
  const recs = []; let parseErr = 0;
  await mod.streamParseNdjsonGzip(streamFromBytes(gz), async (r) => recs.push(r), () => { parseErr++; }, 2000);
  assert.equal(recs.length, 2);
  assert.equal(parseErr, 1);
});

// ── ② 补扫候选挑选（纯函数，幂等/去重核心）───────────────────────────────────────
test('selectReconcileCandidates: picks stale, skips done/fresh/old/missing-handled', () => {
  const now = 1_700_000_000_000;
  const lookbackMs = 120 * 60000, graceMs = 120_000, maxEnqueue = 500;
  const raw = new Map([
    ['logs/d/fresh.log.gz',   now - 60_000],    // 太新(<grace) → 跳过，交给事件路径
    ['logs/d/stale.log.gz',   now - 300_000],   // 窗口内、无 .done → 候选
    ['logs/d/done.log.gz',    now - 300_000],   // 窗口内但已 .done → 跳过
    ['logs/d/old.log.gz',     now - 8_000_000], // 早于 lookback → 跳过
    ['logs/d/noup.log.gz',    0],               // uploaded 缺失 → 保守视为候选
  ]);
  const done = new Set(['logs/d/done.log.gz']);
  const got = mod.selectReconcileCandidates(raw, done, now, lookbackMs, graceMs, maxEnqueue).sort();
  assert.deepEqual(got, ['logs/d/noup.log.gz', 'logs/d/stale.log.gz']);
});

test('selectReconcileCandidates: respects maxEnqueue cap', () => {
  const now = 1_700_000_000_000;
  const raw = new Map();
  for (let i = 0; i < 10; i++) raw.set(`logs/d/o${i}.log.gz`, now - 300_000);
  const got = mod.selectReconcileCandidates(raw, new Set(), now, 7_200_000, 120_000, 3);
  assert.equal(got.length, 3);
});

test('selectReconcileCandidates: a done object is never re-enqueued (no duplicate send path)', () => {
  const now = 1_700_000_000_000;
  const raw = new Map([['logs/d/x.log.gz', now - 600_000]]);
  const done = new Set(['logs/d/x.log.gz']);
  const got = mod.selectReconcileCandidates(raw, done, now, 7_200_000, 120_000, 500);
  assert.deepEqual(got, []);
});

// ── ③ 补扫日期前缀（UTC 边界）────────────────────────────────────────────────────
test('genDayPrefixes: same UTC day → one prefix', () => {
  const d = Date.UTC(2026, 5, 13, 7, 0, 0); // 2026-06-13 (month 0-indexed)
  assert.deepEqual(mod.genDayPrefixes(d, d + 3600_000, {}), ['logs/20260613/']);
});

test('genDayPrefixes: crossing UTC midnight → two prefixes', () => {
  const start = Date.UTC(2026, 5, 12, 23, 30, 0);
  const end = Date.UTC(2026, 5, 13, 0, 30, 0);
  assert.deepEqual(mod.genDayPrefixes(start, end, {}), ['logs/20260612/', 'logs/20260613/']);
});

test('genDayPrefixes: honors custom RAW_LOG_PREFIX', () => {
  const d = Date.UTC(2026, 5, 13, 7, 0, 0);
  assert.deepEqual(mod.genDayPrefixes(d, d, { RAW_LOG_PREFIX: 'raw' }), ['raw/20260613/']);
});
