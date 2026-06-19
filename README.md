# ctyun-logpush-worker

A Cloudflare Worker that transforms Cloudflare Logpush `http_requests` logs into the
CDN partner 145-field format and forwards them, in near real time, to the customer's
log ingestion endpoint.

A single Worker script plays three roles: **Parser** (consumes `parse-queue`),
**Sender** (consumes `send-queue`), and **DLQ re-driver** (consumes `parse-dlq` /
`send-dlq`). Batch payloads live in R2 `processed/*.txt`, so queue messages carry only a
key and stay well under the 128 KB message limit.

## Pipeline

```text
End user ─▶ Cloudflare edge (300+ cities)
   │  edge writes one http_requests log line
   ▼
Logpush job ── batches ~1 min of logs, gzip ──▶ R2  <domain>-com   logs/YYYYMMDD/….log.gz
   │
   │  R2 Event Notification (object-create, prefix logs/) ── one message per file
   ▼
parse-queue ─▶ [Parser]   one file → stream-decompress → transform to 145 fields
   │                      every BATCH_SIZE lines → R2 processed/*.txt + enqueue
   ▼
send-queue  ─▶ [Sender]   gzip → signed POST ──▶ customer ingestion endpoint
   │
parse-dlq / send-dlq ─▶ [DLQ re-driver]  backoff + jitter ──▶ back to its source queue
```

The pipeline is event-driven and idempotent end to end: a deterministic per-batch key
plus `.done` markers in R2 make re-processing safe, so a batch is never POSTed twice and
the receiver needs no de-duplication.

## Per-domain configuration

This template is deployed once **per domain**. For each domain, edit `wrangler.toml`
(every field is documented inline) and set the three secrets. `<domain>` is the
per-deployment slug used consistently across the worker name, bucket, and queue names.

| In `wrangler.toml` | Set to |
|---|---|
| `name` | `<domain>-com-log` |
| `account_id` | the Cloudflare account ID the domain lives in |
| R2 `bucket_name` **and** `[vars].R2_BUCKET_NAME` | `<domain>-com` (must match) |
| Queue names — 2 producers, 4 consumers, and the 4 `[vars]` `*_QUEUE_NAME` / `*_DLQ_NAME` | `parse-queue-<domain>`, `send-queue-<domain>`, `parse-dlq-<domain>`, `send-dlq-<domain>` (keep all references consistent) |
| `[vars].FIELD11_SERVER_IP` | **Required.** The domain's anycast / resolved IP, written into log field #11 `server_ip` (e.g. `172.65.90.64`). If left empty, field #11 is emitted as `-`. |

Secrets (per domain):

```bash
wrangler secret put CTYUN_ENDPOINT      # ingestion base URL, no trailing slash
wrangler secret put CTYUN_PRIVATE_KEY   # auth private key
wrangler secret put CTYUN_URI_EDGE      # target URI path
```

For GitHub Actions deployment, set the repository secret `CLOUDFLARE_API_TOKEN`.

Everything else (`BATCH_SIZE`, `PARSE_PARALLELISM`, `SEND_PARALLELISM`,
`PARSE_READ_IDLE_TIMEOUT_MS`, `SEND_FETCH_TIMEOUT_MS`, retry / re-drive delays,
`RAW_LOG_PREFIX` / `SUFFIX`) carries sensible defaults documented inline in
`wrangler.toml`; normally you only set the per-domain identifiers above.

Configure the R2 Event Notification for raw Logpush objects **only**: `object-create`
with prefix `logs/`. Notifying on the whole bucket would feed `processed/` and marker
files back into `parse-queue`.

## Output format

- 145 fields separated by `\u0001`, per CDN partner log interface v3.0.
- HTTP body is gzip-compressed; `auth_key = ts-rand-md5(uri-ts-rand-privateKey)`.
- Field #45 maps `EdgeColoCode` to a country code; unmapped values fall back to `SG`.

## How it works

- **Idempotent delivery.** Queue delivery is at-least-once; a deterministic per-batch key
  plus `.done` markers in R2 make re-processing safe, so a batch is never POSTed twice. A
  source-level `.done` also lets the parser skip a file it has already fully processed. The
  receiver needs no de-duplication.
- **Self-healing dead letters.** `parse-dlq` and `send-dlq` are consumed by the same Worker,
  which re-drives messages back to their source queue with exponential backoff + jitter, so
  transient failures recover automatically — no alerts, no manual draining.
- **Bounded, streaming parse.** The parser processes a file batch through a small
  in-invocation pool (`PARSE_PARALLELISM`) and streams each file (decompress → transform →
  flush every `BATCH_SIZE` lines), so peak memory is bounded by the batch size, not the file
  size. Each R2 read carries a per-read idle timeout (`PARSE_READ_IDLE_TIMEOUT_MS`, default
  45s): a stalled read is aborted and that one file retried, so a single object never blocks
  the queue. Being a per-read idle timeout (not a whole-file deadline), the same value is
  safe for any file size — a healthy large file emits chunks continuously and never trips it.
- **Streaming send.** The sender streams `R2 object → gzip → fetch body`, so memory per
  request stays small. The request uses HTTP/1.1 `Transfer-Encoding: chunked` (no
  `Content-Length`). **The customer endpoint must accept chunked request bodies**; HTTP
  `400` / `411` / `415` means it does not. Each POST is bounded by `SEND_FETCH_TIMEOUT_MS`
  (default 30s) so a stalled receiver is aborted and retried.
- **Horizontal scale.** `max_concurrency` is left unset on the main queues, so the Queue
  autoscaler scales invocations with backlog (per-queue cap 250).
- The parser ignores non-raw R2 objects (defaults: only `logs/…*.log.gz`).

## Deploy

Push to `main` triggers GitHub Actions deployment. To validate locally:

```bash
npx wrangler deploy --dry-run
```

## Documentation

| Language | Guide |
|---|---|
| English | [CF Logpush – Format Transform & Push Guide](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF-Logpush-Format-Transform-and-Push-Guide.html) |
| Chinese | [CF 日志格式转换与自动推送指南](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF%E6%97%A5%E5%BF%97%E6%A0%BC%E5%BC%8F%E8%BD%AC%E6%8D%A2%E4%B8%8E%E8%87%AA%E5%8A%A8%E6%8E%A8%E9%80%81%E6%8C%87%E5%8D%97.html) |
