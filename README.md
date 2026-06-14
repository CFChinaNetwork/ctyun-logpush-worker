# ctyun-logpush-worker

A Cloudflare Worker that transforms Cloudflare Logpush `http_requests` logs into the
CDN partner 145-field format and forwards them, in near real time, to the customer's
log ingestion endpoint.

## Flow

```text
Cloudflare Logpush ─▶ R2 logs/
   ├─ R2 Event Notification ─▶ parse-queue ─▶ [Parser] ─▶ R2 processed/*.txt
   │                                              └─▶ send-queue ─▶ [Sender] ─▶ gzip + auth_key POST ─▶ customer endpoint
   └─ cron (every 1 min) ─▶ [Reconcile sweep] ─▶ re-enqueue any logs/ object that is past
                                                  its grace window and still has no .done ─▶ parse-queue
```

The event-notification path is the real-time main path (~2 min end-to-end). The cron
reconcile sweep is the safety net for objects whose notification was delayed or dropped.

A single Worker script acts as the **parser** (consumes `parse-queue`), the **sender**
(consumes `send-queue`), and the **DLQ re-driver** (consumes `parse-dlq` / `send-dlq`).
R2 `processed/*.txt` holds each batch payload, so the queue messages only carry a key
(messages are capped at 128 KB).

Configure the R2 Event Notification for raw Logpush objects **only**: `object-create`
with prefix `logs/`. Notifying on the whole bucket would feed `processed/` and marker
files back into `parse-queue`.

## Per-domain configuration

This template is deployed once **per domain**. For each domain, edit `wrangler.toml`
(every field is documented inline) and set the secrets. `<domain>` below is the
per-deployment hostname slug used consistently across the worker name, bucket, and
queue names.

| In `wrangler.toml` | Set to |
|---|---|
| `name` | `<domain>-com-log` |
| `account_id` | the Cloudflare account ID the domain lives in |
| R2 `bucket_name` **and** `[vars].R2_BUCKET_NAME` | `<domain>-com` (must match) |
| Queue names — 2 producers, 4 consumers, and the 4 `[vars]` `*_QUEUE_NAME` / `*_DLQ_NAME` | `parse-queue-<domain>`, `send-queue-<domain>`, `parse-dlq-<domain>`, `send-dlq-<domain>` (keep all references consistent) |
| `[vars].FIELD11_SERVER_IP` | **Required.** The domain's resolved / anycast IP, written into log field #11 `server_ip` (e.g. `172.65.90.64`). If left empty, field #11 is emitted as `-`. |

Secrets (per domain):

```bash
wrangler secret put CTYUN_ENDPOINT
wrangler secret put CTYUN_PRIVATE_KEY
wrangler secret put CTYUN_URI_EDGE
```

For GitHub Actions deployment, set the repository secret `CLOUDFLARE_API_TOKEN`.

All tunable variables (`BATCH_SIZE`, `SEND_PARALLELISM`, `SEND_FETCH_TIMEOUT_MS`,
retry / re-drive delays, `RAW_LOG_PREFIX`/`SUFFIX`, etc.) carry sensible defaults and
are documented inline in `wrangler.toml`; normally you only change the per-domain
identifiers above.

## Output format

- 145 fields separated by `\u0001`, per CDN partner log interface v3.0.
- HTTP body is gzip-compressed; `auth_key = ts-rand-md5(uri-ts-rand-privateKey)`.
- Field #45 maps `EdgeColoCode` to a country code; unmapped values fall back to `SG`.

## How it works

- **Exactly-once-in-effect delivery.** Queue delivery is at-least-once; a deterministic
  per-batch key plus a `.done` marker in R2 make re-processing idempotent, so a batch is
  never POSTed twice. The receiver does **not** need its own de-duplication.
- **Self-healing dead-letter handling.** `parse-dlq` and `send-dlq` are consumed by the
  same Worker, which re-drives messages back to their source queue with exponential
  backoff + jitter. Transient failures recover automatically — no alerts, no manual
  draining, no permanent gaps.
- **Reconciliation safety net (cron, every minute).** R2 Event Notifications are
  best-effort: a notification can be delayed by tens of minutes (observed up to ~53 min)
  or, rarely, dropped — so an object can sit in `logs/` unprocessed (a customer-visible
  gap) even while the event path itself is healthy and the queues are empty. The event
  path cannot self-heal this (the Worker is simply never triggered). A `scheduled()`
  sweep lists recent `logs/` objects and re-enqueues any that are older than
  `RECONCILE_GRACE_SECONDS` and still lack a source-level `.done` marker. Combined with
  the per-batch `.done` idempotency above (and a source-level `.done` early-skip in the
  parser), the re-enqueue never causes a duplicate send. This keeps end-to-end delivery
  inside the customer's window even when a notification is late or missing.
- **Bounded per-invocation work.** The parser handles **one file per invocation**
  (`max_batch_size = 1`), keeping memory and CPU per invocation predictable regardless of
  file size or per-record size. Throughput scales horizontally via consumer autoscaling
  (`max_concurrency` left unset, per-queue limit 250).
- **Streaming send.** The sender streams `R2 object → gzip → fetch body`, so memory per
  request stays small. The request uses HTTP/1.1 `Transfer-Encoding: chunked` (no
  `Content-Length`). **The customer endpoint must accept chunked request bodies**; HTTP
  `400`/`411`/`415` means it does not, and the receiver must enable it.
- **Timeouts & retries.** Each R2 read is bounded by a per-read **idle timeout**
  (`PARSE_READ_IDLE_TIMEOUT_MS`, default 45s): a stalled read (a hung connection — observed
  as 900–1922 s of pure I/O wait) is aborted and that single file retried, so one stuck
  object never blocks the rest of the queue. Because it is an *idle* (per-read) timeout,
  not a whole-file deadline, the same value is safe for every domain regardless of object
  size — a healthy large file emits chunks continuously and never trips it. Each send
  `fetch` is bounded by `SEND_FETCH_TIMEOUT_MS` (default 120s) so a stalled receiver is
  aborted and retried rather than holding an invocation open. Failed parse/send messages
  retry with a delay before, as a last resort, being dead-lettered and then
  auto-re-driven (above).
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
