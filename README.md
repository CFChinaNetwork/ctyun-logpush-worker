# ctyun-logpush-worker

Cloudflare Worker that transforms Cloudflare Logpush `http_requests` logs into the CDN partner 145-field format and forwards them to the customer log ingestion endpoint.

## Flow

```text
Cloudflare Logpush -> R2 logs/
  -> R2 Event Notification -> parse-queue
  -> Parser Worker -> R2 processed/*.txt
  -> send-queue -> Sender Worker -> gzip + auth_key POST -> customer endpoint
```

Configure the R2 Event Notification for raw Logpush objects only: `object-create` with prefix `logs/`. Notifying on the full bucket would feed `processed/` files and marker files back into `parse-queue`.

## Configure

Edit `wrangler.toml` (`account_id`, `name`, R2 `bucket_name` and `R2_BUCKET_NAME`, queue names if customised) and set the three secrets:

```bash
wrangler secret put CTYUN_ENDPOINT
wrangler secret put CTYUN_PRIVATE_KEY
wrangler secret put CTYUN_URI_EDGE
```

For GitHub Actions deployment, also set repository secret `CLOUDFLARE_API_TOKEN`.

Per-variable semantics (BATCH_SIZE, RAW_LOG_PREFIX/SUFFIX, etc.) are documented inline in `wrangler.toml`.

## Output Format

- 145 fields separated by `\u0001`, per CDN partner log interface v3.0.
- HTTP body is gzip-compressed; `auth_key = ts-rand-md5(uri-ts-rand-privateKey)`.
- Field #45 maps `EdgeColoCode` to country; unmapped values fall back to `SG`.

## Reliability Notes

- Queue delivery is at-least-once. `.done` markers in R2 narrow the duplicate-POST window from Queue redelivery.
- `send-queue` consumer leaves `max_concurrency` unset to allow autoscaling; messages within each invocation are processed by a small parallel pool (`SEND_PARALLELISM`, default 2).
- Sender uses streaming gzip + chunked request body. **Customer endpoint must accept HTTP/1.1 `Transfer-Encoding: chunked`** (no `Content-Length`). If you see HTTP `400`/`411`/`415`, the endpoint does not support chunked bodies and the customer side needs to enable it.
- Each `fetch` to the customer endpoint is bounded by `AbortSignal.timeout(30_000)`. A hang on the receiver side is aborted after 30s and falls back to queue retry, preventing a single invocation from exhausting the Queue consumer's 15-minute wall-time limit.
- The parser ignores non-raw R2 objects (defaults: only `logs/...*.log.gz`).

## PUSH_START_TIME

`PUSH_START_TIME` controls when forwarding starts:

| Value | Behavior |
|---|---|
| Empty string | No filtering; process all new logs |
| Future ISO time | Skip files/records before that time until cutover |
| Past ISO time | Cron scans `logs/YYYYMMDD/` once and re-enqueues files in `[PUSH_START_TIME, now]` |

Past-time recovery is one-shot per exact `PUSH_START_TIME` value and capped at 62 days. For wider ranges or precise `[A, B]` backfill use [`CFChinaNetwork/ctyun-logpush-backfill`](https://github.com/CFChinaNetwork/ctyun-logpush-backfill).

## Deploy

Push to `main` triggers GitHub Actions deployment. To validate locally:

```bash
npx wrangler deploy --dry-run
```

## Documentation

| Language | Guide |
|---|---|
| English | [CF Logpush - Format Transform & Push Guide](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF-Logpush-Format-Transform-and-Push-Guide.html) |
| Chinese | [CF 日志格式转换与自动推送指南](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF%E6%97%A5%E5%BF%97%E6%A0%BC%E5%BC%8F%E8%BD%AC%E6%8D%A2%E4%B8%8E%E8%87%AA%E5%8A%A8%E6%8E%A8%E9%80%81%E6%8C%87%E5%8D%97.html) |
