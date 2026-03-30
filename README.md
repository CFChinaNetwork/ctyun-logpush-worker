# ctyun-logpush-worker

Cloudflare Worker — transforms and forwards Logpush logs to a CDN partner log ingestion endpoint.

## Architecture

```
CF Edge → Logpush → R2 (cdn-logs-raw)
  → R2 Event Notification → parse-queue
  → Parser Worker (streaming gzip decompress + field mapping)
  → R2 (processed/) → send-queue
  → Sender Worker (gzip + MD5 auth + HTTP POST)
  → Customer log ingestion server
```

## Format

145-field `\u0001`-delimited plaintext format per CDN partner log interface spec v3.0.

## Environment Variables

| Name | Type | Description |
|---|---|---|
| `CLOUDFLARE_API_TOKEN` | Secret (GitHub) | Cloudflare API token for deployment |
| `CTYUN_ENDPOINT` | Secret | Customer log server base URL |
| `CTYUN_PRIVATE_KEY` | Secret | Customer authentication private key |
| `CTYUN_URI_EDGE` | Secret | Customer log POST URI path |
| `BATCH_SIZE` | Var | Log lines per POST request (default: 1000) |
| `LOG_LEVEL` | Var | Logging verbosity: `info` or `debug` |

## Deployment

Push to `main` branch triggers automatic deployment via GitHub Actions.

## Optional: Enable Content-Length Logging (Field #21)

By default, field #21 (`sent_http_content_length`) uses `EdgeResponseBodyBytes` as fallback. To capture the true `Content-Length` response header, configure Logpush Custom Fields via the Cloudflare API (one-time zone-level setup). See the deployment guides in `docs/` for the full API configuration steps.

## Documentation

| Language | File |
|---|---|
| English | [CF Logpush — Format Transform & Push Guide](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF-Logpush-Format-Transform-and-Push-Guide.html) |
| 中文 | [CF 日志格式转换与自动推送指南](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF%E6%97%A5%E5%BF%97%E6%A0%BC%E5%BC%8F%E8%BD%AC%E6%8D%A2%E4%B8%8E%E8%87%AA%E5%8A%A8%E6%8E%A8%E9%80%81%E6%8C%87%E5%8D%97.html) |
