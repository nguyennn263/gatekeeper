# HTTP API

## Authentication

- Admin endpoints: `Authorization: Bearer <NORTHGATE_ADMIN_TOKEN>`
- Client endpoints: `Authorization: Bearer <NORTHGATE_GATEWAY_KEY>`

`GET /healthz` is intentionally unauthenticated and returns:

```json
{"status":"ok","service":"northgate"}
```

The browser pages (`/`, `/dashboard`, `/app.js`, `/dashboard.js`, and
`/style.css`) are also unauthenticated static assets. Their calls to admin APIs
still require the admin bearer token.

## Admin

### Discover models

`POST /admin/api/discover`

```json
{
  "base_url": "https://api.example.com/v1",
  "api_key": "secret",
  "protocol": "openai"
}
```

`protocol` is one of `openai`, `anthropic`, or `responses`.

`base_url` should include the provider's version prefix (for example
`https://api.example.com/v1`). Northgate appends `/models` here and
`/chat/completions`, `/messages`, or `/responses` when proxying. A bare origin
without a path is treated as `/v1`.

### Enrich selected model metadata

`POST /admin/api/models/enrich`

```json
{
  "base_url": "https://api.example.com/v1",
  "api_key": "secret",
  "protocol": "openai",
  "model_ids": ["model-a"]
}
```

Northgate rechecks the provider's model list and attempts model-detail lookups
for missing context, output, and capability metadata.

### Save provider

`POST /admin/api/providers`

```json
{
  "name": "Example",
  "base_url": "https://api.example.com/v1",
  "api_key": "secret",
  "fallback_api_keys": ["fallback-secret-1", "fallback-secret-2"],
  "protocol": "openai",
  "models": [
    {
      "id": "model-a"
    }
  ]
}
```

Only model IDs are accepted as authority. Northgate repeats discovery and
metadata enrichment during save. Provider names must produce a unique lowercase
slug. Saving is atomic and accepts 1–256 selected models.

`fallback_api_keys` is optional and accepts at most two unique keys. Northgate
always starts every request with the primary `api_key`; fallback keys are tried
in order only after that request attempt fails at the provider.

### Inventory

- `GET /admin/api/providers`
- `DELETE /admin/api/providers/{slug}`
- `POST /admin/api/providers/{slug}/discover` rediscover models using the
  provider's stored primary credential. The credential is never returned.
- `POST /admin/api/providers/{slug}/models` adds selected models to an existing
  provider:

```json
{"models":[{"id":"model-a"}]}
```

  Northgate rediscovers and enriches the selected IDs immediately before the
  atomic insert, so stale or invented model IDs cannot be saved. A provider may
  have at most 256 models in total.
- `DELETE /admin/api/providers/{slug}/models?model=model-a` removes one model
  alias from the registry. Its historical usage remains available as archived
  usage data.
- `PUT /admin/api/providers/{slug}/enabled` with `{"enabled":false}` or
  `{"enabled":true}`
- `PUT /admin/api/providers/{slug}/fallback-keys/{position}` with
  `{"api_key":"secret"}`
- `DELETE /admin/api/providers/{slug}/fallback-keys/{position}`
- `GET /admin/api/health`

Provider responses include an `enabled` boolean, only a masked `api_key_hint`, and ordered
`fallback_api_key_hints` (for example, `sk-pro…9xYz`) so operators can
distinguish credentials. Positions are 1 or 2. Removing position 1 shifts
position 2 forward. Full API keys and encrypted key material are never returned.
Disabling preserves the provider, its models, credentials, and historical usage.
It only removes its models from the Harness-facing catalog and prevents new
requests from resolving those aliases. Enabling makes the same records available
again without rediscovery.

`GET /admin/api/health` returns configured/available upstream capacity and
first-byte/stream-idle timeout settings for read-only operational checks.

### Usage statistics

`GET /admin/api/stats?range=24h`

`range` is one of `24h`, `7d`, `30d`, or `90d`. The response contains totals,
a time series, and a per-model breakdown for requests, failed provider requests,
uncached input tokens, output tokens, and cached input tokens. `failed_requests`
is a subset of `requests` and counts failures attributed to the upstream
provider, including connection failures, provider HTTP errors, timeouts, invalid
provider responses, and stream errors. Token counters are based only on usage
reported by providers; a request is still counted when a provider omits token
usage. Northgate stores hourly numeric aggregates, not prompts or responses.

Each model row includes `active` and `enabled`. Historical rows whose provider/model has been
removed from the registry return `active: false`. An administrator may
permanently delete all stored aggregates for such a model with:

`DELETE /admin/api/stats/models?model=provider%2Fmodel-id`

Northgate returns `409 Conflict` if the model is still active in the registry,
preventing accidental deletion while traffic can still arrive.

### Test a model

`POST /admin/api/models/test`

```json
{
  "model": "example/model-a",
  "stream": true
}
```

Northgate sends a short fixed prompt through the saved model's protocol. With
`stream: false`, the response is OpenAI Chat Completions JSON. With
`stream: true`, the endpoint returns the complete OpenAI-compatible SSE stream
through `[DONE]`. The test counts toward usage statistics.

### Error source

Errors returned as JSON include `error.source`, either `PROVIDER` or
`NORTHGATE`, and the same value is returned in the
`x-northgate-error-source` response header. Error messages are prefixed with
the source for clients that only display `error.message`. Stream failures emit
the same `source: "PROVIDER"` field in the SSE error event before `[DONE]`.
When all configured keys fail, `error.message` lists the sanitized failure for
the primary and every fallback attempt. API key values are never included.

## Harness-facing OpenAI API

### Models

`GET /v1/models` returns models selected from enabled providers. Models belonging
to disabled providers are omitted, and direct requests to their aliases are
rejected by Northgate until the provider is enabled again.
Example IDs:

```text
anthropic/claude-sonnet-4-5
openrouter/openai/gpt-5
local/qwen3-coder
```

### Chat completions

`POST /v1/chat/completions` accepts an OpenAI Chat Completions body. Every
standard field is preserved for an OpenAI upstream. For Anthropic and Responses
upstreams, fields with equivalent protocol semantics are converted. Unknown
extension fields are forwarded unchanged. Known standard fields without a safe
target representation return a clear 400 error instead of disappearing.

Both JSON and `stream: true` responses use OpenAI Chat Completions shapes.
Optional fields sent as `null` are treated as absent. For Anthropic upstreams,
`temperature` takes precedence when `top_p` is also set, empty text parts are
dropped, and `data:` image URLs are converted to inline base64 sources.
`usage.prompt_tokens` includes cached input, with the cached share reported in
`usage.prompt_tokens_details.cached_tokens`.

Fallback attempts are sequential and remain within the same inbound request.
For streaming calls, Northgate may switch keys only before any SSE output has
been sent to the harness. If a provider stream fails after output starts,
Northgate returns a provider error event and does not mix output from another
generation.
