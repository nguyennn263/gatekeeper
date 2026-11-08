# Architecture

## Scope

Northgate is a persistent catalog of upstream models and a translation proxy.
It deliberately has no automatic model routing, quotas, budgets, reasoning
policy, or per-request usage warehouse. Providers may have up to two encrypted
API-key fallbacks.
Only numeric hourly usage aggregates are retained.

```text
OpenAI-compatible harness
          |
          | GET /v1/models
          | POST /v1/chat/completions
          v
       Northgate
          |
          +-- OpenAI Chat Completions
          +-- Anthropic Messages
          `-- OpenAI Responses / Codex tool calling
```

## Runtime

The runtime is one Rust process using Axum, Reqwest/Rustls, and SQLite through a
single bounded database worker. HTML, CSS, and JavaScript are embedded in the
binary. The default bind is `127.0.0.1:8787`.

Configuration is supplied through environment variables:

- `NORTHGATE_MASTER_KEY`: 32 random bytes encoded as base64; encrypts provider
  API keys with XChaCha20-Poly1305.
- `NORTHGATE_ADMIN_TOKEN`: protects provider administration and discovery.
- `NORTHGATE_GATEWAY_KEY`: bearer key used by harnesses.
- `NORTHGATE_DATABASE_PATH`: defaults to `data/northgate.db`.
- `NORTHGATE_BIND`: defaults to `127.0.0.1:8787`.
- `NORTHGATE_MAX_UPSTREAM_CONCURRENCY`: defaults to 32.
- `NORTHGATE_MAX_INBOUND_CONCURRENCY`: defaults to 64 and bounds concurrent
  request extraction/dispatch.
- `NORTHGATE_FIRST_BYTE_TIMEOUT_SECS`: defaults to 300 and bounds the wait for
  provider response headers/first streamed output; accepted range is 1–900.
- `NORTHGATE_STREAM_IDLE_TIMEOUT_SECS`: defaults to `0` (disabled). Set a
  positive value only if the operator wants an application-level stream idle
  deadline.
- `NORTHGATE_ALLOW_PRIVATE_UPSTREAMS`: defaults to `false`. It must be enabled
  explicitly for trusted localhost or private-network providers.

Secrets are accepted only over the admin API, encrypted before persistence, and
never logged. Inventory responses contain only a masked first/last-character
hint for primary and fallback provider keys. Admin and gateway comparisons use constant-time
verification. The browser keeps the admin token and provider key in memory
only.

## Provider onboarding

1. The operator enters a base URL, provider API key, and compatibility type.
2. `POST /admin/api/discover` calls the provider's model endpoint using the
   supplied credential. It normalizes common model metadata fields including
   context window, maximum output, ownership, and capabilities.
3. The UI displays the discovered models. As models are selected,
   `POST /admin/api/models/enrich` retrieves model-detail records when the list
   omitted context or capability metadata.
4. The operator enters a provider name. `POST /admin/api/providers`
   independently repeats discovery, rejects IDs the provider no longer
   exposes, enriches missing metadata, encrypts the API key, and atomically
   saves the provider and selected models. Browser-supplied metadata is never
   trusted.
5. The public alias is `<provider-slug>/<upstream-model-id>`. The provider slug
   is stable after creation. Upstream model IDs may themselves contain `/`.
6. The saved base URL is used verbatim for `/models`, `/chat/completions`,
   `/messages`, and `/responses`; only a bare origin receives a `/v1` prefix.

The operator may configure up to two fallback keys while saving a provider or
manage the encrypted fallback slots later from Inventory. Duplicate primary or
fallback credentials are rejected.

A saved provider can be disabled and enabled from Inventory. Disable is a
reversible registry state: models, encrypted credentials, fallback slots, and
usage aggregates remain stored, but disabled models are excluded from
`GET /v1/models` and cannot resolve for new proxy requests. Existing providers
from databases created before this feature migrate as enabled.

Discovery metadata is best effort because model-list APIs do not all publish a
context size. Northgate checks common fields (`context_length`,
`context_window`, `max_context_length`, `max_input_tokens`, and nested limits).
It then attempts the provider's model-detail endpoint for each selected model.
An absent value is stored and displayed as unknown; Northgate does not invent a
limit when the provider publishes none.

## Request path

The client sends an OpenAI Chat Completions request. Northgate validates the
body size and `model`, resolves the alias in SQLite, decrypts the provider key,
and acquires a bounded upstream permit.

Provider enablement is checked during alias resolution. Disabling does not
cancel a request that already resolved and started; it blocks subsequent
requests after the state update completes.

Every request starts with the primary provider key. Provider-class failures
(connection/header timeout, non-success HTTP response, invalid response body,
or pre-output stream failure) advance sequentially through fallback slot 1 and
slot 2. A successful fallback completes the original request and is not counted
as a failed request. Northgate never permanently promotes a fallback key.

- OpenAI Chat Completions: preserve the complete JSON object and replace only
  `model` with the upstream model ID.
- Anthropic Messages: map messages, system/developer instructions, multimodal
  content, tools, tool choices, token limits, sampling fields, and tool results.
- OpenAI Responses: map chat messages and tool history to Responses input
  items, flatten function definitions, and map tool choice, structured output,
  reasoning, logprob, and generation fields.

Protocol-independent extension fields are forwarded unchanged. A standard
OpenAI field that has no safe equivalent is rejected with an OpenAI-shaped 400
error instead of being silently discarded.

Non-stream responses are bounded before conversion. Stream responses are parsed
incrementally and emitted as OpenAI `chat.completion.chunk` SSE frames. The
gateway never waits for the full stream. Terminal protocol events immediately
emit one `[DONE]` and release the upstream connection even when the provider
keeps its socket open. Backpressure propagates from the client socket to the
upstream body, and dropping the downstream stream drops the upstream response.

## Resource limits

- Request body: 4 MiB. Requests over this limit receive a 413 response;
  malformed JSON remains a 400 response.
- Non-stream success body: 8 MiB; error body: 256 KiB.
- Global upstream concurrency: 32 by default, configurable down to 1.
- Concurrent inbound dispatch: 64 by default, load-shed with `503 gateway_busy`
  when exceeded; bearer authentication happens before JSON body extraction.
- Connect timeout: 10 seconds; provider response-header/first-byte timeout: 300
  seconds by default.
- Non-stream/discovery body idle timeout: 60 seconds per chunk.
- Stream idle timeout: disabled by default; no total stream-duration timeout.
- SQLite busy timeout: 5 seconds and WAL mode.
- Model discovery: at most 2,000 models and 4 MiB.
- Saved models per provider: at most 256.
- SSE input: 1 MiB per event, 4 MiB or 4,096 events per decoded network batch.
- No prompt/response persistence and no per-request task fan-out.

These bounds keep normal operation well below 1 GB RAM on one CPU while still
allowing long-lived streams.

The supplied Compose profile tightens this further to 16 upstream and 32
inbound requests, a 256 MiB hard memory limit, one CPU, a read-only non-root
container, and a persistent SQLite volume.

## Failure behavior

Northgate returns OpenAI-shaped JSON errors before streaming starts. Every JSON
error carries `error.source` and `x-northgate-error-source` as either
`PROVIDER` or `NORTHGATE`. After a stream starts, upstream protocol errors become
an SSE error event marked `source: "PROVIDER"` followed by `[DONE]`. Provider
failures increment the hourly `failed_requests` aggregate only when all usable
keys fail. Error messages list every sanitized provider attempt. A stream may
fallback only before any downstream frame is emitted; after output begins,
retrying could duplicate or mix a generation, so Northgate emits the error
without retrying. HTTP redirects are disabled so credentials are
not forwarded to another origin.

Provider DNS is resolved and checked before use. Public-provider addresses are
pinned into a short-lived client cache to prevent DNS rebinding; private,
loopback, link-local, and reserved addresses are rejected unless the operator
explicitly enables private upstreams.
