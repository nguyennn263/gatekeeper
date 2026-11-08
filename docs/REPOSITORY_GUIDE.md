# Repository guide for maintainers and agents

This document is the starting point for an agent or maintainer changing
Northgate. It describes the repository as implemented, its invariants, and the
smallest safe places to make common changes. Read [API.md](API.md) for endpoint
payloads, [ARCHITECTURE.md](ARCHITECTURE.md) for runtime behavior, and
[DOCKER.md](DOCKER.md) for container operations.

## Product boundary

Northgate is a persistent registry of upstream LLM models and an OpenAI Chat
Completions gateway. An operator saves a provider and selected models once;
harnesses refer to models by a stable public alias:

```text
<provider-slug>/<upstream-model-id>
```

It supports these saved upstream protocols:

| Protocol | Endpoint appended to provider base URL | Role |
| --- | --- | --- |
| `openai` | `/chat/completions` | OpenAI Chat Completions passthrough |
| `anthropic` | `/messages` | Anthropic Messages translation |
| `responses` | `/responses` | OpenAI Responses / Codex-style translation |

The public API is deliberately small: `GET /v1/models` and
`POST /v1/chat/completions`. Northgate does not route between models, load
balance, impose quotas, retain prompts/responses, or promote fallback keys.

## Fast orientation

```text
main.rs
  -> loads Config, prepares the SQLite path, opens Store, serves app::router

app.rs
  -> routes, auth middleware, onboarding, proxying, retries/fallback,
     usage aggregation, SSRF checks, and error shaping

store.rs
  -> schema/migrations, encrypted credentials, registry, usage aggregates,
     one dedicated SQLite worker

protocol/
  -> request/response/SSE adapters; no HTTP client or database access

discovery.rs
  -> upstream model discovery and metadata enrichment

web.rs + web/
  -> embedded static admin and dashboard UI
```

[`tests/gateway.rs`](../tests/gateway.rs) is the highest-value executable
specification. It creates mock upstreams and covers onboarding, translation,
streaming, timeout, fallback, usage, and provider-disable behavior end to end.

## Repository map

| Path | Responsibility | Change here when… |
| --- | --- | --- |
| `src/main.rs` | Bootstrap, database-file permissions, graceful shutdown | startup or native process behavior changes |
| `src/config.rs` | Environment parsing/validation and token handling | adding a runtime setting |
| `src/auth.rs` | Strict constant-time bearer validation | changing auth semantics |
| `src/error.rs` | OpenAI-shaped error status/code/source mapping | adding an application failure category |
| `src/app.rs` | Routes and request orchestration | adding an endpoint or proxy behavior |
| `src/store.rs` | Schema, migrations, encrypted data, registry, aggregates | changing persisted/catalog data |
| `src/discovery.rs` | `/v1/models` discovery and normalized metadata | supporting model catalog variations |
| `src/protocol/common.rs` | Protocol enum and redacted transport request type | protocol-wide changes |
| `src/protocol/endpoint.rs` | Safe provider URL joining | changing allowed upstream URL forms |
| `src/protocol/openai.rs` | OpenAI passthrough | OpenAI mapping changes |
| `src/protocol/anthropic.rs` | Chat Completions ↔ Anthropic Messages | Anthropic mapping changes |
| `src/protocol/responses.rs` | Chat Completions ↔ Responses | Responses/Codex mapping changes |
| `src/protocol/sse.rs` | Incremental SSE parsing/conversion | streaming behavior or bounds |
| `src/web.rs` and `web/` | Static UI embedded with `include_str!` | changing browser UI/assets |
| `tests/gateway.rs` | Black-box gateway tests | externally observable behavior changes |
| `Dockerfile`, `compose.yaml` | Container image and hardened default runtime | Docker build/runtime changes |
| `deploy/` | Caddy and systemd examples | non-Docker deployment guidance |
| `scripts/init-env.sh` | One-time secret generation | environment defaults/required variables |
| `scripts/check.sh` | Required repository checks | adding a required verification step |

## Runtime ownership boundaries

Northgate is one Tokio process. HTTP work is async. SQLite never runs on a
Tokio executor: `Store::open` starts one dedicated worker with a bounded queue
of 32 commands. New storage behavior must use the existing command/reply worker
pattern rather than call SQLite directly from an async route.

`app.rs` owns network concerns: body limits (4 MiB inbound), upstream permits,
timeouts, DNS, HTTP-client caching, retries, and public error responses.
`protocol/` must stay
transport-neutral: it converts JSON/SSE but must not acquire a Reqwest client,
database handle, or logging path that can expose credentials.

The browser is not authoritative. Provider save repeats model discovery and
metadata enrichment; only selected model IDs are trusted from the browser. UI
secrets live only in page memory, while saved provider credentials are encrypted
before reaching SQLite.

## Data model and migration rules

SQLite runs with WAL, foreign keys, `synchronous=NORMAL`, and a five-second busy
timeout. Its database file is the only persistent application state.

| Table | Lifecycle |
| --- | --- |
| `providers` | unique slug, name, base URL, protocol, encrypted primary key, enabled flag, timestamp |
| `provider_fallback_keys` | at most two encrypted keys in positions 1–2; cascade-delete with provider |
| `models` | selected upstream model and public alias; cascade-delete with provider |
| `usage_hourly` | hourly numeric aggregates by slug and alias; no FK so usage survives a provider deletion |

Credentials are XChaCha20-Poly1305 encrypted with a fresh 24-byte nonce. The
32-byte base64-decoded `NORTHGATE_MASTER_KEY` is required to decrypt saved
credentials. Losing/rotating it without a credential migration makes current
provider keys unusable. Do not log, serialize, or add `Debug` output that
reveals a `SecretString`.

Migrations live in `configure_database` in `store.rs`. They must be additive
and safe for existing data. Current legacy migrations add
`usage_hourly.failed_requests` with default zero and `providers.enabled` with
default true. Every new migration needs a focused test that opens a legacy
schema first.

Deleting a provider is destructive: models and fallback credentials cascade
away, but its usage row becomes archived. Disabling is reversible: it preserves
all stored data, hides models from `/v1/models`, and prevents new alias
resolution. It does not cancel a request that already resolved the provider.

## Request lifecycle

```text
Harness request
  -> gateway bearer check before JSON body extraction
  -> validate public model
  -> Store resolves enabled alias and decrypts primary/fallback keys
  -> record one request aggregate
  -> acquire nonblocking upstream permit
  -> validate/pin provider origin and create/reuse HTTP client
  -> primary key, fallback 1, fallback 2 on provider failures only
  -> convert JSON or incrementally convert SSE to OpenAI Chat Completions
  -> record provider usage and final provider failure when applicable
```

Inbound concurrency and upstream concurrency are separate, nonblocking limits.
Exhaustion returns `503 gateway_busy`, not an unbounded queue. Non-stream
success bodies are capped at 8 MiB; provider error bodies at 256 KiB. Discovery
is capped at 2,000 models and 4 MiB.

### Fallback and streaming invariants

Every request begins with the primary key. A fallback runs only after a
provider-class failure (transport, timeout, non-success HTTP, invalid/oversized
body, or pre-output stream failure). It is never proactive and is never
persisted as the new primary. A successful fallback is a successful request and
does not increment `failed_requests`.

For SSE, fallback is safe only before any frame reaches the harness. After an
output frame, retry could duplicate or mix a generation; Northgate therefore
emits a provider SSE error event followed by `[DONE]`. Streaming changes must
preserve this rule and release the upstream permit when the stream ends or is
dropped.

### Usage/token semantics

Only hourly numeric aggregates are stored; prompts, outputs, test text, and
per-request records are never stored. `requests` increments after model
resolution and before upstream dispatch. `failed_requests` increments only for
the final provider failure. Cached input is stored separately and excluded from
`input_tokens` to avoid double counting.

| Protocol | Input | Output | Cache |
| --- | --- | --- | --- |
| OpenAI | `prompt_tokens` or `input_tokens`, minus cache | `completion_tokens` or `output_tokens` | `prompt_tokens_details.cached_tokens` or `input_tokens_details.cached_tokens` |
| Responses | `input_tokens`, minus cache | `output_tokens` | `input_tokens_details.cached_tokens` |
| Anthropic | `input_tokens + cache_creation_input_tokens` | `output_tokens` | `cache_read_input_tokens` |

If the provider omits usage, tokens are zero while the request count remains.

## Security invariants

- Admin and gateway tokens are distinct 16–4096 byte secrets and use
  constant-time comparison. Their middleware runs before JSON body extraction.
- Provider keys are validated, zeroized where practical, encrypted at rest,
  redacted in `Debug`, and never returned by APIs. Inventory exposes hints only.
- Provider URLs accept only HTTP(S), no embedded credentials/query/fragment,
  and no unsafe path traversal. Redirects are disabled.
- DNS is resolved before client construction. Public addresses are pinned in a
  five-minute origin-client cache. Private, loopback, link-local, reserved, and
  metadata-style addresses are rejected unless private upstreams are explicitly
  enabled.
- Public deployments should retain loopback binding and enforce TLS/domain
  policy at a reverse proxy; `deploy/Caddyfile.example` is the reference.

Do not relax SSRF behavior or add arbitrary URL fetching without direct tests
for reserved/private addresses and redirects.

## Configuration reference

| Variable | Default | Meaning / validation |
| --- | --- | --- |
| `NORTHGATE_MASTER_KEY` | required | standard base64 decoding to exactly 32 bytes; encrypts provider credentials |
| `NORTHGATE_ADMIN_TOKEN` | required | 16–4096 bytes; protects `/admin/api/*` |
| `NORTHGATE_GATEWAY_KEY` | required | 16–4096 bytes; protects `/v1/*`; differs from admin token |
| `NORTHGATE_BIND` | `127.0.0.1:8787` | socket address |
| `NORTHGATE_DATABASE_PATH` | `data/northgate.db` | persistent SQLite location |
| `NORTHGATE_MAX_UPSTREAM_CONCURRENCY` | `32` | integer 1–128 |
| `NORTHGATE_MAX_INBOUND_CONCURRENCY` | `64` | integer 1–256 |
| `NORTHGATE_FIRST_BYTE_TIMEOUT_SECS` | `300` | integer 1–900; provider headers/first SSE event |
| `NORTHGATE_STREAM_IDLE_TIMEOUT_SECS` | `0` | zero disables application-level SSE idle deadline |
| `NORTHGATE_ALLOW_PRIVATE_UPSTREAMS` | `false` | accepts true/false, yes/no, or 1/0; trusted internal providers only |

Connect timeout is fixed at 10 seconds, body-idle timeout at 60 seconds, and
inbound request-body timeout at 30 seconds. Docker-only `NORTHGATE_DOCKER_*`
variables are expanded by Compose; see [DOCKER.md](DOCKER.md).

## HTTP surface

| Route | Auth | Purpose |
| --- | --- | --- |
| `GET /`, `/dashboard` | none | static admin UI shells |
| `GET /app.js`, `/dashboard.js`, `/style.css` | none | embedded static assets |
| `GET /healthz` | none | liveness response |
| `GET /admin/api/health` | admin | upstream capacity and timeout settings |
| `POST /admin/api/discover` | admin | model discovery |
| `POST /admin/api/models/enrich` | admin | selected-model metadata enrichment |
| `GET`, `POST /admin/api/providers` | admin | inventory / atomic provider creation |
| `DELETE /admin/api/providers/{slug}` | admin | destructive provider deletion |
| `POST /admin/api/providers/{slug}/discover` | admin | rediscover using the saved credential |
| `POST`, `DELETE /admin/api/providers/{slug}/models` | admin | add selected models / remove one model |
| `PUT /admin/api/providers/{slug}/enabled` | admin | reversible enable/disable |
| `PUT`, `DELETE /admin/api/providers/{slug}/fallback-keys/{position}` | admin | fallback credential management |
| `GET /admin/api/stats` | admin | aggregates plus active/enabled model state |
| `DELETE /admin/api/stats/models` | admin | delete only archived model usage |
| `POST /admin/api/models/test` | admin | fixed-prompt JSON or live SSE test |
| `GET /v1/models` | gateway | enabled public aliases |
| `POST /v1/chat/completions` | gateway | OpenAI-compatible proxy |

JSON errors are OpenAI-shaped. Both `error.source` and
`x-northgate-error-source` are `NORTHGATE` for local/auth/storage/capacity
failures and `PROVIDER` for upstream failures. See [API.md](API.md) for payloads.

## Frontend

There is no asset build step. `web.rs` compiles the files in `web/` into the
binary with `include_str!`, so a server rebuild is required after UI edits. The
registry UI handles discovery, selection, saving, fallback slots, and provider
enablement. The dashboard groups/searches/filters models, renders aggregates,
and reads live test SSE through `ReadableStream` so frames render immediately.

When changing browser API behavior, update the matching handler and error UI,
then run Node syntax checks. Keep using DOM `textContent` for provider-derived
values; do not build HTML from remote strings.

## Development and verification

Requirements: pinned Rust in `rust-toolchain.toml`, Python 3 for secret
generation, and Node.js for JavaScript syntax checks. `.env.example` is a
template; `./scripts/init-env.sh` creates a mode-600 `.env` and refuses to
overwrite an existing file.

```bash
./scripts/init-env.sh     # once, only if .env does not exist
cargo run --release
./scripts/check.sh
```

`scripts/check.sh` runs formatting, target checks, tests, Clippy with warnings
as errors, and both JavaScript syntax checks. Gateway integration tests bind
temporary loopback ports and use their own temporary SQLite databases; never
run them against production secrets or a production database.

## Agent change checklist

1. Read this guide and the directly affected source/test files.
2. Preserve alias, credential, and schema compatibility unless an explicit
   migration/upgrade plan is part of the change.
3. For routes: update `app.rs`, [API.md](API.md), UI behavior if applicable,
   and an integration test.
4. For storage: use an additive migration in `configure_database`, add a
   legacy-schema test, and review encryption/zeroization.
5. For protocol changes: update request conversion, response conversion, SSE,
   usage extraction, and relevant protocol tests.
6. For failures: classify `PROVIDER` vs `NORTHGATE` correctly and ensure no key
   leaks in JSON, SSE, tracing, or debug output.
7. For Docker/runtime changes: retain non-root execution, read-only root,
   persistent `/data`, and loopback-only publication unless explicitly changed.
8. Run `./scripts/check.sh` and update documentation in the same change.

## Packaging exclusions

Never ship `.env`, `data/`, `*.db`, SQLite WAL/SHM files, `target/`, `.git/`,
or previous archive files. They contain secrets/state, are rebuildable, or are
repository metadata. A source archive should include all tracked-style source,
documentation, scripts, deployment examples, `Cargo.lock`, and hidden project
configuration such as `.dockerignore` and `.env.example`.
