# Northgate

Northgate is a small model registry and protocol gateway. Configure an upstream
provider once, select the models to keep, and point OpenAI-compatible harnesses
at one stable endpoint.

The public model IDs are always `provider/model`. Northgate does not rank,
substitute, rate-limit, or tune models. It resolves the selected model and
forwards the request to the configured upstream protocol.

## Product contract

- Provider onboarding: URL, API key, compatibility type, model discovery,
  model selection, provider name, save.
- Compatibility types: OpenAI Chat Completions, Anthropic Messages, and OpenAI
  Responses (Codex-compatible tool calling).
- OpenAI-compatible client surface: `GET /v1/models` and
  `POST /v1/chat/completions`.
- Provider keys are encrypted at rest. Prompts and responses are never stored.
- The admin dashboard shows hourly request and provider-reported input, output,
  and cached-token aggregates for 24 hours to 90 days.
- Saved models can be tested from the dashboard in JSON or live full-SSE mode.
- Streaming is incremental and bounded; client cancellation cancels the
  upstream response.
- One process, one SQLite file, embedded web UI, suitable for 1 CPU and 1 GB RAM.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/API.md](docs/API.md).

## Run locally

Requirements: Rust 1.97.1, Python 3, and Node.js (Node is used only by checks).

```bash
./scripts/init-env.sh
cargo run --release
```

Open `http://127.0.0.1:8787`, paste the generated admin token, and configure a
provider. Usage and live model tests are available at
`http://127.0.0.1:8787/dashboard`. Configure each harness once with:

```text
base_url = http://127.0.0.1:8787/v1
api_key  = <NORTHGATE_GATEWAY_KEY>
model    = <provider-slug>/<upstream-model-id>
```

Run the repository checks with `./scripts/check.sh`.

## Docker

Northgate includes a multi-stage, non-root Docker image and a resource-limited
Compose deployment:

```bash
docker compose up -d --build
```

The default container is limited to 1 CPU and 256 MiB RAM, publishes only on
`127.0.0.1:8787`, and stores SQLite in a named volume. See
[docs/DOCKER.md](docs/DOCKER.md) for operation, tuning, upgrades, and connecting
to providers running on the Docker host.

Private-network providers are blocked by default. Set
`NORTHGATE_ALLOW_PRIVATE_UPSTREAMS=true` only when the URL intentionally points
to a trusted localhost or LAN service. Long streams have no application-level
idle timeout by default; set `NORTHGATE_STREAM_IDLE_TIMEOUT_SECS` to a positive
value if your deployment requires one.

## Production service

Build once and install the stripped binary:

```bash
cargo build --release --locked
sudo useradd --system --home /var/lib/northgate --shell /usr/sbin/nologin northgate
sudo install -d -m 0755 -o root -g root /opt/northgate
sudo install -d -m 0700 -o northgate -g northgate /var/lib/northgate
sudo install -m 0755 target/release/northgate /opt/northgate/northgate
sudo install -m 0644 deploy/northgate.service /etc/systemd/system/northgate.service
sudo install -m 0600 -o root -g root .env /etc/northgate.env
sudoedit /etc/northgate.env
sudo systemctl daemon-reload
sudo systemctl enable --now northgate
```

In `sudoedit`, set
`NORTHGATE_DATABASE_PATH=/var/lib/northgate/northgate.db`. The supplied unit
expects that writable path. Re-running `useradd` on an existing installation is
unnecessary.

Keep Northgate bound to loopback and terminate TLS at a reverse proxy. An
example Caddy configuration is in `deploy/Caddyfile.example`. If exposing the
admin UI beyond localhost, protect it with an additional network access policy;
the admin bearer token remains mandatory.
