# Docker deployment

## Start

Generate secrets once if `.env` does not exist:

```bash
./scripts/init-env.sh
```

Then build and start Northgate:

```bash
docker compose up -d --build
docker compose ps
```

Open `http://127.0.0.1:8787`. The Compose file overrides the in-container bind
address and database path, so the existing native `.env` can be reused safely.
Secrets are injected at runtime and `.env` is excluded from the image.

Stop without deleting data:

```bash
docker compose down
```

Remove the container and its database volume only when the data is no longer
needed:

```bash
docker compose down --volumes
```

## Resource profile

The runtime image contains only the stripped Rust binary, Alpine, and CA
certificates. It runs as UID/GID `10001`, with a read-only root filesystem and
all Linux capabilities dropped. Compose applies these defaults:

- 1 CPU
- 256 MiB hard memory limit and 64 MiB reservation
- 64-process limit
- 16 concurrent provider requests
- 32 concurrent inbound requests
- 8 MiB temporary filesystem
- two rotated 5 MiB log files

To tune concurrency without editing `compose.yaml`, add either variable to
`.env`:

```env
NORTHGATE_DOCKER_MAX_UPSTREAM_CONCURRENCY=8
NORTHGATE_DOCKER_MAX_INBOUND_CONCURRENCY=16
NORTHGATE_DOCKER_PORT=8788
NORTHGATE_FIRST_BYTE_TIMEOUT_SECS=300
```

These Docker-only variables are expanded by Compose and are not read directly
by Northgate. `NORTHGATE_DOCKER_PORT` is useful when a native Northgate process
already occupies port `8787`.

## Data and upgrades

SQLite is stored in the named volume `northgate-data`. Rebuilding or replacing
the container preserves that volume:

```bash
docker compose build --pull
docker compose up -d
```

The native `data/northgate.db` is intentionally not mounted because its host
ownership and permissions may not match the non-root container user. To import
an existing database, stop both native Northgate and the container first, then
copy the database into the named volume with an explicit backup procedure.
Never copy a live SQLite database without its WAL/SHM files.

## Local providers

Inside a container, `127.0.0.1` refers to the Northgate container itself. For a
provider running on the Docker host, use a URL based on
`http://host.docker.internal:<port>` and set:

```env
NORTHGATE_ALLOW_PRIVATE_UPSTREAMS=true
```

Only enable private upstreams for trusted host or LAN services.
