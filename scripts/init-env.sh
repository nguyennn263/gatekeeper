#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -e .env ]]; then
  echo ".env already exists; refusing to overwrite it" >&2
  exit 1
fi

umask 077
master_key="$(python3 -c 'import base64,secrets; print(base64.b64encode(secrets.token_bytes(32)).decode())')"
admin_token="ng_admin_$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
gateway_key="ng_key_$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"

{
  echo "NORTHGATE_MASTER_KEY=$master_key"
  echo "NORTHGATE_ADMIN_TOKEN=$admin_token"
  echo "NORTHGATE_GATEWAY_KEY=$gateway_key"
  echo "NORTHGATE_BIND=127.0.0.1:8787"
  echo "NORTHGATE_DATABASE_PATH=data/northgate.db"
  echo "NORTHGATE_MAX_UPSTREAM_CONCURRENCY=32"
  echo "NORTHGATE_MAX_INBOUND_CONCURRENCY=64"
  echo "NORTHGATE_FIRST_BYTE_TIMEOUT_SECS=300"
  echo "NORTHGATE_STREAM_IDLE_TIMEOUT_SECS=0"
  echo "NORTHGATE_ALLOW_PRIVATE_UPSTREAMS=false"
} > .env

echo "Created .env with mode 600"
echo "Admin token: $admin_token"
echo "Gateway key: $gateway_key"
echo "Save these values in your password manager."
