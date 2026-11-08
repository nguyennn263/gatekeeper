use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, bail};
use base64::Engine as _;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

#[derive(Clone)]
pub struct Secret(Arc<Zeroizing<String>>);

impl Secret {
    pub fn new(value: String, name: &str) -> anyhow::Result<Self> {
        if value.len() < 16 || value.len() > 4096 {
            bail!("{name} must contain between 16 and 4096 bytes");
        }
        Ok(Self(Arc::new(Zeroizing::new(value))))
    }

    pub fn expose(&self) -> &str {
        self.0.as_str()
    }

    pub fn matches(&self, candidate: &str) -> bool {
        let expected = self.0.as_bytes();
        let candidate = candidate.as_bytes();
        expected.len() == candidate.len() && bool::from(expected.ct_eq(candidate))
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub database_path: PathBuf,
    pub master_key: [u8; 32],
    pub admin_token: Secret,
    pub gateway_key: Secret,
    pub max_upstream_concurrency: usize,
    pub max_inbound_concurrency: usize,
    pub connect_timeout: Duration,
    pub first_byte_timeout: Duration,
    pub body_idle_timeout: Duration,
    pub stream_idle_timeout: Option<Duration>,
    pub request_body_timeout: Duration,
    pub allow_private_upstreams: bool,
}

impl Drop for Config {
    fn drop(&mut self) {
        self.master_key.zeroize();
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let _ = dotenvy::dotenv();
        let bind = env_or("NORTHGATE_BIND", "127.0.0.1:8787")
            .parse()
            .context("NORTHGATE_BIND must be a socket address")?;
        let database_path = PathBuf::from(env_or("NORTHGATE_DATABASE_PATH", "data/northgate.db"));
        let mut decoded = base64::engine::general_purpose::STANDARD
            .decode(required("NORTHGATE_MASTER_KEY")?)
            .context("NORTHGATE_MASTER_KEY must be standard base64")?;
        if decoded.len() != 32 {
            decoded.zeroize();
            bail!("NORTHGATE_MASTER_KEY must decode to exactly 32 bytes");
        }
        let mut master_key = [0_u8; 32];
        master_key.copy_from_slice(&decoded);
        decoded.zeroize();

        let max_upstream_concurrency = env_or("NORTHGATE_MAX_UPSTREAM_CONCURRENCY", "32")
            .parse::<usize>()
            .context("NORTHGATE_MAX_UPSTREAM_CONCURRENCY must be an integer")?;
        if !(1..=128).contains(&max_upstream_concurrency) {
            bail!("NORTHGATE_MAX_UPSTREAM_CONCURRENCY must be between 1 and 128");
        }

        let max_inbound_concurrency = env_or("NORTHGATE_MAX_INBOUND_CONCURRENCY", "64")
            .parse::<usize>()
            .context("NORTHGATE_MAX_INBOUND_CONCURRENCY must be an integer")?;
        if !(1..=256).contains(&max_inbound_concurrency) {
            bail!("NORTHGATE_MAX_INBOUND_CONCURRENCY must be between 1 and 256");
        }

        let admin = required("NORTHGATE_ADMIN_TOKEN")?;
        let gateway = required("NORTHGATE_GATEWAY_KEY")?;
        if admin.as_bytes() == gateway.as_bytes() {
            bail!("NORTHGATE_ADMIN_TOKEN and NORTHGATE_GATEWAY_KEY must differ");
        }
        let stream_idle_seconds = env_or("NORTHGATE_STREAM_IDLE_TIMEOUT_SECS", "0")
            .parse::<u64>()
            .context("NORTHGATE_STREAM_IDLE_TIMEOUT_SECS must be an integer")?;
        let first_byte_seconds = env_or("NORTHGATE_FIRST_BYTE_TIMEOUT_SECS", "300")
            .parse::<u64>()
            .context("NORTHGATE_FIRST_BYTE_TIMEOUT_SECS must be an integer")?;
        if !(1..=900).contains(&first_byte_seconds) {
            bail!("NORTHGATE_FIRST_BYTE_TIMEOUT_SECS must be between 1 and 900");
        }
        let allow_private_upstreams = parse_bool_env("NORTHGATE_ALLOW_PRIVATE_UPSTREAMS", false)?;

        Ok(Self {
            bind,
            database_path,
            master_key,
            admin_token: Secret::new(admin, "NORTHGATE_ADMIN_TOKEN")?,
            gateway_key: Secret::new(gateway, "NORTHGATE_GATEWAY_KEY")?,
            max_upstream_concurrency,
            max_inbound_concurrency,
            connect_timeout: Duration::from_secs(10),
            first_byte_timeout: Duration::from_secs(first_byte_seconds),
            body_idle_timeout: Duration::from_secs(60),
            stream_idle_timeout: (stream_idle_seconds > 0)
                .then(|| Duration::from_secs(stream_idle_seconds)),
            request_body_timeout: Duration::from_secs(30),
            allow_private_upstreams,
        })
    }
}

fn required(name: &str) -> anyhow::Result<String> {
    std::env::var(name).with_context(|| format!("missing required environment variable {name}"))
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn parse_bool_env(name: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(name) {
        Ok(value) if matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes") => {
            Ok(true)
        }
        Ok(value) if matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "no") => {
            Ok(false)
        }
        Ok(_) => bail!("{name} must be true or false"),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

#[cfg(test)]
mod tests {
    use super::Secret;

    #[test]
    fn secret_comparison_is_exact() {
        let secret = Secret::new("0123456789abcdef".to_owned(), "test").unwrap();
        assert!(secret.matches("0123456789abcdef"));
        assert!(!secret.matches("0123456789abcdeg"));
        assert!(!secret.matches("short"));
        assert_eq!(format!("{secret:?}"), "[REDACTED]");
    }
}
