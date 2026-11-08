//! Persistent provider and model catalog.
//!
//! The store deliberately owns one SQLite connection on one bounded worker
//! thread.  SQLite work never runs on an async executor thread, and the
//! bounded command queue gives callers backpressure on small deployments.

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use rand_core::OsRng;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use zeroize::Zeroize;

use crate::protocol::UpstreamProtocol;

pub const MAX_MODELS_PER_PROVIDER: usize = 256;
pub const MAX_FALLBACK_API_KEYS: usize = 2;
pub const MAX_PROVIDER_NAME_BYTES: usize = 128;
pub const MAX_PROVIDER_SLUG_BYTES: usize = 64;
pub const MAX_BASE_URL_BYTES: usize = 2_048;
pub const MAX_API_KEY_BYTES: usize = 16 * 1024;
pub const MAX_UPSTREAM_ID_BYTES: usize = 512;
pub const MAX_DISPLAY_NAME_BYTES: usize = 256;
pub const MAX_ALIAS_BYTES: usize = MAX_PROVIDER_SLUG_BYTES + 1 + MAX_UPSTREAM_ID_BYTES;
pub const DATABASE_QUEUE_CAPACITY: usize = 32;
const MAX_METADATA_NUMBER: u64 = 1 << 30;
const NONCE_BYTES: usize = 24;
const USAGE_BUCKET_SECONDS: i64 = 60 * 60;

/// Provider-reported token usage. Cached input is kept separate from ordinary
/// input so dashboards can display it without double counting.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_tokens: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct UsageTotals {
    pub requests: u64,
    pub failed_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_tokens: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UsagePoint {
    pub bucket_start: i64,
    #[serde(flatten)]
    pub totals: UsageTotals,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ModelUsage {
    pub model: String,
    pub provider: String,
    #[serde(flatten)]
    pub totals: UsageTotals,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct UsageStats {
    pub summary: UsageTotals,
    pub series: Vec<UsagePoint>,
    pub models: Vec<ModelUsage>,
}

/// A secret string which never prints its contents and clears its allocation
/// when dropped.  Callers should use `expose_secret` only while building an
/// upstream request.
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Result<Self, StoreError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_API_KEY_BYTES {
            return Err(StoreError::InvalidInput);
        }
        if value.chars().any(char::is_control) {
            return Err(StoreError::InvalidInput);
        }
        Ok(Self(value))
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    pub fn into_exposed(mut self) -> String {
        std::mem::take(&mut self.0)
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl Drop for SecretString {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl TryFrom<String> for SecretString {
    type Error = StoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Metadata discovered from an upstream model endpoint and stored with the
/// selected model.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ModelMetadata {
    pub upstream_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
    #[serde(default)]
    pub capabilities: ModelCapabilities,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ModelCapabilities {
    #[serde(default)]
    pub tools: bool,
    #[serde(default)]
    pub vision: bool,
    #[serde(default)]
    pub reasoning: bool,
    #[serde(default = "default_true")]
    pub streaming: bool,
}

fn default_true() -> bool {
    true
}

/// Input for an atomic provider save.  The API key is kept out of ordinary
/// debug output and is encrypted before SQLite receives it.
pub struct NewProvider {
    pub name: String,
    pub base_url: String,
    pub api_key: SecretString,
    pub fallback_api_keys: Vec<SecretString>,
    pub protocol: UpstreamProtocol,
    pub models: Vec<ModelMetadata>,
}

impl NewProvider {
    pub fn new(
        name: impl Into<String>,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        protocol: UpstreamProtocol,
        models: Vec<ModelMetadata>,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            name: name.into(),
            base_url: base_url.into(),
            api_key: SecretString::new(api_key)?,
            fallback_api_keys: Vec::new(),
            protocol,
            models,
        })
    }

    pub fn with_fallback_api_keys(mut self, api_keys: Vec<String>) -> Result<Self, StoreError> {
        if api_keys.len() > MAX_FALLBACK_API_KEYS {
            return Err(StoreError::InvalidInput);
        }
        self.fallback_api_keys = api_keys
            .into_iter()
            .map(SecretString::new)
            .collect::<Result<Vec<_>, _>>()?;
        validate_api_keys(&self.api_key, &self.fallback_api_keys)?;
        Ok(self)
    }
}

impl fmt::Debug for NewProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewProvider")
            .field("name", &self.name)
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .field("fallback_api_keys", &self.fallback_api_keys.len())
            .field("protocol", &self.protocol)
            .field("models", &self.models)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProviderSummary {
    pub slug: String,
    pub name: String,
    pub base_url: String,
    pub protocol: UpstreamProtocol,
    pub enabled: bool,
    pub model_count: usize,
    pub api_key_hint: String,
    pub fallback_api_key_hints: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ModelSummary {
    pub alias: String,
    pub provider_slug: String,
    pub upstream_id: String,
    pub display_name: Option<String>,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub capabilities: ModelCapabilities,
}

/// A resolved model contains the credential needed by the request forwarder.
/// It is intentionally not serializable and its debug representation remains
/// safe because `SecretString` is redacted.
#[derive(Debug)]
pub struct ResolvedModel {
    pub model: ModelSummary,
    pub provider_name: String,
    pub base_url: String,
    pub protocol: UpstreamProtocol,
    pub api_key: SecretString,
    pub fallback_api_keys: Vec<SecretString>,
}

/// Connection details for an existing provider. This is intentionally not
/// serializable: callers may use the primary credential only for an upstream
/// operation performed on the server.
#[derive(Debug)]
pub struct ResolvedProvider {
    pub base_url: String,
    pub protocol: UpstreamProtocol,
    pub api_key: SecretString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    InvalidInput,
    NotFound,
    Conflict,
    Database,
    Encryption,
    WorkerUnavailable,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidInput => "invalid store input",
            Self::NotFound => "store item not found",
            Self::Conflict => "store item already exists",
            Self::Database => "store database operation failed",
            Self::Encryption => "store encryption operation failed",
            Self::WorkerUnavailable => "store worker unavailable",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for StoreError {}

/// Turn a human provider name into the stable public slug used in aliases.
pub fn provider_slug(name: &str) -> Result<String, StoreError> {
    validate_bounded_text(name, MAX_PROVIDER_NAME_BYTES, false)?;

    let mut slug = String::with_capacity(name.len().min(MAX_PROVIDER_SLUG_BYTES));
    let mut separator_pending = false;
    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            if separator_pending && !slug.is_empty() {
                slug.push('-');
            }
            slug.push(character.to_ascii_lowercase());
            separator_pending = false;
        } else {
            separator_pending = true;
        }
    }

    if slug.len() > MAX_PROVIDER_SLUG_BYTES {
        slug.truncate(MAX_PROVIDER_SLUG_BYTES);
        while slug.ends_with('-') {
            slug.pop();
        }
    }
    validate_slug(&slug)?;
    Ok(slug)
}

fn validate_slug(slug: &str) -> Result<(), StoreError> {
    if slug.is_empty()
        || slug.len() > MAX_PROVIDER_SLUG_BYTES
        || slug.starts_with('-')
        || slug.ends_with('-')
        || !slug
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(StoreError::InvalidInput);
    }
    Ok(())
}

fn validate_bounded_text(
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
) -> Result<(), StoreError> {
    if (!allow_empty && value.is_empty())
        || value.len() > max_bytes
        || value.chars().any(char::is_control)
    {
        return Err(StoreError::InvalidInput);
    }
    Ok(())
}

fn validate_model(model: &ModelMetadata) -> Result<(), StoreError> {
    validate_bounded_text(&model.upstream_id, MAX_UPSTREAM_ID_BYTES, false)?;
    validate_bounded_text(
        model.display_name.as_deref().unwrap_or_default(),
        MAX_DISPLAY_NAME_BYTES,
        true,
    )?;
    if model
        .context_window
        .is_some_and(|value| value == 0 || value > MAX_METADATA_NUMBER)
        || model
            .max_output_tokens
            .is_some_and(|value| value == 0 || value > MAX_METADATA_NUMBER)
    {
        return Err(StoreError::InvalidInput);
    }
    Ok(())
}

fn validate_api_keys(
    primary: &SecretString,
    fallback_api_keys: &[SecretString],
) -> Result<(), StoreError> {
    if fallback_api_keys.len() > MAX_FALLBACK_API_KEYS {
        return Err(StoreError::InvalidInput);
    }
    let mut unique = std::collections::HashSet::with_capacity(fallback_api_keys.len() + 1);
    unique.insert(primary.expose_secret());
    for key in fallback_api_keys {
        if !unique.insert(key.expose_secret()) {
            return Err(StoreError::Conflict);
        }
    }
    Ok(())
}

fn validate_provider(input: &NewProvider) -> Result<String, StoreError> {
    let slug = provider_slug(&input.name)?;
    validate_bounded_text(&input.base_url, MAX_BASE_URL_BYTES, false)?;
    if input.models.is_empty() || input.models.len() > MAX_MODELS_PER_PROVIDER {
        return Err(StoreError::InvalidInput);
    }
    validate_api_keys(&input.api_key, &input.fallback_api_keys)?;

    let mut ids = std::collections::HashSet::with_capacity(input.models.len());
    for model in &input.models {
        validate_model(model)?;
        if !ids.insert(&model.upstream_id) {
            return Err(StoreError::Conflict);
        }
        let alias_len = slug.len() + 1 + model.upstream_id.len();
        if alias_len > MAX_ALIAS_BYTES {
            return Err(StoreError::InvalidInput);
        }
    }
    Ok(slug)
}

fn protocol_to_db(protocol: &UpstreamProtocol) -> Result<String, StoreError> {
    serde_json::to_string(protocol).map_err(|_| StoreError::InvalidInput)
}

fn protocol_from_db(value: &str) -> Result<UpstreamProtocol, StoreError> {
    serde_json::from_str(value).map_err(|_| StoreError::Database)
}

fn capabilities_to_db(capabilities: &ModelCapabilities) -> Result<String, StoreError> {
    serde_json::to_string(capabilities).map_err(|_| StoreError::InvalidInput)
}

struct MasterKey([u8; 32]);

impl Drop for MasterKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

fn encrypt_api_key(master_key: &MasterKey, api_key: &SecretString) -> Result<Vec<u8>, StoreError> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&master_key.0));
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let mut ciphertext = cipher
        .encrypt(&nonce, api_key.expose_secret().as_bytes())
        .map_err(|_| StoreError::Encryption)?;
    let mut packed = Vec::with_capacity(NONCE_BYTES + ciphertext.len());
    packed.extend_from_slice(nonce.as_slice());
    packed.append(&mut ciphertext);
    Ok(packed)
}

fn decrypt_api_key(master_key: &MasterKey, encrypted: &[u8]) -> Result<SecretString, StoreError> {
    if encrypted.len() <= NONCE_BYTES {
        return Err(StoreError::Encryption);
    }
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&master_key.0));
    let nonce = XNonce::from_slice(&encrypted[..NONCE_BYTES]);
    let plaintext = cipher
        .decrypt(nonce, &encrypted[NONCE_BYTES..])
        .map_err(|_| StoreError::Encryption)?;
    String::from_utf8(plaintext)
        .map_err(|_| StoreError::Encryption)
        .and_then(SecretString::new)
}

fn api_key_hint(api_key: &str) -> String {
    let characters = api_key.chars().collect::<Vec<_>>();
    match characters.len() {
        0..=8 => "••••".to_owned(),
        9..=15 => format!(
            "{}…{}",
            characters[..2].iter().collect::<String>(),
            characters[characters.len() - 2..]
                .iter()
                .collect::<String>()
        ),
        16..=23 => format!(
            "{}…{}",
            characters[..4].iter().collect::<String>(),
            characters[characters.len() - 3..]
                .iter()
                .collect::<String>()
        ),
        _ => format!(
            "{}…{}",
            characters[..6].iter().collect::<String>(),
            characters[characters.len() - 4..]
                .iter()
                .collect::<String>()
        ),
    }
}

fn configure_database(connection: &Connection) -> Result<(), StoreError> {
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|_| StoreError::Database)?;
    connection
        .execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS providers (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 slug TEXT NOT NULL UNIQUE CHECK(length(slug) <= 64),
                 name TEXT NOT NULL CHECK(length(name) <= 128),
                 base_url TEXT NOT NULL CHECK(length(base_url) <= 2048),
                 protocol TEXT NOT NULL,
                 api_key_encrypted BLOB NOT NULL,
                 enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
                 created_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS provider_fallback_keys (
                 provider_id INTEGER NOT NULL,
                 position INTEGER NOT NULL CHECK(position BETWEEN 1 AND 2),
                 api_key_encrypted BLOB NOT NULL,
                 PRIMARY KEY(provider_id, position),
                 FOREIGN KEY(provider_id) REFERENCES providers(id) ON DELETE CASCADE
             );
             CREATE TABLE IF NOT EXISTS models (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 provider_id INTEGER NOT NULL,
                 upstream_id TEXT NOT NULL CHECK(length(upstream_id) <= 512),
                 alias TEXT NOT NULL UNIQUE CHECK(length(alias) <= 577),
                 display_name TEXT CHECK(display_name IS NULL OR length(display_name) <= 256),
                 context_window INTEGER,
                 max_output_tokens INTEGER,
                 capabilities_json TEXT NOT NULL,
                 FOREIGN KEY(provider_id) REFERENCES providers(id) ON DELETE CASCADE,
                 UNIQUE(provider_id, upstream_id),
                 CHECK(
                     context_window IS NULL OR
                     (context_window > 0 AND context_window <= 1073741824)
                 ),
                 CHECK(
                     max_output_tokens IS NULL OR
                     (max_output_tokens > 0 AND max_output_tokens <= 1073741824)
                 )
             );
             CREATE INDEX IF NOT EXISTS idx_models_provider_id ON models(provider_id);
             CREATE TABLE IF NOT EXISTS usage_hourly (
                 bucket_start INTEGER NOT NULL,
                 provider_slug TEXT NOT NULL CHECK(length(provider_slug) <= 64),
                 model_alias TEXT NOT NULL CHECK(length(model_alias) <= 577),
                 requests INTEGER NOT NULL DEFAULT 0 CHECK(requests >= 0),
                 failed_requests INTEGER NOT NULL DEFAULT 0 CHECK(failed_requests >= 0),
                 input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(input_tokens >= 0),
                 output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(output_tokens >= 0),
                 cache_tokens INTEGER NOT NULL DEFAULT 0 CHECK(cache_tokens >= 0),
                 PRIMARY KEY(bucket_start, provider_slug, model_alias)
             );
             CREATE INDEX IF NOT EXISTS idx_usage_hourly_model
                 ON usage_hourly(model_alias, bucket_start);",
        )
        .map_err(|_| StoreError::Database)?;

    if !column_exists(connection, "usage_hourly", "failed_requests")? {
        connection
            .execute(
                "ALTER TABLE usage_hourly ADD COLUMN failed_requests INTEGER NOT NULL DEFAULT 0 CHECK(failed_requests >= 0)",
                [],
            )
            .map_err(|_| StoreError::Database)?;
    }
    if !column_exists(connection, "providers", "enabled")? {
        connection
            .execute(
                "ALTER TABLE providers ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1))",
                [],
            )
            .map_err(|_| StoreError::Database)?;
    }
    Ok(())
}

fn column_exists(connection: &Connection, table: &str, column: &str) -> Result<bool, StoreError> {
    let mut statement = connection
        .prepare("SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2")
        .map_err(|_| StoreError::Database)?;
    statement
        .exists(params![table, column])
        .map_err(|_| StoreError::Database)
}

fn now_unix_seconds() -> Result<i64, StoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::Database)
        .and_then(|duration| i64::try_from(duration.as_secs()).map_err(|_| StoreError::Database))
}

fn integer_metadata(value: Option<u64>) -> Result<Option<i64>, StoreError> {
    value
        .map(|number| i64::try_from(number).map_err(|_| StoreError::InvalidInput))
        .transpose()
}

fn save_provider_sync(
    connection: &mut Connection,
    master_key: &MasterKey,
    input: NewProvider,
) -> Result<ProviderSummary, StoreError> {
    let slug = validate_provider(&input)?;
    let key_hint = api_key_hint(input.api_key.expose_secret());
    let fallback_api_key_hints = input
        .fallback_api_keys
        .iter()
        .map(|key| api_key_hint(key.expose_secret()))
        .collect::<Vec<_>>();
    let protocol = protocol_to_db(&input.protocol)?;
    let encrypted_key = encrypt_api_key(master_key, &input.api_key)?;
    let created_at = now_unix_seconds()?;
    let transaction = connection.transaction().map_err(|_| StoreError::Database)?;

    let existing = transaction
        .query_row(
            "SELECT id FROM providers WHERE slug = ?1",
            params![&slug],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|_| StoreError::Database)?;
    if existing.is_some() {
        return Err(StoreError::Conflict);
    }

    transaction
        .execute(
            "INSERT INTO providers
                (slug, name, base_url, protocol, api_key_encrypted, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                &slug,
                &input.name,
                &input.base_url,
                protocol,
                encrypted_key,
                created_at
            ],
        )
        .map_err(|_| StoreError::Database)?;
    let provider_id = transaction.last_insert_rowid();

    for (index, api_key) in input.fallback_api_keys.iter().enumerate() {
        let encrypted = encrypt_api_key(master_key, api_key)?;
        transaction
            .execute(
                "INSERT INTO provider_fallback_keys
                    (provider_id, position, api_key_encrypted)
                 VALUES (?1, ?2, ?3)",
                params![provider_id, (index + 1) as i64, encrypted],
            )
            .map_err(|_| StoreError::Database)?;
    }

    for model in &input.models {
        let alias = format!("{slug}/{}", model.upstream_id);
        let capabilities = capabilities_to_db(&model.capabilities)?;
        transaction
            .execute(
                "INSERT INTO models
                    (provider_id, upstream_id, alias, display_name, context_window,
                     max_output_tokens, capabilities_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    provider_id,
                    &model.upstream_id,
                    alias,
                    model.display_name.as_deref(),
                    integer_metadata(model.context_window)?,
                    integer_metadata(model.max_output_tokens)?,
                    capabilities
                ],
            )
            .map_err(|_| StoreError::Database)?;
    }

    transaction.commit().map_err(|_| StoreError::Database)?;
    Ok(ProviderSummary {
        slug,
        name: input.name,
        base_url: input.base_url,
        protocol: input.protocol,
        enabled: true,
        model_count: input.models.len(),
        api_key_hint: key_hint,
        fallback_api_key_hints,
    })
}

fn row_to_model_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<ModelSummary> {
    let positive = |index: usize| -> rusqlite::Result<Option<u64>> {
        row.get::<_, Option<i64>>(index)?
            .map(|number| u64::try_from(number).map_err(|_| rusqlite::Error::InvalidQuery))
            .transpose()
    };
    Ok(ModelSummary {
        alias: row.get(0)?,
        provider_slug: row.get(1)?,
        upstream_id: row.get(2)?,
        display_name: row.get(3)?,
        context_window: positive(4)?,
        max_output_tokens: positive(5)?,
        capabilities: serde_json::from_str(&row.get::<_, String>(6)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
    })
}

fn list_models_sync(
    connection: &Connection,
    enabled_only: bool,
) -> Result<Vec<ModelSummary>, StoreError> {
    let query = if enabled_only {
        "SELECT m.alias, p.slug, m.upstream_id, m.display_name,
                m.context_window, m.max_output_tokens, m.capabilities_json
         FROM models m JOIN providers p ON p.id = m.provider_id
         WHERE p.enabled = 1
         ORDER BY p.slug, m.id"
    } else {
        "SELECT m.alias, p.slug, m.upstream_id, m.display_name,
                m.context_window, m.max_output_tokens, m.capabilities_json
         FROM models m JOIN providers p ON p.id = m.provider_id
         ORDER BY p.slug, m.id"
    };
    let mut statement = connection
        .prepare(query)
        .map_err(|_| StoreError::Database)?;
    let rows = statement
        .query_map([], row_to_model_summary)
        .map_err(|_| StoreError::Database)?;
    rows.map(|row| row.map_err(|_| StoreError::Database))
        .collect()
}

fn fallback_api_keys_sync(
    connection: &Connection,
    master_key: &MasterKey,
    provider_id: i64,
) -> Result<Vec<SecretString>, StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT api_key_encrypted FROM provider_fallback_keys
             WHERE provider_id = ?1 ORDER BY position",
        )
        .map_err(|_| StoreError::Database)?;
    let rows = statement
        .query_map(params![provider_id], |row| row.get::<_, Vec<u8>>(0))
        .map_err(|_| StoreError::Database)?;
    let mut keys = Vec::new();
    for row in rows {
        let mut encrypted = row.map_err(|_| StoreError::Database)?;
        let key = decrypt_api_key(master_key, &encrypted);
        encrypted.zeroize();
        keys.push(key?);
    }
    Ok(keys)
}

fn list_providers_sync(
    connection: &Connection,
    master_key: &MasterKey,
) -> Result<Vec<ProviderSummary>, StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT p.slug, p.name, p.base_url, p.protocol, p.enabled, COUNT(m.id),
                    p.api_key_encrypted, p.id
             FROM providers p LEFT JOIN models m ON m.provider_id = p.id
             GROUP BY p.id ORDER BY p.slug",
        )
        .map_err(|_| StoreError::Database)?;
    let rows = statement
        .query_map([], |row| {
            let protocol = protocol_from_db(
                &row.get::<_, String>(3)
                    .map_err(|_| rusqlite::Error::InvalidQuery)?,
            )
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
            let model_count = row
                .get::<_, i64>(5)
                .map_err(|_| rusqlite::Error::InvalidQuery)
                .and_then(|value| {
                    usize::try_from(value).map_err(|_| rusqlite::Error::InvalidQuery)
                })?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                protocol,
                row.get::<_, bool>(4)?,
                model_count,
                row.get::<_, Vec<u8>>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })
        .map_err(|_| StoreError::Database)?;
    let mut providers = Vec::new();
    for row in rows {
        let (slug, name, base_url, protocol, enabled, model_count, mut encrypted_key, provider_id) =
            row.map_err(|_| StoreError::Database)?;
        let key = decrypt_api_key(master_key, &encrypted_key);
        encrypted_key.zeroize();
        let key = key?;
        let fallback_api_key_hints = fallback_api_keys_sync(connection, master_key, provider_id)?
            .iter()
            .map(|key| api_key_hint(key.expose_secret()))
            .collect();
        providers.push(ProviderSummary {
            slug,
            name,
            base_url,
            protocol,
            enabled,
            model_count,
            api_key_hint: api_key_hint(key.expose_secret()),
            fallback_api_key_hints,
        });
    }
    Ok(providers)
}

fn find_model_sync(
    connection: &Connection,
    master_key: &MasterKey,
    alias: &str,
) -> Result<ResolvedModel, StoreError> {
    validate_bounded_text(alias, MAX_ALIAS_BYTES, false)?;
    let (provider_slug, upstream_id) = alias.split_once('/').ok_or(StoreError::NotFound)?;
    validate_slug(provider_slug)?;
    validate_bounded_text(upstream_id, MAX_UPSTREAM_ID_BYTES, false)?;

    let result = connection
        .query_row(
            "SELECT m.alias, p.slug, m.upstream_id, m.display_name,
                    m.context_window, m.max_output_tokens, m.capabilities_json,
                    p.name, p.base_url, p.protocol, p.api_key_encrypted, p.id
             FROM models m JOIN providers p ON p.id = m.provider_id
             WHERE p.slug = ?1 AND m.upstream_id = ?2 AND p.enabled = 1",
            params![provider_slug, upstream_id],
            |row| {
                let encrypted_key: Vec<u8> = row.get(10)?;
                Ok((
                    row_to_model_summary(row)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    encrypted_key,
                    row.get::<_, i64>(11)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StoreError::Database)?
        .ok_or(StoreError::NotFound)?;

    let (model, provider_name, base_url, protocol_value, mut encrypted_key, provider_id) = result;
    let api_key = decrypt_api_key(master_key, &encrypted_key);
    encrypted_key.zeroize();
    let fallback_api_keys = fallback_api_keys_sync(connection, master_key, provider_id)?;
    Ok(ResolvedModel {
        model,
        provider_name,
        base_url,
        protocol: protocol_from_db(&protocol_value)?,
        api_key: api_key?,
        fallback_api_keys,
    })
}

fn find_provider_sync(
    connection: &Connection,
    master_key: &MasterKey,
    slug: &str,
) -> Result<ResolvedProvider, StoreError> {
    validate_slug(slug)?;
    let (base_url, protocol_value, mut encrypted_key) = connection
        .query_row(
            "SELECT base_url, protocol, api_key_encrypted FROM providers WHERE slug = ?1",
            params![slug],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|_| StoreError::Database)?
        .ok_or(StoreError::NotFound)?;
    let api_key = decrypt_api_key(master_key, &encrypted_key);
    encrypted_key.zeroize();
    Ok(ResolvedProvider {
        base_url,
        protocol: protocol_from_db(&protocol_value)?,
        api_key: api_key?,
    })
}

fn add_models_sync(
    connection: &mut Connection,
    slug: &str,
    models: Vec<ModelMetadata>,
) -> Result<(), StoreError> {
    validate_slug(slug)?;
    if models.is_empty() || models.len() > MAX_MODELS_PER_PROVIDER {
        return Err(StoreError::InvalidInput);
    }
    let mut ids = std::collections::HashSet::with_capacity(models.len());
    for model in &models {
        validate_model(model)?;
        if slug.len() + 1 + model.upstream_id.len() > MAX_ALIAS_BYTES {
            return Err(StoreError::InvalidInput);
        }
        if !ids.insert(&model.upstream_id) {
            return Err(StoreError::Conflict);
        }
    }

    let transaction = connection.transaction().map_err(|_| StoreError::Database)?;
    let provider_id = transaction
        .query_row(
            "SELECT id FROM providers WHERE slug = ?1",
            params![slug],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|_| StoreError::Database)?
        .ok_or(StoreError::NotFound)?;
    let existing_count = transaction
        .query_row(
            "SELECT COUNT(*) FROM models WHERE provider_id = ?1",
            params![provider_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|_| StoreError::Database)?;
    if usize::try_from(existing_count).map_err(|_| StoreError::Database)? + models.len()
        > MAX_MODELS_PER_PROVIDER
    {
        return Err(StoreError::InvalidInput);
    }

    for model in &models {
        let exists = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM models WHERE provider_id = ?1 AND upstream_id = ?2)",
                params![provider_id, &model.upstream_id],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|_| StoreError::Database)?;
        if exists {
            return Err(StoreError::Conflict);
        }
        transaction
            .execute(
                "INSERT INTO models
                    (provider_id, upstream_id, alias, display_name, context_window,
                     max_output_tokens, capabilities_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    provider_id,
                    &model.upstream_id,
                    format!("{slug}/{}", model.upstream_id),
                    model.display_name.as_deref(),
                    integer_metadata(model.context_window)?,
                    integer_metadata(model.max_output_tokens)?,
                    capabilities_to_db(&model.capabilities)?,
                ],
            )
            .map_err(|_| StoreError::Database)?;
    }
    transaction.commit().map_err(|_| StoreError::Database)
}

fn delete_model_sync(
    connection: &Connection,
    slug: &str,
    upstream_id: &str,
) -> Result<bool, StoreError> {
    validate_slug(slug)?;
    validate_bounded_text(upstream_id, MAX_UPSTREAM_ID_BYTES, false)?;
    let deleted = connection
        .execute(
            "DELETE FROM models WHERE id IN (
                SELECT m.id FROM models m JOIN providers p ON p.id = m.provider_id
                WHERE p.slug = ?1 AND m.upstream_id = ?2
             )",
            params![slug, upstream_id],
        )
        .map_err(|_| StoreError::Database)?;
    Ok(deleted != 0)
}

fn delete_provider_sync(connection: &Connection, slug: &str) -> Result<bool, StoreError> {
    validate_slug(slug)?;
    let deleted = connection
        .execute("DELETE FROM providers WHERE slug = ?1", params![slug])
        .map_err(|_| StoreError::Database)?;
    Ok(deleted != 0)
}

fn set_provider_enabled_sync(
    connection: &Connection,
    slug: &str,
    enabled: bool,
) -> Result<bool, StoreError> {
    validate_slug(slug)?;
    let updated = connection
        .execute(
            "UPDATE providers SET enabled = ?2 WHERE slug = ?1",
            params![slug, enabled],
        )
        .map_err(|_| StoreError::Database)?;
    Ok(updated != 0)
}

fn set_fallback_api_key_sync(
    connection: &mut Connection,
    master_key: &MasterKey,
    slug: &str,
    position: usize,
    api_key: Option<SecretString>,
) -> Result<(), StoreError> {
    validate_slug(slug)?;
    if !(1..=MAX_FALLBACK_API_KEYS).contains(&position) {
        return Err(StoreError::InvalidInput);
    }
    let transaction = connection.transaction().map_err(|_| StoreError::Database)?;
    let (provider_id, mut encrypted_primary) = transaction
        .query_row(
            "SELECT id, api_key_encrypted FROM providers WHERE slug = ?1",
            params![slug],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(|_| StoreError::Database)?
        .ok_or(StoreError::NotFound)?;

    if let Some(api_key) = api_key {
        if position == 2 {
            let first_exists = transaction
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM provider_fallback_keys
                        WHERE provider_id = ?1 AND position = 1
                     )",
                    params![provider_id],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(|_| StoreError::Database)?;
            if !first_exists {
                encrypted_primary.zeroize();
                return Err(StoreError::InvalidInput);
            }
        }
        let primary = decrypt_api_key(master_key, &encrypted_primary);
        encrypted_primary.zeroize();
        let primary = primary?;
        let existing = fallback_api_keys_sync(&transaction, master_key, provider_id)?;
        if api_key.expose_secret() == primary.expose_secret()
            || existing.iter().enumerate().any(|(index, key)| {
                index + 1 != position && key.expose_secret() == api_key.expose_secret()
            })
        {
            return Err(StoreError::Conflict);
        }
        let encrypted = encrypt_api_key(master_key, &api_key)?;
        transaction
            .execute(
                "INSERT INTO provider_fallback_keys
                    (provider_id, position, api_key_encrypted)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(provider_id, position) DO UPDATE SET
                    api_key_encrypted = excluded.api_key_encrypted",
                params![provider_id, position as i64, encrypted],
            )
            .map_err(|_| StoreError::Database)?;
    } else {
        encrypted_primary.zeroize();
        transaction
            .execute(
                "DELETE FROM provider_fallback_keys
                 WHERE provider_id = ?1 AND position = ?2",
                params![provider_id, position as i64],
            )
            .map_err(|_| StoreError::Database)?;
        if position == 1 {
            transaction
                .execute(
                    "UPDATE provider_fallback_keys SET position = 1
                     WHERE provider_id = ?1 AND position = 2",
                    params![provider_id],
                )
                .map_err(|_| StoreError::Database)?;
        }
    }
    transaction.commit().map_err(|_| StoreError::Database)
}

fn delete_model_usage_sync(connection: &Connection, model_alias: &str) -> Result<bool, StoreError> {
    validate_bounded_text(model_alias, MAX_ALIAS_BYTES, false)?;
    let active = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM models WHERE alias = ?1)",
            params![model_alias],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|_| StoreError::Database)?;
    if active {
        return Err(StoreError::Conflict);
    }
    let deleted = connection
        .execute(
            "DELETE FROM usage_hourly WHERE model_alias = ?1",
            params![model_alias],
        )
        .map_err(|_| StoreError::Database)?;
    Ok(deleted != 0)
}

fn usage_value(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn record_usage_sync(
    connection: &Connection,
    provider_slug: &str,
    model_alias: &str,
    requests: u64,
    failed_requests: u64,
    usage: TokenUsage,
) -> Result<(), StoreError> {
    validate_slug(provider_slug)?;
    validate_bounded_text(model_alias, MAX_ALIAS_BYTES, false)?;
    let now = now_unix_seconds()?;
    let bucket_start = now - now.rem_euclid(USAGE_BUCKET_SECONDS);
    connection
        .execute(
            "INSERT INTO usage_hourly
                 (bucket_start, provider_slug, model_alias, requests, failed_requests,
                  input_tokens, output_tokens, cache_tokens)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(bucket_start, provider_slug, model_alias) DO UPDATE SET
                 requests = requests + excluded.requests,
                 failed_requests = failed_requests + excluded.failed_requests,
                 input_tokens = input_tokens + excluded.input_tokens,
                 output_tokens = output_tokens + excluded.output_tokens,
                 cache_tokens = cache_tokens + excluded.cache_tokens",
            params![
                bucket_start,
                provider_slug,
                model_alias,
                usage_value(requests),
                usage_value(failed_requests),
                usage_value(usage.input_tokens),
                usage_value(usage.output_tokens),
                usage_value(usage.cache_tokens),
            ],
        )
        .map_err(|_| StoreError::Database)?;
    Ok(())
}

fn row_to_totals(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<UsageTotals> {
    let number = |index| -> rusqlite::Result<u64> {
        let value = row.get::<_, i64>(offset + index)?;
        u64::try_from(value).map_err(|_| rusqlite::Error::InvalidQuery)
    };
    Ok(UsageTotals {
        requests: number(0)?,
        failed_requests: number(1)?,
        input_tokens: number(2)?,
        output_tokens: number(3)?,
        cache_tokens: number(4)?,
    })
}

fn usage_stats_sync(
    connection: &Connection,
    since: i64,
    series_bucket_seconds: i64,
) -> Result<UsageStats, StoreError> {
    if since < 0 || series_bucket_seconds <= 0 {
        return Err(StoreError::InvalidInput);
    }
    let summary = connection
        .query_row(
            "SELECT COALESCE(SUM(requests), 0), COALESCE(SUM(failed_requests), 0),
                    COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0),
                    COALESCE(SUM(cache_tokens), 0)
             FROM usage_hourly WHERE bucket_start >= ?1",
            params![since],
            |row| row_to_totals(row, 0),
        )
        .map_err(|_| StoreError::Database)?;

    let mut series_statement = connection
        .prepare(
            "SELECT (bucket_start / ?2) * ?2 AS display_bucket,
                    SUM(requests), SUM(failed_requests), SUM(input_tokens),
                    SUM(output_tokens), SUM(cache_tokens)
             FROM usage_hourly WHERE bucket_start >= ?1
             GROUP BY display_bucket ORDER BY display_bucket",
        )
        .map_err(|_| StoreError::Database)?;
    let series = series_statement
        .query_map(params![since, series_bucket_seconds], |row| {
            Ok(UsagePoint {
                bucket_start: row.get(0)?,
                totals: row_to_totals(row, 1)?,
            })
        })
        .map_err(|_| StoreError::Database)?
        .map(|row| row.map_err(|_| StoreError::Database))
        .collect::<Result<Vec<_>, _>>()?;

    let mut models_statement = connection
        .prepare(
            "SELECT model_alias, provider_slug, SUM(requests), SUM(failed_requests),
                    SUM(input_tokens), SUM(output_tokens), SUM(cache_tokens)
             FROM usage_hourly WHERE bucket_start >= ?1
             GROUP BY model_alias, provider_slug
             ORDER BY SUM(requests) DESC,
                      SUM(input_tokens + output_tokens + cache_tokens) DESC,
                      model_alias",
        )
        .map_err(|_| StoreError::Database)?;
    let models = models_statement
        .query_map(params![since], |row| {
            Ok(ModelUsage {
                model: row.get(0)?,
                provider: row.get(1)?,
                totals: row_to_totals(row, 2)?,
            })
        })
        .map_err(|_| StoreError::Database)?
        .map(|row| row.map_err(|_| StoreError::Database))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(UsageStats {
        summary,
        series,
        models,
    })
}

enum Command {
    ListModels(oneshot::Sender<Result<Vec<ModelSummary>, StoreError>>),
    ListEnabledModels(oneshot::Sender<Result<Vec<ModelSummary>, StoreError>>),
    ListProviders(oneshot::Sender<Result<Vec<ProviderSummary>, StoreError>>),
    FindModel(String, oneshot::Sender<Result<ResolvedModel, StoreError>>),
    FindProvider(
        String,
        oneshot::Sender<Result<ResolvedProvider, StoreError>>,
    ),
    SaveProvider(
        NewProvider,
        oneshot::Sender<Result<ProviderSummary, StoreError>>,
    ),
    AddModels {
        slug: String,
        models: Vec<ModelMetadata>,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    DeleteModel {
        slug: String,
        upstream_id: String,
        reply: oneshot::Sender<Result<bool, StoreError>>,
    },
    DeleteProvider(String, oneshot::Sender<Result<bool, StoreError>>),
    SetProviderEnabled {
        slug: String,
        enabled: bool,
        reply: oneshot::Sender<Result<bool, StoreError>>,
    },
    SetFallbackApiKey {
        slug: String,
        position: usize,
        api_key: Option<SecretString>,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    DeleteModelUsage(String, oneshot::Sender<Result<bool, StoreError>>),
    RecordUsage {
        provider_slug: String,
        model_alias: String,
        requests: u64,
        failed_requests: u64,
        usage: TokenUsage,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    UsageStats {
        since: i64,
        series_bucket_seconds: i64,
        reply: oneshot::Sender<Result<UsageStats, StoreError>>,
    },
}

fn worker_loop(
    mut receiver: mpsc::Receiver<Command>,
    mut connection: Connection,
    master_key: MasterKey,
) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            Command::ListModels(reply) => {
                let _ = reply.send(list_models_sync(&connection, false));
            }
            Command::ListEnabledModels(reply) => {
                let _ = reply.send(list_models_sync(&connection, true));
            }
            Command::ListProviders(reply) => {
                let _ = reply.send(list_providers_sync(&connection, &master_key));
            }
            Command::FindModel(alias, reply) => {
                let _ = reply.send(find_model_sync(&connection, &master_key, &alias));
            }
            Command::FindProvider(slug, reply) => {
                let _ = reply.send(find_provider_sync(&connection, &master_key, &slug));
            }
            Command::SaveProvider(input, reply) => {
                let _ = reply.send(save_provider_sync(&mut connection, &master_key, input));
            }
            Command::AddModels {
                slug,
                models,
                reply,
            } => {
                let _ = reply.send(add_models_sync(&mut connection, &slug, models));
            }
            Command::DeleteModel {
                slug,
                upstream_id,
                reply,
            } => {
                let _ = reply.send(delete_model_sync(&connection, &slug, &upstream_id));
            }
            Command::DeleteProvider(slug, reply) => {
                let _ = reply.send(delete_provider_sync(&connection, &slug));
            }
            Command::SetProviderEnabled {
                slug,
                enabled,
                reply,
            } => {
                let _ = reply.send(set_provider_enabled_sync(&connection, &slug, enabled));
            }
            Command::SetFallbackApiKey {
                slug,
                position,
                api_key,
                reply,
            } => {
                let _ = reply.send(set_fallback_api_key_sync(
                    &mut connection,
                    &master_key,
                    &slug,
                    position,
                    api_key,
                ));
            }
            Command::DeleteModelUsage(model_alias, reply) => {
                let _ = reply.send(delete_model_usage_sync(&connection, &model_alias));
            }
            Command::RecordUsage {
                provider_slug,
                model_alias,
                requests,
                failed_requests,
                usage,
                reply,
            } => {
                let _ = reply.send(record_usage_sync(
                    &connection,
                    &provider_slug,
                    &model_alias,
                    requests,
                    failed_requests,
                    usage,
                ));
            }
            Command::UsageStats {
                since,
                series_bucket_seconds,
                reply,
            } => {
                let _ = reply.send(usage_stats_sync(&connection, since, series_bucket_seconds));
            }
        }
    }
}

/// Async-safe persistent catalog backed by one bounded SQLite worker.
#[derive(Clone)]
pub struct Store {
    sender: Arc<mpsc::Sender<Command>>,
}

impl Store {
    pub async fn open(path: impl AsRef<Path>, master_key: [u8; 32]) -> Result<Self, StoreError> {
        let connection = Connection::open(path).map_err(|_| StoreError::Database)?;
        configure_database(&connection)?;
        let (sender, receiver) = mpsc::channel(DATABASE_QUEUE_CAPACITY);
        let worker_key = MasterKey(master_key);
        std::thread::Builder::new()
            .name("northgate-sqlite".to_owned())
            .spawn(move || worker_loop(receiver, connection, worker_key))
            .map_err(|_| StoreError::WorkerUnavailable)?;
        Ok(Self {
            sender: Arc::new(sender),
        })
    }

    pub async fn list_models(&self) -> Result<Vec<ModelSummary>, StoreError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::ListModels(reply))
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn list_enabled_models(&self) -> Result<Vec<ModelSummary>, StoreError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::ListEnabledModels(reply))
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn list_providers(&self) -> Result<Vec<ProviderSummary>, StoreError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::ListProviders(reply))
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn find_model(&self, alias: &str) -> Result<ResolvedModel, StoreError> {
        validate_bounded_text(alias, MAX_ALIAS_BYTES, false)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::FindModel(alias.to_owned(), reply))
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn find_provider(&self, slug: &str) -> Result<ResolvedProvider, StoreError> {
        validate_slug(slug)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::FindProvider(slug.to_owned(), reply))
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn save_provider(&self, input: NewProvider) -> Result<ProviderSummary, StoreError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::SaveProvider(input, reply))
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn add_models(
        &self,
        slug: &str,
        models: Vec<ModelMetadata>,
    ) -> Result<(), StoreError> {
        validate_slug(slug)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::AddModels {
                slug: slug.to_owned(),
                models,
                reply,
            })
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn delete_model(&self, slug: &str, upstream_id: &str) -> Result<bool, StoreError> {
        validate_slug(slug)?;
        validate_bounded_text(upstream_id, MAX_UPSTREAM_ID_BYTES, false)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::DeleteModel {
                slug: slug.to_owned(),
                upstream_id: upstream_id.to_owned(),
                reply,
            })
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn delete_provider(&self, slug: &str) -> Result<bool, StoreError> {
        validate_slug(slug)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::DeleteProvider(slug.to_owned(), reply))
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn set_provider_enabled(
        &self,
        slug: &str,
        enabled: bool,
    ) -> Result<bool, StoreError> {
        validate_slug(slug)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::SetProviderEnabled {
                slug: slug.to_owned(),
                enabled,
                reply,
            })
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn set_fallback_api_key(
        &self,
        slug: &str,
        position: usize,
        api_key: Option<String>,
    ) -> Result<(), StoreError> {
        validate_slug(slug)?;
        if !(1..=MAX_FALLBACK_API_KEYS).contains(&position) {
            return Err(StoreError::InvalidInput);
        }
        let api_key = api_key.map(SecretString::new).transpose()?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::SetFallbackApiKey {
                slug: slug.to_owned(),
                position,
                api_key,
                reply,
            })
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn delete_model_usage(&self, model_alias: &str) -> Result<bool, StoreError> {
        validate_bounded_text(model_alias, MAX_ALIAS_BYTES, false)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::DeleteModelUsage(model_alias.to_owned(), reply))
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn record_usage(
        &self,
        provider_slug: &str,
        model_alias: &str,
        requests: u64,
        failed_requests: u64,
        usage: TokenUsage,
    ) -> Result<(), StoreError> {
        validate_slug(provider_slug)?;
        validate_bounded_text(model_alias, MAX_ALIAS_BYTES, false)?;
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::RecordUsage {
                provider_slug: provider_slug.to_owned(),
                model_alias: model_alias.to_owned(),
                requests,
                failed_requests,
                usage,
                reply,
            })
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }

    pub async fn usage_stats(
        &self,
        since: i64,
        series_bucket_seconds: i64,
    ) -> Result<UsageStats, StoreError> {
        if since < 0 || series_bucket_seconds <= 0 {
            return Err(StoreError::InvalidInput);
        }
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::UsageStats {
                since,
                series_bucket_seconds,
                reply,
            })
            .await
            .map_err(|_| StoreError::WorkerUnavailable)?;
        result.await.map_err(|_| StoreError::WorkerUnavailable)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_path() -> std::path::PathBuf {
        let number = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "northgate-store-{}-{number}.db",
            std::process::id()
        ))
    }

    fn model(id: &str) -> ModelMetadata {
        ModelMetadata {
            upstream_id: id.to_owned(),
            display_name: Some(id.to_owned()),
            context_window: Some(32_000),
            max_output_tokens: Some(4_000),
            capabilities: ModelCapabilities {
                tools: true,
                vision: false,
                reasoning: true,
                streaming: true,
            },
        }
    }

    fn provider(name: &str, api_key: &str, models: Vec<ModelMetadata>) -> NewProvider {
        NewProvider::new(
            name,
            "https://provider.example/v1",
            api_key,
            UpstreamProtocol::OpenAi,
            models,
        )
        .expect("valid test provider")
    }

    fn run<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn roundtrip_and_alias_resolution_support_slashes() {
        let path = test_path();
        run(async {
            let store = Store::open(&path, [7u8; 32]).await.expect("store");
            store
                .save_provider(provider(
                    "Open Router",
                    "top-secret",
                    vec![model("org/model/v2")],
                ))
                .await
                .expect("save");
            let resolved = store
                .find_model("open-router/org/model/v2")
                .await
                .expect("resolve");
            assert_eq!(resolved.api_key.expose_secret(), "top-secret");
            assert_eq!(resolved.model.upstream_id, "org/model/v2");
            assert_eq!(store.list_models().await.expect("list").len(), 1);
            let providers = store.list_providers().await.expect("providers");
            assert_eq!(providers[0].api_key_hint, "to…et");
            assert!(
                !serde_json::to_string(&providers)
                    .unwrap()
                    .contains("top-secret")
            );
        });
        cleanup(&path);
    }

    #[test]
    fn provider_can_be_disabled_without_deleting_and_enabled_again() {
        let path = test_path();
        run(async {
            let store = Store::open(&path, [7u8; 32]).await.expect("store");
            let saved = store
                .save_provider(provider("Switchable", "secret", vec![model("model-a")]))
                .await
                .expect("save");
            assert!(saved.enabled);
            assert_eq!(store.list_enabled_models().await.expect("enabled").len(), 1);

            assert!(
                store
                    .set_provider_enabled("switchable", false)
                    .await
                    .expect("disable")
            );
            let providers = store.list_providers().await.expect("providers");
            assert!(!providers[0].enabled);
            assert_eq!(providers[0].model_count, 1);
            assert_eq!(store.list_models().await.expect("all models").len(), 1);
            assert!(
                store
                    .list_enabled_models()
                    .await
                    .expect("enabled models")
                    .is_empty()
            );
            assert!(matches!(
                store.find_model("switchable/model-a").await,
                Err(StoreError::NotFound)
            ));

            assert!(
                store
                    .set_provider_enabled("switchable", true)
                    .await
                    .expect("enable")
            );
            assert!(store.find_model("switchable/model-a").await.is_ok());
            assert!(
                !store
                    .set_provider_enabled("missing-provider", false)
                    .await
                    .expect("missing provider")
            );
        });
        cleanup(&path);
    }

    #[test]
    fn existing_provider_table_is_migrated_as_enabled() {
        let path = test_path();
        let connection = Connection::open(&path).expect("legacy database");
        connection
            .execute_batch(
                "CREATE TABLE providers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    base_url TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    api_key_encrypted BLOB NOT NULL,
                    created_at INTEGER NOT NULL
                );",
            )
            .expect("legacy providers schema");
        drop(connection);

        run(async {
            let _store = Store::open(&path, [4u8; 32]).await.expect("migrated store");
            let connection = Connection::open(&path).expect("inspect database");
            let enabled_default: String = connection
                .query_row(
                    "SELECT dflt_value FROM pragma_table_info('providers') WHERE name = 'enabled'",
                    [],
                    |row| row.get(0),
                )
                .expect("enabled column");
            assert_eq!(enabled_default, "1");
        });
        cleanup(&path);
    }

    #[test]
    fn encryption_is_at_rest() {
        let path = test_path();
        run(async {
            let store = Store::open(&path, [9u8; 32]).await.expect("store");
            store
                .save_provider(provider("Encrypted", "plain-secret", vec![model("m")]))
                .await
                .expect("save");
            let connection = Connection::open(&path).expect("read database");
            let encrypted: Vec<u8> = connection
                .query_row("SELECT api_key_encrypted FROM providers", [], |row| {
                    row.get(0)
                })
                .expect("encrypted key");
            assert!(
                !encrypted
                    .windows("plain-secret".len())
                    .any(|window| window == b"plain-secret")
            );
            assert!(encrypted.len() > NONCE_BYTES);
        });
        cleanup(&path);
    }

    #[test]
    fn fallback_keys_are_encrypted_ordered_and_manageable() {
        let path = test_path();
        run(async {
            let store = Store::open(&path, [8u8; 32]).await.expect("store");
            let provider = provider("Fallback", "primary-secret", vec![model("m")])
                .with_fallback_api_keys(vec![
                    "fallback-secret-one".to_owned(),
                    "fallback-secret-two".to_owned(),
                ])
                .expect("fallback keys");
            let saved = store.save_provider(provider).await.expect("save");
            assert_eq!(saved.fallback_api_key_hints.len(), 2);
            let listed = store.list_providers().await.expect("list providers");
            assert_eq!(listed[0].fallback_api_key_hints.len(), 2);

            let resolved = store.find_model("fallback/m").await.expect("resolve");
            assert_eq!(resolved.api_key.expose_secret(), "primary-secret");
            assert_eq!(resolved.fallback_api_keys.len(), 2);
            assert_eq!(
                resolved.fallback_api_keys[0].expose_secret(),
                "fallback-secret-one"
            );
            assert_eq!(
                resolved.fallback_api_keys[1].expose_secret(),
                "fallback-secret-two"
            );

            assert_eq!(
                store
                    .set_fallback_api_key("fallback", 2, Some("primary-secret".to_owned()))
                    .await
                    .expect_err("duplicate key"),
                StoreError::Conflict
            );
            store
                .set_fallback_api_key("fallback", 1, None)
                .await
                .expect("remove first and compact");
            let resolved = store.find_model("fallback/m").await.expect("resolve again");
            assert_eq!(resolved.fallback_api_keys.len(), 1);
            assert_eq!(
                resolved.fallback_api_keys[0].expose_secret(),
                "fallback-secret-two"
            );

            let connection = Connection::open(&path).expect("read database");
            let encrypted = connection
                .query_row(
                    "SELECT api_key_encrypted FROM provider_fallback_keys",
                    [],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .expect("encrypted fallback");
            assert!(
                !encrypted
                    .windows(15)
                    .any(|window| window == b"fallback-secret")
            );
        });
        cleanup(&path);

        let too_many = provider("Too Many Keys", "primary", vec![model("m")])
            .with_fallback_api_keys(vec!["one".into(), "two".into(), "three".into()])
            .expect_err("at most two fallback keys");
        assert_eq!(too_many, StoreError::InvalidInput);
    }

    #[test]
    fn duplicate_slug_is_atomic_and_keeps_existing_provider() {
        let path = test_path();
        run(async {
            let store = Store::open(&path, [3u8; 32]).await.expect("store");
            store
                .save_provider(provider("Same Name", "first", vec![model("one")]))
                .await
                .expect("first save");
            let error = store
                .save_provider(provider("same-name", "second", vec![model("two")]))
                .await
                .expect_err("duplicate slug");
            assert_eq!(error, StoreError::Conflict);
            let providers = store.list_providers().await.expect("providers");
            assert_eq!(providers.len(), 1);
            assert_eq!(providers[0].model_count, 1);
            assert_eq!(
                store
                    .find_model("same-name/one")
                    .await
                    .expect("old model")
                    .api_key
                    .expose_secret(),
                "first"
            );
            assert!(matches!(
                store.find_model("same-name/two").await,
                Err(StoreError::NotFound)
            ));
        });
        cleanup(&path);
    }

    #[test]
    fn delete_cascades_models() {
        let path = test_path();
        run(async {
            let store = Store::open(&path, [5u8; 32]).await.expect("store");
            store
                .save_provider(provider("Cascade", "key", vec![model("a"), model("b")]))
                .await
                .expect("save");
            assert!(store.delete_provider("cascade").await.expect("delete"));
            assert!(store.list_models().await.expect("models").is_empty());
            assert!(
                !store
                    .delete_provider("cascade")
                    .await
                    .expect("delete absent")
            );
        });
        cleanup(&path);
    }

    #[test]
    fn model_limit_is_rejected_before_transaction() {
        let path = test_path();
        run(async {
            let store = Store::open(&path, [1u8; 32]).await.expect("store");
            let models = (0..=MAX_MODELS_PER_PROVIDER)
                .map(|index| model(&format!("model-{index}")))
                .collect();
            let error = store
                .save_provider(provider("Too Many", "key", models))
                .await
                .expect_err("model limit");
            assert_eq!(error, StoreError::InvalidInput);
            assert!(store.list_providers().await.expect("providers").is_empty());
        });
        cleanup(&path);
    }

    #[test]
    fn existing_usage_table_is_migrated_with_zero_failures() {
        let path = test_path();
        let connection = Connection::open(&path).expect("legacy database");
        connection
            .execute_batch(
                "CREATE TABLE usage_hourly (
                    bucket_start INTEGER NOT NULL,
                    provider_slug TEXT NOT NULL,
                    model_alias TEXT NOT NULL,
                    requests INTEGER NOT NULL DEFAULT 0,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    cache_tokens INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(bucket_start, provider_slug, model_alias)
                );
                INSERT INTO usage_hourly
                    (bucket_start, provider_slug, model_alias, requests,
                     input_tokens, output_tokens, cache_tokens)
                VALUES (0, 'legacy', 'legacy/model', 3, 4, 5, 6);",
            )
            .expect("legacy schema");
        drop(connection);

        run(async {
            let store = Store::open(&path, [3u8; 32]).await.expect("migrated store");
            let stats = store.usage_stats(0, 3_600).await.expect("legacy stats");
            assert_eq!(stats.summary.requests, 3);
            assert_eq!(stats.summary.failed_requests, 0);
            store
                .record_usage("legacy", "legacy/model", 0, 1, TokenUsage::default())
                .await
                .expect("record failure");
            let stats = store.usage_stats(0, 3_600).await.expect("updated stats");
            assert_eq!(stats.summary.requests, 3);
            assert_eq!(stats.summary.failed_requests, 1);
        });
        cleanup(&path);
    }

    #[test]
    fn usage_is_aggregated_by_model_and_time_bucket() {
        let path = test_path();
        run(async {
            let store = Store::open(&path, [2u8; 32]).await.expect("store");
            store
                .save_provider(provider("Provider", "key", vec![model("model-a")]))
                .await
                .expect("save provider");
            store
                .record_usage(
                    "provider",
                    "provider/model-a",
                    1,
                    0,
                    TokenUsage {
                        input_tokens: 8,
                        output_tokens: 3,
                        cache_tokens: 5,
                    },
                )
                .await
                .expect("first usage");
            store
                .record_usage(
                    "provider",
                    "provider/model-a",
                    1,
                    1,
                    TokenUsage {
                        input_tokens: 2,
                        output_tokens: 1,
                        cache_tokens: 0,
                    },
                )
                .await
                .expect("second usage");
            let stats = store.usage_stats(0, 3_600).await.expect("stats");
            assert_eq!(stats.summary.requests, 2);
            assert_eq!(stats.summary.failed_requests, 1);
            assert_eq!(stats.summary.input_tokens, 10);
            assert_eq!(stats.summary.output_tokens, 4);
            assert_eq!(stats.summary.cache_tokens, 5);
            assert_eq!(stats.series.len(), 1);
            assert_eq!(stats.models[0].model, "provider/model-a");
            assert_eq!(
                store
                    .delete_model_usage("provider/model-a")
                    .await
                    .expect_err("active model must be protected"),
                StoreError::Conflict
            );
            assert!(store.delete_provider("provider").await.expect("provider"));
            assert!(
                store
                    .delete_model_usage("provider/model-a")
                    .await
                    .expect("archived usage")
            );
            assert!(
                !store
                    .delete_model_usage("provider/model-a")
                    .await
                    .expect("already deleted")
            );
            assert_eq!(
                store
                    .usage_stats(0, 3_600)
                    .await
                    .expect("empty stats")
                    .summary,
                UsageTotals::default()
            );
        });
        cleanup(&path);
    }
}
