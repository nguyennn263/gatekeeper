use super::common::{ProtocolError, UpstreamProtocol};

/// Return the endpoint relative to a configured provider base URL.
pub fn upstream_path(protocol: UpstreamProtocol) -> &'static str {
    match protocol {
        UpstreamProtocol::OpenAi => "/chat/completions",
        UpstreamProtocol::Anthropic => "/messages",
        UpstreamProtocol::Responses => "/responses",
    }
}

/// Prefix assumed for a bare-origin base URL, so `https://host` and
/// `https://host/v1` resolve to the same endpoints.
pub const DEFAULT_VERSION_PREFIX: &str = "/v1";

/// Safely append a fixed endpoint path to a provider base URL.
///
/// The function rejects credentials, fragments, queries, control characters,
/// and traversal segments.  It intentionally accepts only HTTP(S), and never
/// treats an endpoint path as a replacement URL.
pub fn build_endpoint_url(base_url: &str, endpoint_path: &str) -> Result<String, ProtocolError> {
    let base = base_url.trim();
    if base.is_empty()
        || base
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b' ')
    {
        return Err(ProtocolError::endpoint(
            "base URL is empty or contains whitespace",
        ));
    }
    let scheme_end = base
        .find("://")
        .ok_or_else(|| ProtocolError::endpoint("base URL must use http or https"))?;
    let scheme = &base[..scheme_end];
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return Err(ProtocolError::endpoint("base URL must use http or https"));
    }
    let authority_start = scheme_end + 3;
    let authority_end = base[authority_start..]
        .find(['/', '?', '#'])
        .map(|offset| authority_start + offset)
        .unwrap_or(base.len());
    let authority = &base[authority_start..authority_end];
    if authority.is_empty() || authority.contains('@') {
        return Err(ProtocolError::endpoint(
            "base URL must contain a host without credentials",
        ));
    }
    if authority.starts_with(':') || authority.ends_with(':') {
        return Err(ProtocolError::endpoint("base URL has an invalid host"));
    }
    if base[authority_end..].contains(['?', '#']) {
        return Err(ProtocolError::endpoint(
            "base URL must not contain a query or fragment",
        ));
    }

    let path = endpoint_path.trim();
    if path.is_empty()
        || path.contains(['?', '#'])
        || path
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b' ')
    {
        return Err(ProtocolError::endpoint("endpoint path is invalid"));
    }
    let path = path.trim_start_matches('/');
    if path.is_empty()
        || path
            .split('/')
            .any(|segment| segment == "." || segment == "..")
    {
        return Err(ProtocolError::endpoint("endpoint path contains traversal"));
    }
    let lower = path.to_ascii_lowercase();
    if lower.contains("%2e") || lower.contains("%2f") || lower.contains("%5c") {
        return Err(ProtocolError::endpoint(
            "endpoint path contains encoded traversal",
        ));
    }

    let origin = &base[..authority_end];
    let base_path = base[authority_end..].trim_end_matches('/');
    let joined = if base_path.is_empty() {
        format!("{origin}{DEFAULT_VERSION_PREFIX}/{path}")
    } else {
        format!("{origin}{base_path}/{path}")
    };
    Ok(joined)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_provider_prefix_without_losing_it() {
        assert_eq!(
            build_endpoint_url("https://example.test/v1/", "/chat/completions").unwrap(),
            "https://example.test/v1/chat/completions"
        );
    }

    #[test]
    fn bare_origin_gets_default_version_prefix() {
        for base in ["https://example.test", "https://example.test/"] {
            assert_eq!(
                build_endpoint_url(base, "/chat/completions").unwrap(),
                "https://example.test/v1/chat/completions"
            );
        }
        assert_eq!(
            build_endpoint_url("https://example.test/v1beta/openai/", "/chat/completions").unwrap(),
            "https://example.test/v1beta/openai/chat/completions"
        );
    }

    #[test]
    fn rejects_credentials_and_traversal() {
        assert!(build_endpoint_url("https://user:pass@example.test", "/messages").is_err());
        assert!(build_endpoint_url("https://example.test", "/../messages").is_err());
        assert!(build_endpoint_url("https://example.test?key=secret", "/messages").is_err());
    }
}
