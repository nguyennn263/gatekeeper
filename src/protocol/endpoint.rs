use super::common::{ProtocolError, UpstreamProtocol};

/// Return the endpoint relative to a configured provider base URL.
pub fn upstream_path(protocol: UpstreamProtocol) -> &'static str {
    match protocol {
        UpstreamProtocol::OpenAi => "/chat/completions",
        UpstreamProtocol::Anthropic => "/messages",
        UpstreamProtocol::Responses => "/responses",
    }
}

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

    let base_path = &base[authority_end..];
    let joined = if base_path.is_empty() || base_path == "/" {
        format!("{base}/{path}")
    } else {
        format!("{}/{}", base.trim_end_matches('/'), path)
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
    fn rejects_credentials_and_traversal() {
        assert!(build_endpoint_url("https://user:pass@example.test", "/messages").is_err());
        assert!(build_endpoint_url("https://example.test", "/../messages").is_err());
        assert!(build_endpoint_url("https://example.test?key=secret", "/messages").is_err());
    }
}
