use axum::http::{HeaderMap, header::AUTHORIZATION};

use crate::{config::Secret, error::AppError};

pub fn require_bearer(headers: &HeaderMap, expected: &Secret) -> Result<(), AppError> {
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(AppError::Unauthorized)?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or(AppError::Unauthorized)?;
    if expected.matches(token) {
        Ok(())
    } else {
        Err(AppError::Unauthorized)
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};

    use super::require_bearer;
    use crate::config::Secret;

    #[test]
    fn bearer_auth_is_strict() {
        let expected = Secret::new("0123456789abcdef".to_owned(), "test").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer 0123456789abcdef"),
        );
        assert!(require_bearer(&headers, &expected).is_ok());
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("bearer 0123456789abcdef"),
        );
        assert!(require_bearer(&headers, &expected).is_err());
    }
}
