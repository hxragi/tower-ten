use http::Response;

pub fn classify_http_5xx<B>(response: &Response<B>) -> bool {
    !response.status().is_server_error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Response, StatusCode};

    fn response(status: StatusCode) -> Response<()> {
        Response::builder().status(status).body(()).unwrap()
    }

    #[test]
    fn classifies_server_errors_as_failure() {
        for status in [
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            assert!(
                !classify_http_5xx(&response(status)),
                "status {status} must be a failure"
            );
        }
    }

    #[test]
    fn classifies_non_server_errors_as_success() {
        for status in [
            StatusCode::OK,
            StatusCode::CREATED,
            StatusCode::NO_CONTENT,
            StatusCode::MOVED_PERMANENTLY,
            StatusCode::BAD_REQUEST,
            StatusCode::NOT_FOUND,
            StatusCode::UNAUTHORIZED,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert!(
                classify_http_5xx(&response(status)),
                "status {status} must be a success"
            );
        }
    }
}
