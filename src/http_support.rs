use http::Response;

pub fn classifiy_http_5xx<B>(reponse: &Response<B>) -> bool {
    !reponse.status().is_server_error()
}
