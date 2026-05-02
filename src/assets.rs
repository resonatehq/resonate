use axum::{
    body::Body,
    http::{header, HeaderValue, Response, StatusCode},
    response::IntoResponse,
};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "web/dist/"]
struct Assets;

pub async fn web_handler(uri: axum::http::Uri) -> impl IntoResponse {
    let raw = uri.path();
    let path = raw.strip_prefix("/web").unwrap_or(raw);
    let path = path.strip_prefix('/').unwrap_or(path);

    let resolved = if path.is_empty() || !path.contains('.') {
        "index.html"
    } else {
        path
    };

    serve(resolved).await
}

async fn serve(path: &str) -> Response<Body> {
    match Assets::get(path) {
        Some(content) => {
            let mime = content.metadata.mimetype();
            let etag = fnv64_hex(&content.data);
            Response::builder()
                .status(StatusCode::OK)
                .header(
                    header::CONTENT_TYPE,
                    HeaderValue::from_str(mime)
                        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
                )
                .header(
                    header::ETAG,
                    HeaderValue::from_str(&etag)
                        .unwrap_or_else(|_| HeaderValue::from_static("")),
                )
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => {
            // SPA fallback for unknown paths
            match Assets::get("index.html") {
                Some(content) => {
                    let etag = fnv64_hex(&content.data);
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, HeaderValue::from_static("text/html"))
                        .header(
                            header::ETAG,
                            HeaderValue::from_str(&etag)
                                .unwrap_or_else(|_| HeaderValue::from_static("")),
                        )
                        .body(Body::from(content.data.into_owned()))
                        .unwrap()
                }
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap(),
            }
        }
    }
}

fn fnv64_hex(data: &[u8]) -> String {
    let mut hash: u64 = 14695981039346656037;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    format!("{:016x}", hash)
}
