use crate::{cache::local, matcher};
use actix_web::{
    http::{self, header},
    web::{self},
    HttpRequest, HttpResponse, HttpResponseBuilder, ResponseError,
};
use derive_more::Display;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::{event, instrument, Level};

#[derive(Debug)]
pub struct Handler {
    caches: HashMap<String, std::sync::Arc<local::Local>>,
    linker: matcher::ContextLinker,
}

#[derive(Serialize, Deserialize)]
pub struct Response {
    pub allowed: bool,
}

#[derive(Debug, Display, Serialize, Deserialize)]
#[display(fmt = "{}", msg)]
pub struct HTTPError {
    msg: String,
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    code: actix_web::http::StatusCode,
}

impl ResponseError for HTTPError {
    fn status_code(&self) -> http::StatusCode {
        self.code
    }

    fn error_response(&self) -> HttpResponse<actix_web::body::BoxBody> {
        let mut res = HttpResponseBuilder::new(self.status_code());
        let b = serde_json::to_string(self).expect("failed to serialize response error");

        res.content_type(header::ContentType::json())
            .body(actix_web::body::BoxBody::new(b))
    }
}

impl Handler {

    pub fn new(linker: matcher::ContextLinker) -> Handler {
        let mut caches = HashMap::new();

        for (k, v) in linker.get_ttls() {
            let local = std::sync::Arc::new(local::Local::new(
                local::DEFAULT_PARTITIONS,
                *v,
                local::DEFAULT_WINDOW,
                linker.sweep,
            ));
            local.start_lru();
            local.start_clock();
            caches.insert(k.to_owned(), local);
        }

        Handler { caches, linker }
    }

    #[instrument]
    pub async fn handle(
        parent: web::Data<Handler>,
        req: HttpRequest,
    ) -> Result<HttpResponse, actix_web::Error> {
        let span = tracing::error_span!("error_span! macro");
        span.in_scope(|| {
            tracing::error!("error! macro");
        });

        let coll = req.match_info().get("collection").ok_or_else(|| {
            tracing::error!("no collection URL parameter");

            HTTPError {
                msg: "missing collection parameter".to_string(),
                code: http::StatusCode::BAD_REQUEST,
            }
        })?;

        let key = req.match_info().get("key").ok_or_else(|| {
            tracing::info!("no key URL parameter");

            HTTPError {
                msg: "missing key parameter".to_string(),
                code: http::StatusCode::BAD_REQUEST,
            }
        })?;

        let cache = parent.caches.get(coll).ok_or_else(|| {
            event!(
                Level::ERROR,
                message = "no cache found for provided collection parameter",
                collection = coll
            );

            HTTPError {
                msg: "no cache for collection parameter".to_string(),
                code: http::StatusCode::BAD_REQUEST,
            }
        })?;

        let link = parent.linker.get_context(coll).ok_or_else(|| {
            event!(
                Level::INFO,
                message = "no linker found for collection, even though cache was found",
                collection = coll,
            );

            HTTPError {
                msg: "cannot find linker for collection parameter".to_string(),
                code: http::StatusCode::INTERNAL_SERVER_ERROR,
            }
        })?;

        // these need logging
        let val = cache.get_or_create(key, true).map_err(|e| {
            event!(Level::ERROR, message = "can't get or create val", error = %e);

            HTTPError {
                msg: format!("failed to get_or_create val: {}", e),
                code: http::StatusCode::INTERNAL_SERVER_ERROR,
            }
        })?;

        if val > link.rate as u64 {
            event!(Level::DEBUG, "value already above limit");

            return Ok(HttpResponse::build(http::StatusCode::OK)
                .insert_header(header::ContentType::json())
                .body(json!(Response { allowed: false }).to_string()));
        }

        let mut linked = Self::get_linked_vals(&parent, &key, &link)?;
        let mut resp = HttpResponse::build(http::StatusCode::OK);
        let resp = resp.insert_header(header::ContentType::json());

        match linked > link.rate {
            true => Ok(resp.body(json!(Response { allowed: false }).to_string())),
            false => Ok(resp.body(json!(Response { allowed: true }).to_string())),
        }
    }

    // get_linked_vals gets values from linked contexts, if any, for the context
    fn get_linked_vals(handler: &web::Data<Handler>, key: &str, link: &matcher::Link) -> Result<u64, HTTPError> {
        let mut linked = 0;

        for k in link.contexts.iter() {
            let related = match handler.caches.get(k) {
                Some(r) => r,
                None => continue,
            };

            linked += related.get_or_create(key, false).map_err(|e| {
                event!(Level::ERROR, message = "can't get val", error = %e);

                HTTPError {
                    msg: format!("failed to get val: {}", e),
                    code: http::StatusCode::INTERNAL_SERVER_ERROR,
                }
            })?;
        }

        Ok(linked)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use actix_web::{body::MessageBody, test};

    #[test]
    async fn test_new_handler() {
        let mut linker: matcher::ContextLinker = matcher::ContextLinker {
            ttls: HashMap::default(),
            contexts: HashMap::default(),
            sweep: 30,
        };

        linker.ttls.insert("foo".to_string(), 10);
        linker.ttls.insert("bar".to_string(), 20);

        let handler = Handler::new(linker);

        assert_eq!(
            handler
                .caches
                .get("foo")
                .expect("no cache with key foo")
                .ttl(),
            10
        );

        assert_eq!(
            handler
                .caches
                .get("bar")
                .expect("no cache with key foo")
                .ttl(),
            20
        );
    }

    macro_rules! handle_rate_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            async fn $name() {

                let (count, allowed) = $value;

        let allow_two_linker = matcher::ContextLinker::new(
            r#"
            {
                "linkers": [
                    {
                        "name": "foo",
                        "contexts": ["bar"],
                        "rate": {
                            "count": 2,
                            "ttl_seconds": 60
                        }
                    },
                    {
                        "name": "bar",
                        "contexts": ["foo"],
                        "rate": {
                            "count": 2,
                            "ttl_seconds": 60
                        }
                    }
                ],
                "sweep_seconds": 30
            }
            "#,
        )
        .expect("failed to create linker");

        let handler = Handler::new(allow_two_linker);
        let data = web::Data::new(handler);

        let mut limited = true;

        for _ in 0..count {
            let req = test::TestRequest::with_uri("http://localhost")
                .param("key", "foobar")
                .param("collection", "foo")
                .method(http::Method::GET)
                .to_http_request();
            let resp = Handler::handle(data.clone(), req.clone())
                .await
                .expect("unexpected handler error");
            assert_eq!(resp.status(), http::StatusCode::OK);

            let body = resp
                .into_body()
                .try_into_bytes()
                .expect("unable to ready body");
            let parsed: Response =
                serde_json::from_slice(&body[..]).expect("cannot parse as Response");
            limited = parsed.allowed;
        }
                assert_eq!(allowed, limited);
            }
        )*
        }
    }

    handle_rate_tests! {
        handle_rate_one_request: (1, true),
        handle_rate_two_requests: (2, true),
        handle_rate_three_requests: (3, false),
    }


    macro_rules! handle_errors_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[test]
                async fn $name() {
                    let (code, err, collection, key) = $value; 

                    let linker = matcher::ContextLinker::new(
                        r#"
                        {
                            "linkers": [
                                {
                                    "name": "foo",
                                    "contexts": ["bar"],
                                    "rate": {
                                        "count": 2,
                                        "ttl_seconds": 60
                                    }
                                },
                                {
                                    "name": "bar",
                                    "contexts": ["foo"],
                                    "rate": {
                                        "count": 2,
                                        "ttl_seconds": 60
                                    }
                                }
                            ],
                            "sweep_seconds": 30
                        }
                        "#,
                    )
                    .expect("failed to create linker");

                    let handler = Handler::new(linker);
                    let data = web::Data::new(handler);

                    let resp = do_test_request("http://localhost", key, collection, data.clone())
                    .await
                    .expect_err("did not error as expected")
                    .error_response();

                    assert_eq!(
                        resp.status(),
                        code,
                        "unexpected status code",
                    );
        
                    let body = resp
                        .into_body()
                        .try_into_bytes()
                        .expect("unable to ready body");
                    let parsed: HTTPError =
                        serde_json::from_slice(&body[..]).expect("cannot parse as HTTPError");
                    assert!(
                        parsed.msg.contains(err),
                        "body does not match expected: {}",
                        parsed.msg
                    );
                }
    
            )*
        }
    }

    handle_errors_tests! {
        handle_missing_collection_parameter: (
            http::StatusCode::BAD_REQUEST,
            "missing collection parameter",
            None,
            Some("foo"),
        ),
        handle_missing_key_parameter: (
            http::StatusCode::BAD_REQUEST,
            "missing key parameter",
            Some("foo"),
            None,
        ),
        handle_invalid_collection_parameter: (
            http::StatusCode::BAD_REQUEST,
            "no cache for collection parameter",
            Some("foobar"),
            Some("bar"),
        ),
    }

    async fn do_test_request(
        uri: &'static str,
        key: Option<&'static str>,
        collection: Option<&'static str>,
        data: web::Data<Handler>,
    ) -> Result<HttpResponse, actix_web::Error> {
        let mut req = test::TestRequest::with_uri(uri);
        if let Some(c) = collection {
            req = req.param("collection", c);
        }

        if let Some(k) = key {
            req = req.param("key", k);
        }

        Handler::handle(data, req.to_http_request()).await
    }
}
