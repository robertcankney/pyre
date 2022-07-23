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
    caches: HashMap<&'static str, std::sync::Arc<local::Local>>,
    linker: &'static matcher::ContextLinker,
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
    // TODO remove unnecessary statics for config
    pub fn new(linker: &'static matcher::ContextLinker) -> &'static Handler {
        let mut caches = HashMap::new();

        for (k, v) in linker.get_ttls() {
            let local = std::sync::Arc::new(local::Local::new(
                local::DEFAULT_PARTITIONS,
                *v,
                local::DEFAULT_WINDOW,
                linker.sweep,
            ));
            local::Local::start_lru(local.clone());
            local::Local::start_clock(local.clone());
            caches.insert(k.as_str(), local);
        }

        Box::leak(Box::new(Handler { caches, linker }))
    }

    #[instrument]
    pub async fn handle(
        parent: web::Data<&Handler>,
        req: HttpRequest,
    ) -> Result<HttpResponse, actix_web::Error> {
        let coll = req.match_info().get("collection").ok_or_else(|| {
            event!(Level::INFO, "no collection URL parameter");

            HTTPError {
                msg: "missing collection parameter".to_string(),
                code: http::StatusCode::BAD_REQUEST,
            }
        })?;

        let key = req.match_info().get("key").ok_or_else(|| {
            event!(Level::INFO, "no key URL parameter");

            HTTPError {
                msg: "missing key parameter".to_string(),
                code: http::StatusCode::BAD_REQUEST,
            }
        })?;

        let cache = parent.caches.get(coll).ok_or_else(|| {
            event!(
                Level::ERROR,
                message = "no cache for collection parameter, but linker already found with it",
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

        let mut linked = 0;
        for _ in &link.contexts {
            linked += cache.get_or_create(key, false).map_err(|e| {
                event!(Level::ERROR, message = "can't get or create val", error = %e);

                HTTPError {
                    msg: format!("failed to get val: {}", e),
                    code: http::StatusCode::INTERNAL_SERVER_ERROR,
                }
            })?;
        }

        let mut resp = HttpResponse::build(http::StatusCode::OK);
        let resp = resp.insert_header(header::ContentType::json());

        match linked > link.rate {
            true => Ok(resp.body(json!(Response { allowed: false }).to_string())),
            false => Ok(resp.body(json!(Response { allowed: true }).to_string())),
        }
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

        let leaked = Box::leak(Box::new(linker));
        let handler = Handler::new(leaked);

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

    #[test]
    async fn test_handle_rate() {
        struct TestCase {
            allowed: bool,
            name: &'static str,
        }

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

        let leaked = Box::leak(Box::new(allow_two_linker));
        let handler = Handler::new(leaked);
        let data = web::Data::new(handler);

        let testcases = vec![
            TestCase {
                name: "initially allowed",
                allowed: true,
            },
            TestCase {
                name: "still allowed",
                allowed: true,
            },
            TestCase {
                name: "no longer allowed",
                allowed: false,
            },
        ];

        for tc in testcases {
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
            assert_eq!(
                parsed.allowed, tc.allowed,
                "allowed value not correct for {}",
                tc.name
            );
        }
    }

    #[test]
    async fn test_handle_errors() {
        struct TestCase {
            name: &'static str,
            code: http::StatusCode,
            body: &'static str,
            collection: Option<&'static str>,
            key: Option<&'static str>,
        }

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

        let leaked = Box::leak(Box::new(linker));
        let handler = Handler::new(leaked);
        let data = web::Data::new(handler);

        let testcases = vec![
            TestCase {
                name: "missing collection parameter",
                code: http::StatusCode::BAD_REQUEST,
                body: "missing collection parameter",
                collection: None,
                key: Some("foo"),
            },
            TestCase {
                name: "missing key parameter",
                code: http::StatusCode::BAD_REQUEST,
                body: "missing key parameter",
                collection: Some("foo"),
                key: None,
            },
            TestCase {
                name: "invalid collection parameter",
                code: http::StatusCode::BAD_REQUEST,
                body: "no cache for collection parameter",
                collection: Some("foobar"),
                key: Some("bar"),
            },
        ];

        for tc in testcases {
            let resp = do_test_request("http://localhost", tc.key, tc.collection, data.clone())
                .await
                .expect_err(format!("{} did not error as expected", tc.name).as_str())
                .error_response();
            assert_eq!(
                resp.status(),
                tc.code,
                "unexpected status code for {}",
                tc.name
            );

            let body = resp
                .into_body()
                .try_into_bytes()
                .expect("unable to ready body");
            let parsed: HTTPError =
                serde_json::from_slice(&body[..]).expect("cannot parse as HTTPError");
            assert!(
                parsed.msg.contains(tc.body),
                "body does not match expected for {}: {}",
                tc.name,
                parsed.msg
            );
        }
    }

    async fn do_test_request(
        uri: &'static str,
        key: Option<&'static str>,
        collection: Option<&'static str>,
        data: web::Data<&Handler>,
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
