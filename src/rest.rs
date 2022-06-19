use crate::{cache::local, matcher};
use actix_web::{
    http::{self, header},
    web::{self},
    HttpRequest, HttpResponse, HttpResponseBuilder, ResponseError,
};
use derive_more::{Display, Error};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

pub struct Handler {
    caches: HashMap<&'static str, local::Local>,
    linker: &'static matcher::ContextLinker,
}

#[derive(Serialize, Deserialize)]
pub struct Response {
    pub allowed: bool,
}

#[derive(Debug, Display, Serialize, Deserialize)]
#[display(fmt = "{}", msg)]
pub struct SepulcherError {
    msg: String,
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    code: actix_web::http::StatusCode,
}

impl ResponseError for SepulcherError {
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
    pub fn new(linker: &'static matcher::ContextLinker) -> &'static Handler {
        let mut caches = HashMap::new();

        for (k, v) in linker.get_ttls() {
            caches.insert(k.as_str(), local::Local::new(local::DEFAULT_PARTITIONS, *v));
        }

        Box::leak(Box::new(Handler { caches, linker }))
    }

    pub async fn handle<'a>(
        parent: web::Data<&'a Handler>,
        req: HttpRequest,
    ) -> Result<HttpResponse, actix_web::Error> {
        let collection = req.match_info().get("collection").ok_or(SepulcherError {
            msg: "missing collection parameter".to_string(),
            code: http::StatusCode::BAD_REQUEST,
        })?;

        let key = req.match_info().get("key").ok_or(SepulcherError {
            msg: "missing key parameter".to_string(),
            code: http::StatusCode::BAD_REQUEST,
        })?;

        let link = parent.linker.get_context(collection).ok_or({
            SepulcherError {
                msg: "cannot find linker for collection parameter".to_string(),
                code: http::StatusCode::BAD_REQUEST,
            }
        })?;

        let cache = parent.caches.get(collection).ok_or(SepulcherError {
            msg: "no cache for collection parameter".to_string(),
            code: http::StatusCode::BAD_REQUEST,
        })?;

        // these need logging
        let val = cache.get_or_create(key, true).map_err(|e| {
            println!("can't get_or_create val: {}", e);
            SepulcherError {
                msg: format!("failed to get_or_create val: {}", e),
                code: http::StatusCode::INTERNAL_SERVER_ERROR,
            }
        })?;

        if val >= link.rate as u64 {
            return Ok(HttpResponse::build(http::StatusCode::OK)
                .insert_header(header::ContentType::json())
                .body(json!(Response { allowed: false }).to_string()));
        }

        let mut linked = 0;
        for _ in &link.contexts {
            linked += cache.get_or_create(key, false).map_err(|e| {
                println!("can't get val: {}", e);
                SepulcherError {
                    msg: format!("failed to get val: {}", e),
                    code: http::StatusCode::BAD_REQUEST,
                }
            })?;
        }

        let mut resp = HttpResponse::build(http::StatusCode::OK);
        let resp = resp.insert_header(header::ContentType::json());

        match linked >= link.rate {
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
    async fn test_handle() {
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
        let parsed: Response = serde_json::from_slice(&body[..]).expect("cannot parse as Response");
        assert!(parsed.allowed);

        let resp = Handler::handle(data, req)
            .await
            .expect("unexpected handler error");
        assert_eq!(resp.status(), http::StatusCode::OK);

        let body = resp
            .into_body()
            .try_into_bytes()
            .expect("unable to ready body");
        let parsed: Response = serde_json::from_slice(&body[..]).expect("cannot parse as Response");

        assert!(!parsed.allowed);
    }

    #[test]
    async fn test_handle_errors() {
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

        let resp = do_test_request("http://localhost", Some("foobar"), None, data.clone())
            .await
            .expect_err("did not error as expected")
            .error_response();
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);

        let body = resp
            .into_body()
            .try_into_bytes()
            .expect("unable to ready body");
        let parsed: SepulcherError =
            serde_json::from_slice(&body[..]).expect("cannot parse as SepulcherError");
        assert!(parsed.msg.contains("missing collection parameter"));

        let resp = do_test_request("http://localhost", None, Some("foo"), data.clone())
            .await
            .expect_err("did not error as expected")
            .error_response();
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);

        let body = resp
            .into_body()
            .try_into_bytes()
            .expect("unable to ready body");
        let parsed: SepulcherError =
            serde_json::from_slice(&body[..]).expect("cannot parse as SepulcherError");
        assert!(parsed.msg.contains("missing key parameter"));

        let resp = do_test_request(
            "http://localhost",
            Some("foobar"),
            Some("foobar"),
            data.clone(),
        )
        .await
        .expect_err("did not error as expected")
        .error_response();
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);

        let body = resp
            .into_body()
            .try_into_bytes()
            .expect("unable to ready body");
        let parsed: SepulcherError =
            serde_json::from_slice(&body[..]).expect("cannot parse as SepulcherError");
        assert!(parsed.msg.contains("cannot find linker"));
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
