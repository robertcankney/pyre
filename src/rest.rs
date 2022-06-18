use crate::{cache::local, matcher};
use actix_web::{
    http::{self, header},
    web::{self},
    HttpRequest, HttpResponse,
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

#[derive(Debug, Display, Error)]
pub enum SepulcherErrors {
    #[display(fmt = "internal error")]
    InternalError,

    #[display(fmt = "bad request")]
    MissingCollection,

    #[display(fmt = "bad request")]
    MissingKey,
}

impl actix_web::error::ResponseError for SepulcherErrors {
    fn status_code(&self) -> actix_web::http::StatusCode {
        match *self {
            SepulcherErrors::InternalError => http::StatusCode::INTERNAL_SERVER_ERROR,
            SepulcherErrors::MissingCollection => http::StatusCode::BAD_REQUEST,
            SepulcherErrors::MissingKey => http::StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let mut res = HttpResponse::build(self.status_code());
        let res = res.insert_header(actix_web::http::header::ContentType::json());

        match self {
            SepulcherErrors::MissingCollection => {
                res.body(json!({ "msg": format!("{}: missing collection parameter", self.to_string())}).to_string())
            },
            SepulcherErrors::MissingKey => {
                res.body(json!({ "msg": format!("{}: missing key parameter", self.to_string())}).to_string())
            }
            _ => res.body(
                json!(
                    {
                        "msg": self.to_string()
                    }
                )
                .to_string(),
            ),
        }
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
    ) -> Result<HttpResponse, SepulcherErrors> {
        let collection =
            req.match_info()
                .get("collection")
                .ok_or(Err(SepulcherErrors::MissingCollection))?;

        let key = req
            .match_info()
            .get("key")
            .ok_or(Err(SepulcherErrors::MissingKey))?;

        let link = parent
            .linker
            .get_context(collection)
            .ok_or({
                println!("can't get linker context {}", collection);
                SepulcherErrors::InternalError
            })?;

        let cache = parent
            .caches
            .get(collection)
            .ok_or({
                println!("can't get cache collection {}", collection);
                SepulcherErrors::BadClientData(
                "no cache for collection parameter".to_string())
            })?;

        // these need logging
        let val = cache
            .get_or_create(key, true)
            .map_err(|e| { 
                println!("can't get val: {}", e);
                SepulcherErrors::InternalError
            })?;

        if link.rate as u64 >= val {
            return Ok(HttpResponse::build(http::StatusCode::OK)
                .insert_header(header::ContentType::json())
                .body(json!(Response { allowed: false }).to_string()));
        }

        let mut linked = 0;
        for _ in &link.contexts {
            linked += cache
                .get_or_create(key, false)
                .map_err(|_| SepulcherErrors::InternalError)?;
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
    use actix_web::{test, body::MessageBody};

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
            "#
        ).expect("failed to create linker");

        let leaked = Box::leak(Box::new(linker));
        let handler = Handler::new(leaked);
        let data = web::Data::new(handler);

        let req = test::TestRequest::with_uri("http://localhost")
            .param("collection", "foo")
            .param("key", "foobar")
            .method(http::Method::GET)
            .to_http_request();
        let resp = Handler::handle(data.clone(), req.clone())
            .await
            .expect("unexpected handler error");
        assert_eq!(resp.status(), http::StatusCode::OK);

        //        let body = test::read_body(actix_web::dev::ServiceResponse::new(
        //            req.clone(),
        //            resp,
        //       ))
        let body = resp.into_body().try_into_bytes().expect("unable to ready body");
        let parsed: Response = serde_json::from_slice(&body[..]).expect("cannot parse as Response");
        assert!(parsed.allowed);

        let resp = Handler::handle(data, req)
            .await
            .expect("unexpected handler error");
        assert_eq!(resp.status(), http::StatusCode::OK);

        let body = resp.into_body().try_into_bytes().expect("unable to ready body");
        let parsed: Response = serde_json::from_slice(&body[..]).expect("cannot parse as Response");
        
        assert!(!parsed.allowed);
    }
}
