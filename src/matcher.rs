use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::{collections::HashMap, hash::Hash, ops::Deref};

#[derive(Debug, PartialEq)]
pub struct ContextLinker {
    pub contexts: HashMap<String, Link>,
    pub ttls: HashMap<String, i64>,
}

#[derive(Debug, PartialEq)]
pub struct Link {
    pub contexts: Vec<String>,
    pub rate: u64,
}

#[derive(Serialize, Deserialize)]
struct ContextLinkerConfig {
    linkers: Vec<ContextLinkerSub>,
    sweep_seconds: i64,
}

#[derive(Serialize, Deserialize)]
struct ContextLinkerSub {
    name: String,
    contexts: Vec<String>,
    rate: RateConfig,
}

#[derive(Serialize, Deserialize)]
struct RateConfig {
    count: i64,
    ttl_seconds: i64,
}

impl ContextLinker {
    pub fn new(val: &str) -> std::io::Result<&'static ContextLinker> {
        let cfg: ContextLinkerConfig = serde_json::from_str(val)?;

        let mut linker = ContextLinker {
            contexts: Default::default(),
            ttls: Default::default(),
        };

        for link in &cfg.linkers {
            linker.contexts.insert(
                link.name.clone(),
                Link {
                    contexts: link
                        .contexts
                        .iter()
                        .filter(|ctx| cfg.linkers.iter().any(|lnk| lnk.name.eq(ctx.deref())))
                        .map(|ctx| ctx.clone())
                        .collect(),
                    rate: link.rate.count as u64,
                },
            );
            linker.ttls.insert(link.name.clone(), link.rate.ttl_seconds);
        }

        // mixed feelings on this
        Ok(Box::leak(Box::new(linker)))
    }

    #[inline(always)]
    pub fn get_context(&'static self, key: &str) -> Option<&'static Link> {
        self.contexts.get(key)
    }

    pub fn get_ttls(&'static self) -> &'static HashMap<String, i64> {
        &self.ttls
    }
}

#[cfg(test)]
mod contextlinker_tests {

    use super::*;

    macro_rules! new_context_linker_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let input: &str = $value.0;
                let expected: Option<ContextLinker> = $value.1;
                let fail: bool = $value.2;
                match ContextLinker::new(input) {
                    Err(_) => assert_eq!(fail, true),
                    Ok(linker) => {
                        assert_eq!(expected.expect("should not be None"), *linker);
                    },
                }
            }
        )*
        }
    }

    new_context_linker_tests! {
        invalid_json: (
            r#"{"linkers":}"#,
            None::<ContextLinker>,
            true
        ),
        one_linker_no_context_match: (
            r#"
            {
                "linkers": [
                    {
                        "name": "foo",
                        "contexts": ["bar"],
                        "rate": {
                            "count": 10,
                            "ttl_seconds": 60
                        }
                    }
                ],
                "sweep_seconds": 30
            }
            "#,
            Some(
                ContextLinker{
                    contexts: HashMap::from([
                            ("foo".to_string(), Link{
                                contexts: Vec::default(),
                                rate: 10,
                            })
                        ],
                    ),
                    ttls: HashMap::from([("foo".to_string(), 60)]),
                }
            ),
            false
        ),
        two_linkers_one_context_ref: (
            r#"
            {
                "linkers": [
                    {
                        "name": "foo",
                        "contexts": ["bar"],
                        "rate": {
                            "count": 10,
                            "ttl_seconds": 60
                        }
                    },
                    {
                        "name": "bar",
                        "contexts": ["foobar"],
                        "rate": {
                            "count": 10,
                            "ttl_seconds": 60
                        }
                    }
                ],
                "sweep_seconds": 30
            }
            "#,
            Some(
                ContextLinker{
                    contexts: HashMap::from([
                            ("foo".to_string(), Link{
                                contexts: vec!["bar".to_string()],
                                rate: 10,
                            }),
                            ("bar".to_string(), Link{
                                contexts: Vec::default(),
                                rate: 10,
                            })
                        ],
                    ),
                    ttls: HashMap::from([
                        ("foo".to_string(), 60),
                        ("bar".to_string(), 60),
                        ]),
                }
            ),
            false
        ),
        three_linkers_with_references: (
            r#"
            {
                "linkers": [
                    {
                        "name": "foo",
                        "contexts": ["bar"],
                        "rate": {
                            "count": 10,
                            "ttl_seconds": 60
                        }
                    },
                    {
                        "name": "bar",
                        "contexts": ["foo"],
                        "rate": {
                            "count": 10,
                            "ttl_seconds": 60
                        }
                    },
                    {
                        "name": "foobar",
                        "contexts": ["foo", "bar"],
                        "rate": {
                            "count": 10,
                            "ttl_seconds": 60
                        }
                    }
                ],
                "sweep_seconds": 30
            }
            "#,
            Some(
                ContextLinker{
                    contexts: HashMap::from([
                            ("foo".to_string(), Link{
                                contexts: vec!["bar".to_string()],
                                rate: 10,
                            }),
                            ("bar".to_string(), Link{
                                contexts: vec!["foo".to_string()],
                                rate: 10,
                            }),
                            ("foobar".to_string(), Link{
                                contexts: vec!["foo".to_string(), "bar".to_string()],
                                rate: 10,
                            }),
                        ],
                    ),
                    ttls: HashMap::from([
                        ("foo".to_string(), 60),
                        ("bar".to_string(), 60),
                        ("foobar".to_string(), 60),
                        ]),
                }
            ),
            false
        ),
    }
}
