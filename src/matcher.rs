use serde::{Deserialize, Serialize};

use std::{collections::HashMap, ops::Deref, error::Error};

#[derive(Debug, PartialEq, Eq)]
pub struct ContextLinker {
    pub contexts: HashMap<String, Link>,
    pub ttls: HashMap<String, u64>,
    pub sweep: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Link {
    pub contexts: Vec<String>,
    pub rate: u64,
}

#[derive(Serialize, Deserialize)]
struct ContextLinkerConfig {
    linkers: Vec<ContextLinkerSub>,
    sweep_seconds: u64,
}

#[derive(Serialize, Deserialize)]
struct ContextLinkerSub {
    name: String,
    contexts: Vec<String>,
    rate: RateConfig,
}

#[derive(Serialize, Deserialize)]
struct RateConfig {
    count: u64,
    ttl_seconds: u64,
}

impl ContextLinker {
    pub fn new(val: &str) -> Result<ContextLinker, Box<dyn Error>> {
        let cfg: ContextLinkerConfig = serde_yaml::from_str(val)?;

        let mut linker = ContextLinker {
            contexts: Default::default(),
            ttls: Default::default(),
            sweep: cfg.sweep_seconds,
        };

        for link in &cfg.linkers {
            linker.contexts.insert(
                link.name.clone(),
                Link {
                    contexts: link
                        .contexts
                        .iter()
                        .filter(|ctx| cfg.linkers.iter().any(|lnk| lnk.name.eq(ctx.deref())))
                        .cloned()
                        .collect(),
                    rate: link.rate.count as u64,
                },
            );
            linker.ttls.insert(link.name.clone(), link.rate.ttl_seconds);
        }

        // mixed feelings on this
        Ok(linker)
    }

    #[inline(always)]
    pub fn get_context(&self, key: &str) -> Option<&Link> {
        self.contexts.get(key)
    }

    pub fn get_ttls(&self) -> &HashMap<String, u64> {
        &self.ttls
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    macro_rules! new_context_linker_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let (input, expected, fail) = $value;
                    match ContextLinker::new(input) {
                        Err(_) => assert_eq!(fail, true),
                        Ok(linker) => {
                            assert_eq!(expected.expect("should not be None"), linker);
                        },
                    }
                }
            )*
        }
    }

    new_context_linker_tests! {
        context_linker_invalid_json: (
            r#"{"linkers":}"#,
            None::<ContextLinker>,
            true
        ),
        context_linker_one_linker_no_context_match: (
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
                    sweep: 30,
                }
            ),
            false
        ),
        context_linker_two_linkers_one_context_ref: (
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
                    sweep: 30,
                }
            ),
            false
        ),
        context_linker_three_linkers_with_references: (
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
                    sweep: 30,
                }
            ),
            false
        ),
    }
}
