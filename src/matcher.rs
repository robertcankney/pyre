use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::{collections::HashMap, hash::Hash};


#[derive(Debug, PartialEq)]
pub struct ContextLinker {
    contexts: HashMap<String, Link>,
    ttls: HashMap<String, i64>,
}

#[derive(Debug, PartialEq)]
pub struct Link {
    contexts: Vec<String>,
    rate: i64,
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
    
    fn new(val: &str) -> std::io::Result<ContextLinker> {
        let cfg: ContextLinkerConfig = serde_json::from_str(val)?;

        let mut linker = ContextLinker{
            contexts: Default::default(),
            ttls: Default::default(),
        };

        for link in cfg.linkers {
            linker.contexts.insert(link.name.clone(), Link{
                 contexts: link.contexts, 
                 rate: link.rate.count,
            });
            linker.ttls.insert(link.name, link.rate.ttl_seconds);
        }

        Ok(linker)
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
                        assert_eq!(expected.expect("should not be None"), linker);
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
        one_linker: (
            r#"
            {
                "linkers": [
                    {
                        "name": "foo",
                        "contexts": [],
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
    }

}