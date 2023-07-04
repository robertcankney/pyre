use std::{collections::HashMap, error::Error, ops::Deref};
use derive_more::{Error, Display};

const NAME_SEPARATOR: &str = "=";
const VAL_DURATION_SEPARATOR: &str = ":";
const RATE_SEPARTOR: &str = ",";
pub const HARDCODED_TTL: u64 = 30;

#[derive(Error, Display, Debug, PartialEq)]
pub struct ConfigError{
    pub msg: String,
}

#[derive(PartialEq, Debug)]
pub struct Config {
    pub configs: HashMap<String, RateConfig>,
    pub ttl_seconds: u64,
}

#[derive(PartialEq, Debug)]
pub struct RateConfig {
    pub name: String,
    pub count: u64,
    pub window: std::time::Duration,
}

impl TryFrom<String> for Config {
    type Error = ConfigError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let rates = value
            .split(RATE_SEPARTOR)
            .map(|e| {
                let val: Result<RateConfig, ConfigError> = e.try_into();
                val
            })
            .collect::<Result<Vec<RateConfig>, ConfigError>>()?
            .into_iter()
            .map(|e| (e.name.clone(), e))
            .collect();

        Ok(Config {
            configs: rates,
            ttl_seconds: HARDCODED_TTL,
        })
    }
}

impl TryFrom<&str> for RateConfig {
    type Error =  ConfigError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut name_split = value.split(NAME_SEPARATOR).collect::<Vec<&str>>();
        let rate = name_split
        .pop()
        .ok_or(ConfigError{msg: "no rate config found".to_string()})?;
        let name = name_split
            .pop()
            .ok_or(ConfigError{msg: "no name in rate".to_string()})?
            .to_string();

        let mut rate_split = rate.split(VAL_DURATION_SEPARATOR).collect::<Vec<&str>>();
        let window_raw = rate_split
        .pop()
        .ok_or(ConfigError{msg: "no window in rate".to_string()})?;
        let window = parse_duration::parse(window_raw)
        .map_err(|e| ConfigError{msg: format!("parse window: {}", e.to_string())})?;

        let count = rate_split
            .pop()
            .ok_or(ConfigError{msg: "no count in rate".to_string()})?
            .parse::<u64>()
            .map_err(|e| ConfigError{msg: format!("parse rate count: {}", e.to_string())})?;

        Ok(RateConfig {
            name,
            count,
            window,
        })
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
                    let (input, expected) = $value;
                    assert_eq!(expected, input.to_string().try_into());
                }
            )*
        }
    }

    new_context_linker_tests! {
        valid_two_configs: (
            "foo=100:1 minute,bar=1000:30 seconds",
            Ok(Config{
                configs: HashMap::from([(
                    "foo".to_string(),
                    RateConfig{
                        name: "foo".to_string(),
                        count: 100,
                        window: std::time::Duration::from_secs(60),
                    }),
                    ("bar".to_string(),
                    RateConfig{
                        name: "bar".to_string(),
                        count: 1000,
                        window: std::time::Duration::from_secs(30),
                    })
                ]),
                ttl_seconds: HARDCODED_TTL
            })
        ),
        empty_config: (
            "",
            Err::<Config, ConfigError>(ConfigError{msg: "no name in rate".to_string()}),
        ),
        no_name_separator: (
            "100:1m",
            Err::<Config, ConfigError>(ConfigError{msg: "no name in rate".to_string()}),
        ),
        no_val_separator: (
            "foo=100",
            Err::<Config, ConfigError>(ConfigError{msg: "no count in rate".to_string()}),
        ),
        bad_duration: (
            "foo=100:50 minuten",
            Err::<Config, ConfigError>(ConfigError{msg: r#"parse window: UnknownUnitError: "minuten" is not a known unit"#.to_string()}),
        ),
    }
}
