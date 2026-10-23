use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    databases: BTreeMap<String, Entry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    url: Option<String>,
    url_env: Option<String>,
    description: Option<String>,
}

pub struct DbSpec {
    pub name: String,
    pub url: String,
    pub description: Option<String>,
}

pub fn load(path: &Path) -> Result<Vec<DbSpec>> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("failed to read config {path:?}"))?;
    parse(&raw, |k| std::env::var(k).ok()).with_context(|| format!("invalid config {path:?}"))
}

fn parse(raw: &str, env: impl Fn(&str) -> Option<String>) -> Result<Vec<DbSpec>> {
    let cfg: Config = serde_json::from_str(raw)?;
    if cfg.databases.is_empty() {
        bail!("no databases configured");
    }
    cfg.databases
        .into_iter()
        .map(|(name, e)| {
            if name.trim().is_empty() {
                bail!("database name must not be empty");
            }
            let url = match (e.url, e.url_env) {
                (Some(url), None) => {
                    expand(&url, &env).with_context(|| format!("database {name:?}"))?
                }
                (None, Some(var)) => env(&var)
                    .with_context(|| format!("database {name:?}: env var {var} is not set"))?,
                _ => bail!("database {name:?}: exactly one of `url` or `url_env` is required"),
            };
            Ok(DbSpec {
                name,
                url,
                description: e.description,
            })
        })
        .collect()
}

/// Substitutes `${VAR}`; a bare `$` is kept as is, since it may be part of a password.
fn expand(s: &str, env: &impl Fn(&str) -> Option<String>) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find('}').context("unterminated ${ in url")?;
        let var = &after[..end];
        out.push_str(&env(var).with_context(|| format!("env var {var} is not set"))?);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(k: &str) -> Option<String> {
        match k {
            "PASS" => Some("s3cr$t".into()),
            "FULL" => Some("mysql://u@h/db".into()),
            _ => None,
        }
    }

    #[test]
    fn parses_url_and_url_env() {
        let dbs = parse(
            r#"{"databases": {
                "billing": {"url": "postgres://ro:${PASS}@h/b", "description": "payments"},
                "auth": {"url_env": "FULL"}
            }}"#,
            env,
        )
        .unwrap();
        assert_eq!(dbs.len(), 2);
        assert_eq!(dbs[0].name, "auth");
        assert_eq!(dbs[0].url, "mysql://u@h/db");
        assert_eq!(dbs[0].description, None);
        assert_eq!(dbs[1].url, "postgres://ro:s3cr$t@h/b");
        assert_eq!(dbs[1].description.as_deref(), Some("payments"));
    }

    #[test]
    fn rejects_invalid() {
        for raw in [
            r#"{"databases": {}}"#,
            r#"{"databases": {"a": {}}}"#,
            r#"{"databases": {"a": {"url": "x", "url_env": "FULL"}}}"#,
            r#"{"databases": {"a": {"url_env": "MISSING"}}}"#,
            r#"{"databases": {"a": {"url": "p://${MISSING}@h"}}}"#,
            r#"{"databases": {"a": {"url": "p://${PASS@h"}}}"#,
            r#"{"databases": {"": {"url": "x"}}}"#,
            r#"{"databases": {"a": {"url": "x", "typo": 1}}}"#,
        ] {
            assert!(parse(raw, env).is_err(), "expected error for {raw}");
        }
    }
}
