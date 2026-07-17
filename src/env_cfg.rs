use anyhow::{bail, Context, Result};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct S3Creds {
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: String, // host:port, no scheme
}

pub fn cabt_home() -> PathBuf {
    dirs_fallback_home().join(".cabt")
}

fn dirs_fallback_home() -> PathBuf {
    if let Ok(h) = std::env::var("HOME") {
        return PathBuf::from(h);
    }
    PathBuf::from("/root")
}

pub fn ensure_dirs() -> Result<()> {
    let home = cabt_home();
    for sub in ["config", "result", "fool", "lib"] {
        fs::create_dir_all(home.join(sub))?;
    }
    Ok(())
}

pub fn load_s3_creds() -> Result<S3Creds> {
    let access = std::env::var("accesskey")
        .or_else(|_| std::env::var("ACCESSKEY"))
        .ok();
    let secret = std::env::var("secretkey")
        .or_else(|_| std::env::var("SECRETKEY"))
        .ok();
    let endpoint = std::env::var("endpoint")
        .or_else(|_| std::env::var("ENDPOINT"))
        .ok();

    if let (Some(a), Some(s), Some(e)) = (access, secret, endpoint) {
        return Ok(S3Creds {
            access_key: a,
            secret_key: s,
            endpoint: strip_scheme(&e),
        });
    }

    // ~/.s3cfg
    let s3cfg = dirs_fallback_home().join(".s3cfg");
    if s3cfg.is_file() {
        let text = fs::read_to_string(&s3cfg)?;
        let mut access_key = String::new();
        let mut secret_key = String::new();
        let mut host_base = String::new();
        for line in text.lines() {
            let line = line.trim();
            if let Some(v) = line.strip_prefix("access_key") {
                access_key = v.trim().trim_start_matches('=').trim().to_string();
            } else if let Some(v) = line.strip_prefix("secret_key") {
                secret_key = v.trim().trim_start_matches('=').trim().to_string();
            } else if let Some(v) = line.strip_prefix("host_base") {
                host_base = v.trim().trim_start_matches('=').trim().to_string();
            }
        }
        if !access_key.is_empty() && !secret_key.is_empty() && !host_base.is_empty() {
            return Ok(S3Creds {
                access_key,
                secret_key,
                endpoint: strip_scheme(&host_base),
            });
        }
    }

    bail!(
        "Missing S3 credentials. Set accesskey/secretkey/endpoint env or ~/.s3cfg (access_key, secret_key, host_base)."
    );
}

fn strip_scheme(s: &str) -> String {
    s.trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_string()
}

pub fn endpoint_url(host: &str) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    }
}

/// Optional probe: HEAD/PUT not required for local mock; skip if CABT_SKIP_S3_PROBE=1
pub fn probe_s3(creds: &S3Creds) -> Result<()> {
    if std::env::var("CABT_SKIP_S3_PROBE").ok().as_deref() == Some("1") {
        return Ok(());
    }
    // lightweight TCP connect check to host:port
    let hostport = &creds.endpoint;
    let (host, port) = if let Some((h, p)) = hostport.rsplit_once(':') {
        (h, p.parse::<u16>().unwrap_or(80))
    } else {
        (hostport.as_str(), 80)
    };
    let addr = format!("{host}:{port}");
    std::net::TcpStream::connect_timeout(
        &addr.parse().with_context(|| format!("parse {addr}"))?,
        std::time::Duration::from_secs(3),
    )
    .with_context(|| format!("cannot reach S3 endpoint {addr}"))?;
    Ok(())
}
