//! Authentication providers. Keystone v3 password auth (PR #428 / issue #409).

use crate::config::AuthConfig;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Clone)]
pub struct AuthResult {
    /// X-Auth-Token / X-Subject-Token
    pub token: String,
    /// Optional public storage URL from the service catalog (object-store / swift)
    pub storage_url: Option<String>,
    pub project_id: Option<String>,
}

pub async fn authenticate(cfg: &AuthConfig) -> anyhow::Result<Option<AuthResult>> {
    match cfg {
        AuthConfig::None => Ok(None),
        AuthConfig::KeystoneV3 {
            url,
            username,
            password,
            project_name,
            user_domain_name,
            project_domain_name,
            user_domain_id,
            project_domain_id,
            timeout_ms,
        } => {
            let r = keystone_v3_password(
                url,
                username,
                password,
                project_name,
                user_domain_name,
                project_domain_name,
                user_domain_id.as_deref(),
                project_domain_id.as_deref(),
                Duration::from_millis(*timeout_ms),
            )
            .await?;
            Ok(Some(r))
        }
    }
}

async fn keystone_v3_password(
    base_url: &str,
    username: &str,
    password: &str,
    project_name: &str,
    user_domain_name: &str,
    project_domain_name: &str,
    user_domain_id: Option<&str>,
    project_domain_id: Option<&str>,
    timeout: Duration,
) -> anyhow::Result<AuthResult> {
    let url = format!("{}/auth/tokens", base_url.trim_end_matches('/'));

    let user_domain = if let Some(id) = user_domain_id {
        json!({ "id": id })
    } else {
        json!({ "name": user_domain_name })
    };
    let project_domain = if let Some(id) = project_domain_id {
        json!({ "id": id })
    } else {
        json!({ "name": project_domain_name })
    };

    let body = json!({
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": username,
                        "domain": user_domain,
                        "password": password
                    }
                }
            },
            "scope": {
                "project": {
                    "name": project_name,
                    "domain": project_domain
                }
            }
        }
    });

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()?;

    let resp = client.post(&url).json(&body).send().await?;
    let status = resp.status();
    let token = resp
        .headers()
        .get("x-subject-token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("keystone v3 auth failed HTTP {status}: {text}");
    }
    let token = token.ok_or_else(|| anyhow::anyhow!("keystone response missing X-Subject-Token"))?;

    #[derive(Deserialize)]
    struct TokenResp {
        token: TokenBody,
    }
    #[derive(Deserialize)]
    struct TokenBody {
        #[serde(default)]
        catalog: Vec<CatalogEntry>,
        project: Option<Project>,
    }
    #[derive(Deserialize)]
    struct Project {
        id: Option<String>,
    }
    #[derive(Deserialize)]
    struct CatalogEntry {
        #[serde(rename = "type")]
        type_: Option<String>,
        endpoints: Option<Vec<Endpoint>>,
    }
    #[derive(Deserialize)]
    struct Endpoint {
        interface: Option<String>,
        url: Option<String>,
    }

    let mut storage_url = None;
    let mut project_id = None;
    if let Ok(parsed) = serde_json::from_str::<TokenResp>(&text) {
        project_id = parsed.token.project.and_then(|p| p.id);
        for ent in parsed.token.catalog {
            let t = ent.type_.unwrap_or_default();
            if t == "object-store" || t == "object_store" {
                if let Some(eps) = ent.endpoints {
                    // prefer public
                    storage_url = eps
                        .iter()
                        .find(|e| e.interface.as_deref() == Some("public"))
                        .and_then(|e| e.url.clone())
                        .or_else(|| eps.into_iter().find_map(|e| e.url));
                }
            }
        }
    }

    info!(
        storage_url = ?storage_url,
        project_id = ?project_id,
        "keystone v3 auth ok"
    );

    Ok(AuthResult {
        token,
        storage_url,
        project_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_config_none() {
        // compile-time / type smoke
        let _ = AuthConfig::None;
    }
}
