//! Prefixed, time-sortable identifiers.

use ulid::Ulid;

pub fn agent_id() -> String {
    format!("agt_{}", Ulid::generate())
}
pub fn msg_id() -> String {
    format!("msg_{}", Ulid::generate())
}
pub fn thread_id() -> String {
    format!("thr_{}", Ulid::generate())
}
pub fn session_id() -> String {
    format!("ses_{}", Ulid::generate())
}
pub fn hub_id() -> String {
    format!("hub_{}", Ulid::generate())
}

/// Lowercase, kebab-case normalisation for names, roles and aliases.
pub fn normalize_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut dash = false;
    for c in s.trim().chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    out
}

/// `name@alias`
pub fn handle(name: &str, alias: &str) -> String {
    format!("{name}@{alias}")
}

/// Split a handle into (name, alias). Accepts an optional leading '@'.
pub fn split_handle(h: &str) -> Option<(&str, &str)> {
    let h = h.strip_prefix('@').unwrap_or(h);
    let (n, a) = h.split_once('@')?;
    if n.is_empty() || a.is_empty() {
        return None;
    }
    Some((n, a))
}

/// Normalise a git remote URL into a project id, e.g.
/// `git@github.com:acme/shop-api.git` → `github.com/acme/shop-api`.
pub fn project_from_remote(url: &str) -> String {
    let mut u = url.trim().to_string();
    for p in ["https://", "http://", "ssh://", "git://"] {
        if let Some(rest) = u.strip_prefix(p) {
            u = rest.to_string();
        }
    }
    if let Some((user_host, path)) = u.split_once(':') {
        // scp-like syntax: git@host:path (but not host:port/path)
        if !path.starts_with("//") && !path.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            u = format!("{user_host}/{path}");
        }
    }
    if let Some((_, rest)) = u.split_once('@') {
        u = rest.to_string();
    }
    let u = u.trim_end_matches('/').trim_end_matches(".git");
    u.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(normalize_name(" Back End_API "), "back-end-api");
        assert_eq!(split_handle("@qa@studio-mbp"), Some(("qa", "studio-mbp")));
        assert_eq!(split_handle("qa"), None);
    }

    #[test]
    fn remotes() {
        assert_eq!(project_from_remote("git@github.com:acme/shop-api.git"), "github.com/acme/shop-api");
        assert_eq!(project_from_remote("https://github.com/Acme/Shop-API"), "github.com/acme/shop-api");
        assert_eq!(project_from_remote("ssh://git@host.example:2222/x/y.git"), "host.example:2222/x/y");
    }
}
