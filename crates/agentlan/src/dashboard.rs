//! `agentlan dashboard`: a local web page over the same data as `agentlan log` — who's
//! online, a live timeline, and conversations — plus the server's network view on the
//! server PC.
//!
//! Only reachable from this PC: it listens on 127.0.0.1, every data request needs the
//! random key from the printed link (kept in the URL fragment, so the browser never sends
//! it anywhere by itself), the Host header must be this address (no DNS rebinding), and a
//! strict Content-Security-Policy applies. The page inserts peer-written text as text,
//! never as HTML. GET only; small requests; short timeouts.

use std::path::{Path, PathBuf};
use std::time::Duration;

use agentlan_node::activity::{self, LogFilter};
use agentlan_proto::ipc::IpcOp;
use anyhow::Result;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const INDEX: &str = include_str!("dashboard/index.html");
const APP_JS: &str = include_str!("dashboard/app.js");
const STYLE: &str = include_str!("dashboard/style.css");
const MAX_REQUEST: usize = 16 * 1024;
const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

pub async fn run(home: &Path, port: u16) -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let port = listener.local_addr()?.port();
    let mut key = [0u8; 16];
    getrandom::fill(&mut key).map_err(|e| anyhow::anyhow!("no randomness for the access key: {e}"))?;
    let key: String = key.iter().map(|b| format!("{b:02x}")).collect();
    println!("AgentLAN dashboard (this PC only): http://127.0.0.1:{port}/#{key}");
    println!("Open that link in a browser; keep this running (Ctrl-C stops it).");
    let home: PathBuf = home.to_path_buf();
    loop {
        let (sock, _) = listener.accept().await?;
        let (home, key) = (home.clone(), key.clone());
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(15), handle(sock, &home, &key, port)).await;
        });
    }
}

struct Request {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    host: String,
    auth: String,
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                match u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or(""), 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse(raw: &[u8]) -> Option<Request> {
    let text = std::str::from_utf8(raw).ok()?;
    let mut lines = text.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let (method, target, version) = (first.next()?, first.next()?, first.next()?);
    if !version.starts_with("HTTP/1.") {
        return None;
    }
    let (path, q) = target.split_once('?').unwrap_or((target, ""));
    let query = q.split('&').filter(|p| !p.is_empty()).map(|p| p.split_once('=').unwrap_or((p, ""))).map(|(k, v)| (pct_decode(k), pct_decode(v))).collect();
    let (mut host, mut auth) = (String::new(), String::new());
    for l in lines.take_while(|l| !l.is_empty()) {
        let Some((k, v)) = l.split_once(':') else { continue };
        match k.trim().to_ascii_lowercase().as_str() {
            "host" => host = v.trim().to_ascii_lowercase(),
            "authorization" => auth = v.trim().to_string(),
            _ => {}
        }
    }
    Some(Request { method: method.to_string(), path: path.to_string(), query, host, auth })
}

/// Constant-time comparison of the access key.
fn key_ok(auth: &str, key: &str) -> bool {
    let Some(given) = auth.strip_prefix("Bearer ") else { return false };
    let (a, b) = (given.as_bytes(), key.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn respond(sock: &mut TcpStream, code: &str, ctype: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {code}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n\
         Referrer-Policy: no-referrer\r\nX-Frame-Options: DENY\r\nContent-Security-Policy: {CSP}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = sock.write_all(head.as_bytes()).await;
    let _ = sock.write_all(body).await;
    let _ = sock.shutdown().await;
}

async fn json_reply(sock: &mut TcpStream, code: &str, v: &Value) {
    respond(sock, code, "application/json; charset=utf-8", v.to_string().as_bytes()).await;
}

async fn handle(mut sock: TcpStream, home: &Path, key: &str, port: u16) {
    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 2048];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        if buf.len() > MAX_REQUEST {
            return respond(&mut sock, "431 Request Header Fields Too Large", "text/plain", b"too large").await;
        }
        match sock.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let Some(req) = parse(&buf) else {
        return respond(&mut sock, "400 Bad Request", "text/plain", b"bad request").await;
    };
    // Only this address (a page on another site can't reach us through DNS rebinding).
    if req.host != format!("127.0.0.1:{port}") && req.host != format!("localhost:{port}") {
        return respond(&mut sock, "403 Forbidden", "text/plain", b"wrong host").await;
    }
    if req.method != "GET" {
        return respond(&mut sock, "405 Method Not Allowed", "text/plain", b"GET only").await;
    }
    match req.path.as_str() {
        "/" => return respond(&mut sock, "200 OK", "text/html; charset=utf-8", INDEX.as_bytes()).await,
        "/app.js" => return respond(&mut sock, "200 OK", "text/javascript; charset=utf-8", APP_JS.as_bytes()).await,
        "/style.css" => return respond(&mut sock, "200 OK", "text/css; charset=utf-8", STYLE.as_bytes()).await,
        p if p.starts_with("/api/") => {}
        _ => return respond(&mut sock, "404 Not Found", "text/plain", b"not found").await,
    }
    if !key_ok(&req.auth, key) {
        return json_reply(&mut sock, "401 Unauthorized", &json!({"error": "open the link printed by `agentlan dashboard`"})).await;
    }
    let q = |k: &str| req.query.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()).filter(|v| !v.is_empty());
    let out = match req.path.as_str() {
        "/api/status" => {
            let st = crate::call(home, IpcOp::Status).await.map_err(|e| format!("{e:#}"));
            Ok(json!({
                "node": st.as_ref().ok(), "nodeError": st.as_ref().err(),
                "network": home.join("hub").join("hub.db").exists(),
                "version": env!("CARGO_PKG_VERSION"),
            }))
        }
        "/api/agents" => crate::call(home, IpcOp::ListAgents { project: None, role: None }).await.map_err(|e| format!("{e:#}")),
        "/api/activity" => {
            let f = LogFilter {
                after_seq: q("after").and_then(|v| v.parse().ok()).unwrap_or(0),
                since: None,
                agent: q("agent"),
                thread: q("thread"),
                limit: q("limit").and_then(|v| v.parse().ok()).unwrap_or(300).min(2000),
                with_text: q("body").as_deref() == Some("1"),
            };
            let network = q("network").as_deref() == Some("1");
            let home = home.to_path_buf();
            tokio::task::spawn_blocking(move || if network { activity::read_network(&home, &f) } else { activity::read_local(&home, &f) })
                .await
                .map_err(|e| e.to_string())
                .and_then(|r| r.map_err(|e| format!("{e:#}")))
                .map(|page| json!(page))
        }
        _ => return json_reply(&mut sock, "404 Not Found", &json!({"error": "no such endpoint"})).await,
    };
    match out {
        Ok(v) => json_reply(&mut sock, "200 OK", &v).await,
        Err(e) => json_reply(&mut sock, "503 Service Unavailable", &json!({"error": e})).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_and_keys() {
        let r = parse(b"GET /api/activity?after=5&agent=co%40pc+x HTTP/1.1\r\nHost: 127.0.0.1:8080\r\nAuthorization: Bearer abc\r\n\r\n").unwrap();
        assert_eq!((r.method.as_str(), r.path.as_str(), r.host.as_str()), ("GET", "/api/activity", "127.0.0.1:8080"));
        assert_eq!(r.query, vec![("after".to_string(), "5".to_string()), ("agent".to_string(), "co@pc x".to_string())]);
        assert!(key_ok(&r.auth, "abc") && !key_ok(&r.auth, "abd") && !key_ok("abc", "abc") && !key_ok("Bearer ab", "abc"));
        assert!(parse(b"GARBAGE\r\n\r\n").is_none());
        assert_eq!(pct_decode("100%"), "100%");
        assert_eq!(pct_decode("%zz"), "%zz");
    }
}
