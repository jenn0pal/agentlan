//! `agentlan hub`: registry, presence and cross-machine routing.

mod state;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use agentlan_net::keys::{self, StaticKey};
use agentlan_proto::join::JoinCode;
use agentlan_proto::wire::{HubToNode, NodeToHub};
use agentlan_proto::{PROTO_VERSION, now};
use agentlan_store::rusqlite::params;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use state::{Cmd, Hub};

pub const DEFAULT_PORT: u16 = 47820;
static GEN: AtomicU64 = AtomicU64::new(1);

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HubFile {
    pub net: String,
    pub hub_id: String,
    pub epoch: u64,
    pub listen: String,
    pub heartbeat_ms: u64,
    pub lease_ms: u64,
}

pub fn hub_dir(home: &Path) -> PathBuf {
    home.join("hub")
}

fn file(home: &Path) -> PathBuf {
    hub_dir(home).join("hub.json")
}

pub fn load(home: &Path) -> Result<HubFile> {
    let p = file(home);
    let s = std::fs::read_to_string(&p).with_context(|| format!("no hub here ({}); run `agentlan hub init` first", p.display()))?;
    Ok(serde_json::from_str(&s)?)
}

pub struct InitInfo {
    pub fingerprint: String,
    pub listen: String,
}

/// Create the hub identity (key, config, database) on this PC.
pub fn init(home: &Path, net: &str, listen: &str) -> Result<InitInfo> {
    let dir = hub_dir(home);
    if file(home).exists() {
        bail!("a hub already exists in {}", dir.display());
    }
    std::fs::create_dir_all(&dir)?;
    let key = StaticKey::load_or_create(&dir.join("hub.key"))?;
    let hf = HubFile {
        net: agentlan_proto::ids::normalize_name(net),
        hub_id: agentlan_proto::ids::hub_id(),
        epoch: 1,
        listen: listen.to_string(),
        heartbeat_ms: 10_000,
        lease_ms: 30_000,
    };
    keys::write_private(&file(home), serde_json::to_string_pretty(&hf)?.as_bytes())?;
    let db = agentlan_store::open(&dir.join("hub.db"))?;
    agentlan_store::migrate(&db, state::SCHEMA)?;
    Ok(InitInfo { fingerprint: keys::fingerprint(&key.public), listen: hf.listen })
}

/// Create a one-time join code (stored as a hash in the hub database).
pub fn invite(home: &Path, addrs: Vec<String>, ttl: Duration) -> Result<JoinCode> {
    let hf = load(home)?;
    let dir = hub_dir(home);
    let key = StaticKey::load(&dir.join("hub.key"))?;
    let token = data_encoding::BASE32_NOPAD.encode(&rand::random::<[u8; 20]>());
    let expires = now() + chrono::Duration::from_std(ttl)?;
    let db = agentlan_store::open(&dir.join("hub.db"))?;
    agentlan_store::migrate(&db, state::SCHEMA)?;
    db.execute("INSERT INTO invites(token_hash, expires) VALUES(?1,?2)", params![state::token_hash(&token), expires.to_rfc3339()])?;
    Ok(JoinCode { net: hf.net, addrs, hub_key: keys::b32(&key.public), token, expires })
}

/// Exclusive advisory lock on `path` (released when the process exits).
pub fn lock_file(path: &Path) -> Result<std::fs::File> {
    let f = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(path)?;
    f.try_lock().map_err(|e| anyhow::anyhow!("{} is locked: {e}", path.display()))?;
    Ok(f)
}

fn env_ms(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Run the hub until the process is stopped.
pub async fn run(home: &Path, listen_override: Option<String>) -> Result<()> {
    run_with_ready(home, listen_override, None).await
}

/// Like [`run`], but reports the address actually bound (so callers such as
/// `agentlan up` don't have to guess by probing a port).
pub async fn run_with_ready(home: &Path, listen_override: Option<String>, ready: Option<oneshot::Sender<std::net::SocketAddr>>) -> Result<()> {
    let hf = load(home)?;
    let dir = hub_dir(home);
    let key = StaticKey::load(&dir.join("hub.key"))?;
    let heartbeat = env_ms("AGENTLAN_HEARTBEAT_MS", hf.heartbeat_ms);
    let lease = env_ms("AGENTLAN_LEASE_MS", hf.lease_ms);
    // One hub per home: hold an OS lock for the life of the process.
    let _lock = lock_file(&dir.join("hub.lock")).context("another `agentlan hub run` is already using this directory")?;
    // Bind before touching persisted presence, so a failed second start changes nothing.
    let addr = listen_override.unwrap_or(hf.listen.clone());
    let listener = TcpListener::bind(&addr).await.with_context(|| format!("binding {addr}"))?;
    if let Some(r) = ready {
        let _ = r.send(listener.local_addr()?);
    }
    let db = agentlan_store::open(&dir.join("hub.db"))?;
    let hub = Hub::new(db, Duration::from_millis(lease))?;
    let (tx, rx) = mpsc::unbounded_channel::<Cmd>();
    std::thread::Builder::new().name("agentlan-hub-state".into()).spawn(move || hub.run(rx))?;
    let ticker = tx.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_millis(250));
        loop {
            iv.tick().await;
            if ticker.send(Cmd::Tick).is_err() {
                break;
            }
        }
    });
    info!("hub '{}' listening on {} (key {})", hf.net, listener.local_addr()?, keys::fingerprint(&key.public));
    println!("agentlan hub listening on {}", listener.local_addr()?);
    let ctx = ConnCtx { private: key.private.clone(), cmd: tx, hub_id: hf.hub_id.clone(), epoch: hf.epoch, heartbeat, lease };
    loop {
        let (stream, peer) = listener.accept().await?;
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, ctx).await {
                warn!("hub: connection from {peer} ended: {e:#}");
            }
        });
    }
}

#[derive(Clone)]
struct ConnCtx {
    private: Vec<u8>,
    cmd: mpsc::UnboundedSender<Cmd>,
    hub_id: String,
    epoch: u64,
    heartbeat: u64,
    lease: u64,
}

async fn ask<T>(cmd: &mpsc::UnboundedSender<Cmd>, mk: impl FnOnce(oneshot::Sender<T>) -> Cmd) -> Result<T> {
    let (tx, rx) = oneshot::channel();
    cmd.send(mk(tx)).map_err(|_| anyhow::anyhow!("hub state stopped"))?;
    Ok(rx.await?)
}

async fn handle_conn(stream: TcpStream, ctx: ConnCtx) -> Result<()> {
    let (mut rd, mut wr, remote) = agentlan_net::accept_responder(stream, &ctx.private).await?;
    let first = Duration::from_secs(10);
    let hello: NodeToHub = tokio::time::timeout(first, rd.recv()).await??.context("closed before hello")?;
    match hello {
        NodeToHub::Hello { protos, .. } if protos.contains(&PROTO_VERSION) => {}
        NodeToHub::Hello { protos, .. } => {
            wr.send(&HubToNode::Error { code: "INCOMPATIBLE_VERSION".into(), msg: format!("hub speaks {PROTO_VERSION}, node speaks {protos:?}") }).await?;
            return Ok(());
        }
        _ => bail!("expected hello"),
    }
    let pubkey = keys::b32(&remote);
    let known = ask(&ctx.cmd, |reply| Cmd::Lookup { pubkey: pubkey.clone(), reply }).await?;
    let (mid, alias) = match known {
        Some(m) if m.revoked => {
            wr.send(&HubToNode::Error { code: "REVOKED".into(), msg: "this machine has been revoked".into() }).await?;
            return Ok(());
        }
        Some(m) => (m.machine_id, m.alias),
        None => match tokio::time::timeout(first, rd.recv::<NodeToHub>()).await?? {
            Some(NodeToHub::Enroll { token, alias }) => {
                match ask(&ctx.cmd, |reply| Cmd::Enroll { pubkey: pubkey.clone(), token, alias, reply }).await? {
                    Ok((mid, alias)) => {
                        wr.send(&HubToNode::Enrolled { machine_id: mid.clone(), alias: alias.clone() }).await?;
                        (mid, alias)
                    }
                    Err(e) => {
                        wr.send(&HubToNode::Error { code: "ENROLL_FAILED".into(), msg: e }).await?;
                        return Ok(());
                    }
                }
            }
            _ => {
                wr.send(&HubToNode::Error { code: "NOT_ENROLLED".into(), msg: "unknown machine key; run `agentlan join <code>`".into() }).await?;
                return Ok(());
            }
        },
    };
    let cgen = GEN.fetch_add(1, Ordering::Relaxed);
    wr.send(&HubToNode::Welcome {
        proto: PROTO_VERSION,
        hub_id: ctx.hub_id.clone(),
        machine_id: mid.clone(),
        alias,
        conn_gen: cgen,
        heartbeat_ms: ctx.heartbeat,
        lease_ms: ctx.lease,
        epoch: ctx.epoch,
    })
    .await?;

    let (otx, mut orx) = mpsc::channel::<HubToNode>(1024);
    let (kill_tx, mut kill_rx) = oneshot::channel::<()>();
    ctx.cmd.send(Cmd::Connected { machine_id: mid.clone(), cgen, tx: otx, kill: kill_tx }).ok();
    let writer = tokio::spawn(async move {
        while let Some(f) = orx.recv().await {
            if wr.send(&f).await.is_err() {
                break;
            }
        }
        wr.shutdown().await;
    });
    let lease = Duration::from_millis(ctx.lease);
    let result: Result<()> = loop {
        tokio::select! {
            _ = &mut kill_rx => break Ok(()),
            r = tokio::time::timeout(lease, rd.recv::<NodeToHub>()) => match r {
                Err(_) => break Err(anyhow::anyhow!("no traffic within lease")),
                Ok(Ok(Some(frame))) => { ctx.cmd.send(Cmd::Frame { machine_id: mid.clone(), cgen, frame }).ok(); }
                Ok(Ok(None)) => break Ok(()),
                Ok(Err(e)) => break Err(e),
            }
        }
    };
    ctx.cmd.send(Cmd::Disconnected { machine_id: mid, cgen }).ok();
    writer.abort();
    result
}
