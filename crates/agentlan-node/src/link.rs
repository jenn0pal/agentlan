//! The node's single connection to the hub: connect, handshake (pinned key),
//! hello/welcome, heartbeats, reconnect with backoff + jitter.

use std::time::Duration;

use agentlan_net::keys::{self, StaticKey};
use agentlan_proto::wire::{HubToNode, NodeToHub};
use agentlan_proto::PROTO_VERSION;
use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::NodeFile;
use crate::state::NCmd;

pub enum LinkCtl {
    /// Drop the current link and reconnect (e.g. after a roster sequence gap).
    Reconnect,
}

pub struct Welcome {
    pub heartbeat_ms: u64,
    pub lease_ms: u64,
}

async fn connect(addr: &str, key: &StaticKey, hub_key: &[u8]) -> Result<(agentlan_net::FrameReader, agentlan_net::FrameWriter, Welcome)> {
    let s = tokio::time::timeout(Duration::from_secs(3), tokio::net::TcpStream::connect(addr)).await.context("connect timed out")??;
    let (mut rd, mut wr, _) = agentlan_net::connect_initiator(s, &key.private, Some(hub_key)).await?;
    wr.send(&NodeToHub::Hello { protos: vec![PROTO_VERSION], app_ver: env!("CARGO_PKG_VERSION").into() }).await?;
    match tokio::time::timeout(Duration::from_secs(10), rd.recv::<HubToNode>()).await?? {
        Some(HubToNode::Welcome { heartbeat_ms, lease_ms, .. }) => Ok((rd, wr, Welcome { heartbeat_ms, lease_ms })),
        Some(HubToNode::Error { code, msg }) => bail!("{code}: {msg}"),
        Some(other) => bail!("unexpected first frame: {other:?}"),
        None => bail!("hub closed the connection"),
    }
}

pub async fn run(cfg: NodeFile, key: StaticKey, cmd: mpsc::UnboundedSender<NCmd>, mut ctl: mpsc::UnboundedReceiver<LinkCtl>, backoff_max_ms: u64) {
    let hub_key = match keys::from_b32(&cfg.hub_key) {
        Ok(k) => k,
        Err(e) => {
            warn!("node: bad pinned hub key: {e}");
            return;
        }
    };
    let candidates: Vec<String> = match &cfg.static_hub {
        Some(s) => vec![s.clone()],
        None => cfg.hub_addrs.clone(),
    };
    let mut backoff = 250u64;
    loop {
        let mut connected = None;
        for addr in &candidates {
            match connect(addr, &key, &hub_key).await {
                Ok(c) => {
                    connected = Some((addr.clone(), c));
                    break;
                }
                Err(e) => warn!("node: hub via {addr}: {e:#}"),
            }
        }
        if let Some((addr, (mut rd, mut wr, welcome))) = connected {
            info!("node: connected to hub at {addr}");
            backoff = 250;
            let (otx, mut orx) = mpsc::channel::<NodeToHub>(1024);
            if cmd.send(NCmd::HubUp { tx: otx.clone() }).is_err() {
                return;
            }
            let writer = tokio::spawn(async move {
                while let Some(f) = orx.recv().await {
                    if wr.send(&f).await.is_err() {
                        break;
                    }
                }
                wr.shutdown().await;
            });
            let pinger = {
                let otx = otx.clone();
                let every = Duration::from_millis(welcome.heartbeat_ms.max(50));
                tokio::spawn(async move {
                    let mut n = 0u64;
                    let mut iv = tokio::time::interval(every);
                    loop {
                        iv.tick().await;
                        n += 1;
                        if otx.send(NodeToHub::Ping { n }).await.is_err() {
                            break;
                        }
                    }
                })
            };
            let lease = Duration::from_millis(welcome.lease_ms.max(100));
            loop {
                tokio::select! {
                    c = ctl.recv() => match c {
                        Some(LinkCtl::Reconnect) => { info!("node: resynchronising link"); break; }
                        None => return,
                    },
                    r = tokio::time::timeout(lease, rd.recv::<HubToNode>()) => match r {
                        Err(_) => { warn!("node: hub silent for {lease:?}; reconnecting"); break; }
                        Ok(Ok(Some(HubToNode::Pong { .. }))) => {}
                        Ok(Ok(Some(HubToNode::Ping { n }))) => { let _ = otx.try_send(NodeToHub::Pong { n }); }
                        Ok(Ok(Some(f))) => { if cmd.send(NCmd::HubFrame(f)).is_err() { return; } }
                        Ok(Ok(None)) => { warn!("node: hub closed the link"); break; }
                        Ok(Err(e)) => { warn!("node: hub link error: {e:#}"); break; }
                    }
                }
            }
            pinger.abort();
            writer.abort();
            if cmd.send(NCmd::HubDown).is_err() {
                return;
            }
        }
        // Drain stale reconnect requests, then back off with jitter.
        while ctl.try_recv().is_ok() {}
        let jitter = rand_jitter(backoff / 4 + 1);
        tokio::time::sleep(Duration::from_millis(backoff + jitter)).await;
        backoff = (backoff * 2).min(backoff_max_ms.max(250));
    }
}

fn rand_jitter(max: u64) -> u64 {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos() as u64).unwrap_or(0);
    nanos % max
}
