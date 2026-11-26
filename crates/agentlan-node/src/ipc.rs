//! Local IPC server: JSON lines over a Unix socket / named pipe.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use agentlan_proto::ipc::{IpcError, IpcOut, IpcRequest, IpcResponse};
use anyhow::{Result, bail};
use interprocess::local_socket::tokio::{Listener, Stream, prelude::*};
use interprocess::local_socket::ListenerOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::state::NCmd;

const MAX_LINE: usize = 2 << 20;
static CONN: AtomicU64 = AtomicU64::new(1);

pub fn listen(home: &Path) -> Result<Listener> {
    #[cfg(not(windows))]
    {
        let path = crate::node_dir(home).join("node.sock");
        if path.exists() {
            // A live node answers; a stale socket file from a crash does not.
            let alive = std::os::unix::net::UnixStream::connect(&path).is_ok();
            if alive {
                bail!("another agentlan node is already running for {}", home.display());
            }
            std::fs::remove_file(&path)?;
        }
    }
    let name = crate::ipc_name(home)?;
    match ListenerOptions::new().name(name).create_tokio() {
        Ok(l) => Ok(l),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => bail!("another agentlan node is already running for {}", home.display()),
        Err(e) => Err(e.into()),
    }
}

pub async fn serve(listener: Listener, cmd: mpsc::UnboundedSender<NCmd>) -> Result<()> {
    loop {
        let conn = listener.accept().await?;
        let cmd = cmd.clone();
        tokio::spawn(async move { handle(conn, cmd).await });
    }
}

async fn handle(conn: Stream, cmd: mpsc::UnboundedSender<NCmd>) {
    let id = CONN.fetch_add(1, Ordering::Relaxed);
    let conn = Arc::new(conn);
    let (tx, mut rx) = mpsc::unbounded_channel::<IpcOut>();
    if cmd.send(NCmd::IpcOpen { conn: id, tx: tx.clone() }).is_err() {
        return;
    }
    let wconn = conn.clone();
    let writer = tokio::spawn(async move {
        let mut w = &*wconn;
        while let Some(out) = rx.recv().await {
            let mut line = match serde_json::to_vec(&out) {
                Ok(l) => l,
                Err(_) => continue,
            };
            line.push(b'\n');
            if w.write_all(&line).await.is_err() {
                break;
            }
        }
    });
    let mut rd = BufReader::new(&*conn);
    let mut line = String::new();
    loop {
        line.clear();
        match rd.read_line(&mut line).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if line.len() > MAX_LINE {
            let _ = tx.send(err(0, "TOO_LARGE", "request line too large"));
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<IpcRequest>(trimmed) {
            Ok(req) => {
                if cmd.send(NCmd::Ipc { conn: id, req }).is_err() {
                    break;
                }
            }
            Err(e) => {
                let rid = serde_json::from_str::<serde_json::Value>(trimmed).ok().and_then(|v| v.get("id").and_then(|i| i.as_u64())).unwrap_or(0);
                let _ = tx.send(err(rid, "BAD_REQUEST", &e.to_string()));
            }
        }
    }
    let _ = cmd.send(NCmd::IpcClose { conn: id });
    drop(tx);
    let _ = writer.await;
}

fn err(id: u64, code: &str, msg: &str) -> IpcOut {
    IpcOut::Response(IpcResponse { id, ok: false, result: None, error: Some(IpcError { code: code.into(), msg: msg.into() }) })
}
