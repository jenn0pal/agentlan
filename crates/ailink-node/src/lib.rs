//! `ailink node`: the per-PC daemon. Holds the machine key and the single hub
//! link, routes same-machine messages locally (no LAN, no hub), keeps durable
//! inboxes/outbox, and serves local clients over IPC.

mod ipc;
mod link;
mod state;

use std::path::{Path, PathBuf};
use std::time::Duration;

use ailink_net::keys::{self, StaticKey};
use ailink_proto::join::JoinCode;
use ailink_proto::records::{AgentMode, AgentProfile, MachineReported};
use ailink_proto::wire::{HubToNode, NodeToHub};
use ailink_proto::PROTO_VERSION;
use anyhow::{Context, Result, bail};
use interprocess::local_socket::Name;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct NodeFile {
    pub net: String,
    pub machine_id: String,
    pub alias: String,
    /// Pinned hub static public key (base32).
    pub hub_key: String,
    /// Candidate hub addresses (from the join code, plus last-known).
    pub hub_addrs: Vec<String>,
    /// Hard override: when set, only this address is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub static_hub: Option<String>,
    #[serde(default = "yes")]
    pub share_hardware: bool,
}

fn yes() -> bool {
    true
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProfilesFile {
    pub agents: Vec<AgentProfile>,
}

pub fn node_dir(home: &Path) -> PathBuf {
    home.join("node")
}

fn config_path(home: &Path) -> PathBuf {
    node_dir(home).join("node.json")
}

fn profiles_path(home: &Path) -> PathBuf {
    node_dir(home).join("profiles.json")
}

pub fn load_config(home: &Path) -> Result<NodeFile> {
    let p = config_path(home);
    let s = std::fs::read_to_string(&p).with_context(|| format!("this PC has not joined a network ({}); run `ailink join <code>`", p.display()))?;
    Ok(serde_json::from_str(&s)?)
}

pub fn load_profiles(home: &Path) -> Result<ProfilesFile> {
    let p = profiles_path(home);
    if !p.exists() {
        return Ok(ProfilesFile::default());
    }
    Ok(serde_json::from_str(&std::fs::read_to_string(&p)?)?)
}

fn ensure_private_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Register a new agent profile on this PC (works offline). The permanent id is
/// minted here and recorded by the hub on the next resync or attach.
pub fn add_agent(home: &Path, name: &str, role: &str, project: Option<String>, runner: &str, mode: AgentMode) -> Result<AgentProfile> {
    let name = ailink_proto::ids::normalize_name(name);
    let role = ailink_proto::ids::normalize_name(role);
    if name.is_empty() || role.is_empty() {
        bail!("name and role must contain letters or digits");
    }
    ensure_private_dir(&node_dir(home))?;
    let mut pf = load_profiles(home)?;
    if pf.agents.iter().any(|a| a.name == name) {
        bail!("an agent named '{name}' already exists on this PC");
    }
    let prof = AgentProfile {
        agent_id: ailink_proto::ids::agent_id(),
        name,
        role,
        project,
        runner: runner.to_string(),
        mode,
        capabilities: vec![],
        description: None,
    };
    pf.agents.push(prof.clone());
    std::fs::write(profiles_path(home), serde_json::to_string_pretty(&pf)?)?;
    Ok(prof)
}

/// Local IPC endpoint for this home: a Unix socket file, or a named pipe on Windows.
pub fn ipc_name(home: &Path) -> std::io::Result<Name<'static>> {
    #[cfg(windows)]
    {
        use interprocess::local_socket::{GenericNamespaced, ToNsName};
        let abs = std::fs::canonicalize(home).unwrap_or_else(|_| home.to_path_buf());
        let mut h: u64 = 0xcbf29ce484222325;
        for b in abs.to_string_lossy().as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        format!("ailink-node-{h:016x}").to_ns_name::<GenericNamespaced>().map(|n| n.into_owned())
    }
    #[cfg(not(windows))]
    {
        use interprocess::local_socket::{GenericFilePath, ToFsName};
        node_dir(home).join("node.sock").to_fs_name::<GenericFilePath>().map(|n| n.into_owned())
    }
}

/// `ailink join <code>`: create this PC's key, enrol with the hub and record it.
pub async fn join(home: &Path, code: &JoinCode, alias: &str) -> Result<(String, String)> {
    if config_path(home).exists() {
        bail!("this PC already joined a network ({}); remove it first to re-join", config_path(home).display());
    }
    let dir = node_dir(home);
    ensure_private_dir(&dir)?;
    let key = StaticKey::load_or_create(&dir.join("node.key"))?;
    let hub_key = keys::from_b32(&code.hub_key)?;
    let mut last_err = None;
    for addr in &code.addrs {
        match try_join(addr, &key, &hub_key, &code.token, alias).await {
            Ok((mid, alias)) => {
                let nf = NodeFile {
                    net: code.net.clone(),
                    machine_id: mid.clone(),
                    alias: alias.clone(),
                    hub_key: code.hub_key.clone(),
                    hub_addrs: code.addrs.clone(),
                    static_hub: None,
                    share_hardware: true,
                };
                keys::write_private(&config_path(home), serde_json::to_string_pretty(&nf)?.as_bytes())?;
                return Ok((mid, alias));
            }
            Err(e) => last_err = Some(e.context(format!("via {addr}"))),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("join code lists no addresses")))
}

async fn try_join(addr: &str, key: &StaticKey, hub_key: &[u8], token: &str, alias: &str) -> Result<(String, String)> {
    let s = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr)).await.context("connect timed out")??;
    let (mut rd, mut wr, _) = ailink_net::connect_initiator(s, &key.private, Some(hub_key)).await?;
    wr.send(&NodeToHub::Hello { protos: vec![PROTO_VERSION], app_ver: env!("CARGO_PKG_VERSION").into() }).await?;
    wr.send(&NodeToHub::Enroll { token: token.to_string(), alias: alias.to_string() }).await?;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), rd.recv::<HubToNode>()).await?? {
            Some(HubToNode::Enrolled { machine_id, alias }) => return Ok((machine_id, alias)),
            // Already enrolled (same key): the hub answers Welcome directly.
            Some(HubToNode::Welcome { machine_id, alias, .. }) => return Ok((machine_id, alias)),
            Some(HubToNode::Error { code, msg }) => bail!("{code}: {msg}"),
            Some(_) => continue,
            None => bail!("hub closed the connection"),
        }
    }
}

/// Collect this machine's reported metadata.
pub fn machine_info(share_hardware: bool) -> MachineReported {
    use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
    let host = System::host_name().unwrap_or_else(|_| "unknown".into());
    let hostname = host.split('.').next().unwrap_or(&host).to_string();
    let mut r = MachineReported {
        hostname,
        os: std::env::consts::OS.to_string(),
        os_version: System::long_os_version().unwrap_or_default(),
        arch: System::cpu_arch(),
        cpu: None,
        cpu_cores: None,
        ram_gb: None,
        addrs: if_addrs::get_if_addrs()
            .map(|v| v.into_iter().filter(|i| !i.is_loopback() && i.ip().is_ipv4()).map(|i| i.ip().to_string()).collect())
            .unwrap_or_default(),
        ailink_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    if share_hardware {
        if let Ok(sys) = System::new_with_specifics(
            RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()).with_memory(MemoryRefreshKind::everything()),
        ) {
            r.cpu = sys.cpus().first().map(|c| c.brand().trim().to_string()).filter(|s| !s.is_empty());
            r.cpu_cores = Some(sys.cpus().len() as u32);
            r.ram_gb = Some(((sys.total_memory() as f64) / (1u64 << 30) as f64).round() as u32);
        }
    }
    r
}

fn lock_file(path: &Path) -> Result<std::fs::File> {
    let f = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(path)?;
    f.try_lock().map_err(|e| anyhow::anyhow!("{} is locked: {e}", path.display()))?;
    Ok(f)
}

fn env_ms(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Run the node daemon until stopped.
pub async fn run(home: &Path) -> Result<()> {
    let cfg = load_config(home)?;
    let dir = node_dir(home);
    ensure_private_dir(&dir)?;
    let key = StaticKey::load(&dir.join("node.key"))?;
    // One node per home: hold an OS lock for the life of the process (before touching the socket).
    let _lock = lock_file(&dir.join("node.lock")).context("another `ailink node run` is already using this directory")?;
    let db = ailink_store::open(&dir.join("node.db"))?;
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<state::NCmd>();
    let (ctl_tx, ctl_rx) = mpsc::unbounded_channel::<link::LinkCtl>();
    let reported = tokio::task::spawn_blocking({
        let share = cfg.share_hardware;
        move || machine_info(share)
    })
    .await?;
    let st = state::NodeState::new(home.to_path_buf(), db, cfg.clone(), reported, ctl_tx)?;
    std::thread::Builder::new().name("ailink-node-state".into()).spawn(move || st.run(cmd_rx))?;

    // Local IPC first: same-machine messaging works even if the hub never answers.
    let listener = ipc::listen(home)?;
    println!("ailink node '{}' ready (machine {}); IPC up", cfg.alias, cfg.machine_id);
    let ticker = cmd_tx.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_millis(100));
        loop {
            iv.tick().await;
            if ticker.send(state::NCmd::Tick).is_err() {
                break;
            }
        }
    });
    tokio::spawn(link::run(cfg, key, cmd_tx.clone(), ctl_rx, env_ms("AILINK_BACKOFF_MAX_MS", 30_000)));
    ipc::serve(listener, cmd_tx).await
}
