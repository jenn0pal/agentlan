//! `agentlan node`: the per-PC daemon. Holds the machine key and the single hub
//! link, routes same-machine messages locally (no LAN, no hub), keeps durable
//! inboxes/outbox, and serves local clients over IPC.

mod ipc;
mod link;
pub mod netif;
mod state;
pub mod wake;

use std::path::{Path, PathBuf};
use std::time::Duration;

use agentlan_net::keys::{self, StaticKey};
use agentlan_proto::join::JoinCode;
use agentlan_proto::records::{AgentMode, AgentProfile, MachineReported};
use agentlan_proto::wire::{HubToNode, NodeToHub};
use agentlan_proto::PROTO_VERSION;
use anyhow::{Context, Result, bail};
use interprocess::local_socket::Name;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::info;

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
    let s = std::fs::read_to_string(&p).with_context(|| format!("this PC has not joined a network ({}); run `agentlan join <code>`", p.display()))?;
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

/// Read-modify-write profiles.json under an exclusive lock (the CLI and the running
/// node both write it), replacing the file atomically so pollers never see a partial write.
pub fn update_profiles<T>(home: &Path, f: impl FnOnce(&mut ProfilesFile) -> Result<T>) -> Result<T> {
    ensure_private_dir(&node_dir(home))?;
    let lock = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(node_dir(home).join("profiles.lock"))?;
    lock.lock()?;
    let mut pf = load_profiles(home)?;
    let out = f(&mut pf)?;
    let path = profiles_path(home);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(&pf)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(out)
}

/// Register a new agent profile on this PC (works offline). The permanent id is
/// minted here and recorded by the hub on the next resync or attach.
pub fn add_agent(home: &Path, name: &str, role: &str, project: Option<String>, runner: &str, mode: AgentMode) -> Result<AgentProfile> {
    let name = agentlan_proto::ids::normalize_name(name);
    let role = agentlan_proto::ids::normalize_name(role);
    if name.is_empty() || role.is_empty() {
        bail!("name and role must contain letters or digits");
    }
    update_profiles(home, |pf| {
        if pf.agents.iter().any(|a| a.name == name) {
            bail!("an agent named '{name}' already exists on this PC");
        }
        let prof = AgentProfile {
            agent_id: agentlan_proto::ids::agent_id(),
            name,
            role,
            project,
            runner: runner.to_string(),
            mode,
            capabilities: vec![],
            description: None,
        };
        pf.agents.push(prof.clone());
        Ok(prof)
    })
}

/// Wake settings for worker profiles (`agentlan wake enable`), node-local.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct WakeFile {
    #[serde(default)]
    pub workers: std::collections::BTreeMap<String, wake::WakeConfig>,
}

fn wake_path(home: &Path) -> PathBuf {
    node_dir(home).join("wake.json")
}

pub fn load_wake(home: &Path) -> Result<WakeFile> {
    let p = wake_path(home);
    if !p.exists() {
        return Ok(WakeFile::default());
    }
    Ok(serde_json::from_str(&std::fs::read_to_string(&p)?)?)
}

/// Locked read-modify-write of wake.json (the CLI and the node both write it).
pub fn update_wake<T>(home: &Path, f: impl FnOnce(&mut WakeFile) -> Result<T>) -> Result<T> {
    ensure_private_dir(&node_dir(home))?;
    let lock = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(node_dir(home).join("wake.lock"))?;
    lock.lock()?;
    let mut wf = load_wake(home)?;
    let out = f(&mut wf)?;
    let path = wake_path(home);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(&wf)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(out)
}

/// Turn wake on for a worker profile: the node starts this agent through ACP when mail
/// arrives and it isn't running. Marks the profile with the `wake` capability so
/// single-recipient requests to its role can reach it while it's offline.
pub fn enable_wake(home: &Path, name: &str, cfg: wake::WakeConfig) -> Result<AgentProfile> {
    let prof = load_profiles(home)?.agents.into_iter().find(|p| p.name == name).ok_or_else(|| anyhow::anyhow!("no agent profile '{name}' on this PC"))?;
    if prof.mode != AgentMode::Worker {
        bail!("'{name}' is not a worker profile; wake is only for profiles created with `agentlan agent add {name} --mode worker`");
    }
    let id = prof.agent_id.clone();
    update_wake(home, |wf| {
        let mut c = cfg;
        // A new revision: a wake still running for the old settings won't record into it.
        // Keep the session to resume if only limits changed.
        if let Some(old) = wf.workers.get(&id) {
            c.rev = old.rev + 1;
            if old.argv == c.argv && old.cwd == c.cwd && old.preset == c.preset {
                c.session_id = old.session_id.clone();
            }
        }
        c.name = name.to_string();
        wf.workers.insert(id.clone(), c);
        Ok(())
    })?;
    update_profiles(home, |pf| {
        let a = pf.agents.iter_mut().find(|a| a.agent_id == id).ok_or_else(|| anyhow::anyhow!("profile vanished"))?;
        if !a.capabilities.iter().any(|c| c == "wake") {
            a.capabilities.push("wake".into());
        }
        Ok(a.clone())
    })
}

pub fn disable_wake(home: &Path, name: &str) -> Result<()> {
    let prof = load_profiles(home)?.agents.into_iter().find(|p| p.name == name).ok_or_else(|| anyhow::anyhow!("no agent profile '{name}' on this PC"))?;
    let id = prof.agent_id.clone();
    update_wake(home, |wf| {
        wf.workers.remove(&id);
        Ok(())
    })?;
    update_profiles(home, |pf| {
        if let Some(a) = pf.agents.iter_mut().find(|a| a.agent_id == id) {
            a.capabilities.retain(|c| c != "wake");
        }
        Ok(())
    })
}

/// Is a node serving this home's IPC endpoint right now?
pub async fn is_running(home: &Path) -> bool {
    use interprocess::local_socket::tokio::{Stream, prelude::*};
    match ipc_name(home) {
        Ok(n) => Stream::connect(n).await.is_ok(),
        Err(_) => false,
    }
}

/// Start `agentlan node run` for this home as a background process that outlives the
/// caller (e.g. an MCP adapter whose host session ends), logging to node/node.log.
/// Returns once the node answers on IPC (another starter winning the race is fine), with
/// whether it will outlive the caller's host (false only on Windows when the host's job
/// object doesn't allow breakaway: then the node stops when the host exits).
pub async fn ensure_running(home: &Path) -> Result<bool> {
    if is_running(home).await {
        return Ok(true);
    }
    #[allow(unused_mut)]
    let mut persistent = true;
    load_config(home)?; // must have joined first
    let dir = node_dir(home);
    ensure_private_dir(&dir)?;
    let log = std::fs::OpenOptions::new().create(true).append(true).open(dir.join("node.log"))?;
    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("--home").arg(home).args(["node", "run"]);
    cmd.stdin(std::process::Stdio::null()).stdout(log.try_clone()?).stderr(log);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0); // not killed by signals sent to the host's process group
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;
        cmd.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW | CREATE_BREAKAWAY_FROM_JOB);
        if cmd.spawn().is_err() {
            // The host's job object doesn't allow breakaway: the node still runs, but
            // only as long as the host does.
            cmd.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
            cmd.spawn()?;
            persistent = false;
        }
    }
    #[cfg(not(windows))]
    cmd.spawn()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if is_running(home).await {
            return Ok(persistent);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("the agentlan node did not start (see {})", dir.join("node.log").display())
}

/// Parent process id of this process (the host that launched it).
pub fn parent_pid() -> Option<u32> {
    ancestor_pids(1).into_iter().next()
}

/// This process's ancestors, nearest first (up to `max`).
pub fn ancestor_pids(max: usize) -> Vec<u32> {
    use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
    let Ok(mut sys) = System::new() else {
        return vec![];
    };
    sys.refresh_processes_specifics(ProcessesToUpdate::All, true, ProcessRefreshKind::nothing());
    let mut out = vec![];
    let mut cur = Pid::from_u32(std::process::id());
    while out.len() < max {
        let Some(parent) = sys.process(cur).and_then(|p| p.parent()) else {
            break;
        };
        if parent.as_u32() <= 1 {
            break;
        }
        out.push(parent.as_u32());
        cur = parent;
    }
    out
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
        format!("agentlan-node-{h:016x}").to_ns_name::<GenericNamespaced>().map(|n| n.into_owned())
    }
    #[cfg(not(windows))]
    {
        use interprocess::local_socket::{GenericFilePath, ToFsName};
        node_dir(home).join("node.sock").to_fs_name::<GenericFilePath>().map(|n| n.into_owned())
    }
}

/// `agentlan join <code>`: create this PC's key, enrol with the hub and record it.
pub async fn join(home: &Path, code: &JoinCode, alias: &str) -> Result<(String, String)> {
    let dir = node_dir(home);
    ensure_private_dir(&dir)?;
    // One join at a time per home (two agent sessions may `connect` at once): check and
    // create the key + config under a lock, so the saved config matches the enrolled key.
    let lock = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("join.lock"))?;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while lock.try_lock().is_err() {
        if std::time::Instant::now() > deadline {
            bail!("another join for this PC is still in progress");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if config_path(home).exists() {
        bail!("this PC already joined a network ({}); remove it first to re-join", config_path(home).display());
    }
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
    let (mut rd, mut wr, _) = agentlan_net::connect_initiator(s, &key.private, Some(hub_key)).await?;
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
        // Only addresses peers can use: LAN and Tailscale (virtual bridges / other VPNs excluded).
        addrs: netif::local_addrs().into_iter().filter(|a| a.kind != netif::AddrKind::Other).map(|a| a.ip.to_string()).collect(),
        agentlan_version: env!("CARGO_PKG_VERSION").to_string(),
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
    let _lock = lock_file(&dir.join("node.lock")).context("another `agentlan node run` is already using this directory")?;
    let db = agentlan_store::open(&dir.join("node.db"))?;
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<state::NCmd>();
    let (ctl_tx, ctl_rx) = mpsc::unbounded_channel::<link::LinkCtl>();
    let reported = tokio::task::spawn_blocking({
        let share = cfg.share_hardware;
        move || machine_info(share)
    })
    .await?;
    let st = state::NodeState::new(home.to_path_buf(), db, cfg.clone(), reported, ctl_tx, cmd_tx.clone(), tokio::runtime::Handle::current())?;
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    std::thread::Builder::new().name("agentlan-node-state".into()).spawn(move || st.run(cmd_rx, stop_tx))?;

    // Local IPC first: same-machine messaging works even if the hub never answers.
    let listener = ipc::listen(home)?;
    println!("agentlan node '{}' ready (machine {}); IPC up", cfg.alias, cfg.machine_id);
    let ticker = cmd_tx.clone();
    let tick = tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_millis(100));
        loop {
            iv.tick().await;
            if ticker.send(state::NCmd::Tick).is_err() {
                break;
            }
        }
    });
    let link = tokio::spawn(link::run(cfg, key, cmd_tx.clone(), ctl_rx, env_ms("AGENTLAN_BACKOFF_MAX_MS", 30_000)));
    // `agentlan node stop` ends only the node: when it shares a process with the server
    // (`agentlan up`), the server keeps running.
    let r = tokio::select! {
        r = ipc::serve(listener, cmd_tx) => r,
        _ = stop_rx => {
            info!("node: stopped");
            Ok(())
        }
    };
    tick.abort();
    link.abort();
    r
}
