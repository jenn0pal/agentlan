//! `agentlan wake …` (configure worker wake) and the hidden fake ACP agent used by tests.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use agentlan_node::wake::{Access, Preset, WakeConfig};
use agentlan_proto::ipc::IpcOp;
use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};

/// `canonicalize`, minus Windows' `\\?\` verbatim prefix for ordinary drive paths
/// (cmd.exe can't start a shim or use a working directory written that way).
fn canonical(p: &Path) -> std::io::Result<PathBuf> {
    let c = std::fs::canonicalize(p)?;
    let s = c.to_string_lossy();
    match s.strip_prefix(r"\\?\") {
        Some(rest) if !rest.starts_with("UNC") => Ok(PathBuf::from(rest)),
        _ => Ok(c),
    }
}

/// Find an executable on PATH (absolute path), like `which`.
fn which(name: &str) -> Option<PathBuf> {
    let exts: Vec<String> = if cfg!(windows) {
        std::env::var("PATHEXT").unwrap_or(".EXE;.CMD;.BAT".into()).split(';').map(|e| e.to_ascii_lowercase()).chain([String::new()]).collect()
    } else {
        vec![String::new()]
    };
    std::env::split_paths(&std::env::var_os("PATH")?).find_map(|d| exts.iter().map(|e| d.join(format!("{name}{e}"))).find(|p| p.is_file()))
}

/// Environment variables the node sets itself for the lockdown; a worker's `--env` can't
/// override them.
const RESERVED_ENV: [&str; 3] = ["CODEX_CONFIG", "INITIAL_AGENT_MODE", "NO_BROWSER"];

#[allow(clippy::too_many_arguments)]
pub fn enable(
    home: &Path,
    name: &str,
    agent: &str,
    command: Option<String>,
    cwd: Option<PathBuf>,
    access: &str,
    timeout_s: u64,
    env: Vec<String>,
    claude_cli: Option<PathBuf>,
) -> Result<()> {
    let (preset, default_bin, install) = match agent {
        "codex" => (Preset::Codex, "codex-acp", "npm install -g @agentclientprotocol/codex-acp@2.12.1"),
        "claude" | "claude-code" => (Preset::Claude, "claude-agent-acp", "npm install -g @agentclientprotocol/claude-agent-acp@0.112.0"),
        "custom" => (Preset::Custom, "", ""),
        a => bail!("unknown agent '{a}' (codex | claude | custom)"),
    };
    // The adapter is resolved now and pinned as a canonical absolute path: nothing is
    // downloaded or looked up (no PATH, no shell) at wake time.
    let mut argv = match command {
        Some(c) => agentlan_node::wake::split_command(&c)?,
        None if preset == Preset::Custom => bail!("--agent custom needs --command '<ACP agent command>'"),
        None => {
            let p = which(default_bin).ok_or_else(|| anyhow!("`{default_bin}` (the ACP adapter) isn't on PATH. Install it with:\n  {install}\nor pass --command '<path to an ACP agent>'"))?;
            vec![p.to_string_lossy().to_string()]
        }
    };
    let prog = PathBuf::from(&argv[0]);
    let resolved = if prog.components().count() > 1 { Some(prog.clone()) } else { which(&argv[0]) };
    let resolved = resolved.and_then(|p| canonical(&p).ok()).ok_or_else(|| anyhow!("can't find the program `{}`", argv[0]))?;
    argv[0] = resolved.to_string_lossy().to_string();
    let command = argv.join(" ");
    let access = match access {
        "messages" => Access::Messages,
        "work" => Access::Work,
        "full" => Access::Full,
        a => bail!("unknown access '{a}' (messages | work | full)"),
    };
    let cwd = match cwd {
        Some(c) => canonical(&c).map_err(|e| anyhow!("--cwd {}: {e}", c.display()))?,
        None => std::env::current_dir()?,
    };
    let mut vars = std::collections::BTreeMap::new();
    for kv in env {
        let (k, v) = kv.split_once('=').ok_or_else(|| anyhow!("--env needs KEY=VALUE (got '{kv}')"))?;
        let k = k.trim();
        if k.is_empty() || !k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') || k.starts_with(|c: char| c.is_ascii_digit()) {
            bail!("--env: '{k}' isn't a valid variable name");
        }
        if RESERVED_ENV.contains(&k) {
            bail!("--env: {k} is set by AgentLAN for the worker's lockdown and can't be overridden");
        }
        vars.insert(k.to_string(), v.to_string());
    }
    if let Some(cli) = claude_cli {
        if preset != Preset::Claude {
            bail!("--claude-cli is for --agent claude");
        }
        let p = canonical(&cli).map_err(|e| anyhow!("--claude-cli {}: {e}", cli.display()))?;
        vars.insert("CLAUDE_CODE_EXECUTABLE".into(), p.to_string_lossy().to_string());
    }
    let shown: Vec<String> = vars.keys().cloned().collect();
    let cfg = WakeConfig { name: name.to_string(), preset, argv, cwd: cwd.clone(), access, timeout_s, env: vars, rev: 1, session_id: None, last: None };
    agentlan_node::enable_wake(home, name, cfg)?;
    println!("✓ wake enabled for '{name}': {command}");
    println!("  working directory {}", cwd.display());
    match access {
        Access::Messages => println!("  access: messages only (AgentLAN tools; no shell, file edits or other MCP servers)"),
        Access::Work => println!("  access: work — reads and edits inside that directory (path-checked); Codex runs commands in its sandbox, Claude's are refused"),
        Access::Full => println!("  access: FULL — every action it asks for is approved, unattended (only the adapter's own limits apply)"),
    }
    if !shown.is_empty() {
        println!("  environment: {} (stored in this PC's private AgentLAN data dir)", shown.join(", "));
    }
    println!("  When mail arrives and '{name}' isn't running, this PC's node starts it (≤ {timeout_s}s per wake).");
    Ok(())
}

pub fn disable(home: &Path, name: &str) -> Result<()> {
    agentlan_node::disable_wake(home, name)?;
    println!("✓ wake disabled for '{name}'");
    Ok(())
}

pub async fn ls(home: &Path, json_out: bool) -> Result<()> {
    let v = match crate::call(home, IpcOp::WakeStatus).await {
        Ok(v) => v,
        Err(_) => {
            // Node not running: show the configuration only.
            let wf = agentlan_node::load_wake(home)?;
            json!({"workers": wf.workers.iter().map(|(id, c)| json!({"agentId": id, "handle": c.name, "config": c})).collect::<Vec<_>>(), "node": "not running"})
        }
    };
    if json_out {
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }
    let ws = v["workers"].as_array().cloned().unwrap_or_default();
    if ws.is_empty() {
        println!("(no workers have wake enabled; see `agentlan wake enable --help`)");
    }
    for w in ws {
        let c = &w["config"];
        println!(
            "{}  {} · access {} · timeout {}s{}{}",
            w["handle"].as_str().unwrap_or("?"),
            c["preset"].as_str().unwrap_or("?"),
            c["access"].as_str().unwrap_or("?"),
            c["timeoutS"],
            if w["running"] == true { " · WAKING NOW" } else { "" },
            if w["online"] == true { " · online" } else { "" },
        );
        let argv: Vec<&str> = c["argv"].as_array().map(|a| a.iter().filter_map(|x| x.as_str()).collect()).unwrap_or_default();
        println!("    {}  (in {})", argv.join(" "), c["cwd"].as_str().unwrap_or(""));
        if let Some(env) = c["env"].as_object().filter(|e| !e.is_empty()) {
            // Names only: values may be credentials.
            println!("    env: {}", env.keys().cloned().collect::<Vec<_>>().join(", "));
        }
        if let Some(l) = c.get("last").filter(|l| !l.is_null()) {
            println!("    last wake {}: {} ({}s)", l["at"].as_str().unwrap_or("?"), l["outcome"].as_str().unwrap_or("?"), l["seconds"]);
        }
    }
    Ok(())
}

pub async fn now(home: &Path, name: &str) -> Result<()> {
    let v = crate::call(home, IpcOp::WakeNow { agent: name.to_string() }).await?;
    println!("✓ waking {}", v["scheduled"].as_str().unwrap_or(name));
    Ok(())
}

// ---------------------------------------------------------------------- fake ACP agent

/// `agentlan dev fake-acp --log FILE`: a minimal ACP agent for tests. On a prompt it asks
/// permission to edit a file (expecting a messages-only wake to refuse), then answers
/// every unread message through the agentlan CLI named in its MCP server config.
pub fn fake_acp(log: &Path) -> Result<()> {
    let mut logf = std::fs::OpenOptions::new().create(true).append(true).open(log)?;
    let mut say = |s: String| {
        let _ = writeln!(logf, "{s}");
    };
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    let mut out = std::io::stdout();
    let mut send = |v: Value| -> Result<()> {
        writeln!(out, "{v}")?;
        out.flush()?;
        Ok(())
    };
    let mut mcp: Option<(String, Vec<String>)> = None;
    while let Some(line) = lines.next() {
        let m: Value = match serde_json::from_str(&line?) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let id = m["id"].clone();
        let p = &m["params"];
        match m["method"].as_str().unwrap_or("") {
            "initialize" => {
                say("init".into());
                send(json!({"jsonrpc": "2.0", "id": id, "result": {"protocolVersion": 1, "agentCapabilities": {"loadSession": true, "sessionCapabilities": {"resume": {}}}, "authMethods": []}}))?;
            }
            "session/new" | "session/resume" | "session/load" => {
                let srv = &p["mcpServers"][0];
                mcp = Some((srv["command"].as_str().unwrap_or("").to_string(), srv["args"].as_array().cloned().unwrap_or_default().iter().filter_map(|a| a.as_str().map(String::from)).collect()));
                let sid = p["sessionId"].as_str().map(String::from).unwrap_or_else(|| format!("fake-{}", std::process::id()));
                say(format!("{} {sid}", m["method"].as_str().unwrap_or("")));
                let res = if m["method"] == "session/new" { json!({"sessionId": sid}) } else { json!({}) };
                send(json!({"jsonrpc": "2.0", "id": id, "result": res}))?;
            }
            "session/set_mode" => send(json!({"jsonrpc": "2.0", "id": id, "result": {}}))?,
            "session/prompt" => {
                say(format!("prompt {}", p["prompt"][0]["text"].as_str().unwrap_or("")));
                if let Ok(mark) = std::env::var("AGENTLAN_FAKE_MARK") {
                    say(format!("env AGENTLAN_FAKE_MARK={mark}"));
                }
                // 1. Try something outside AgentLAN.
                send(json!({"jsonrpc": "2.0", "id": 9001, "method": "session/request_permission", "params": {
                    "sessionId": p["sessionId"], "toolCall": {"toolCallId": "t1", "title": "Edit files", "kind": "edit"},
                    "options": [{"optionId": "yes", "name": "Allow", "kind": "allow_once"}, {"optionId": "no", "name": "Reject", "kind": "reject_once"}]}}))?;
                for l in lines.by_ref() {
                    let r: Value = serde_json::from_str(&l?).unwrap_or(Value::Null);
                    if r["id"] == 9001 {
                        say(format!("permission {}", r["result"]["outcome"]["optionId"].as_str().unwrap_or("cancelled")));
                        break;
                    }
                }
                // 2. Answer the mail with the CLI the MCP config points at.
                let (cmd, args) = mcp.clone().ok_or_else(|| anyhow!("prompt before session"))?;
                let home = args.iter().position(|a| a == "--home").and_then(|i| args.get(i + 1)).cloned().unwrap_or_default();
                let me = args.iter().position(|a| a == "--agent").and_then(|i| args.get(i + 1)).cloned().unwrap_or_default();
                let run = |extra: &[&str], stdin: Option<String>| -> Result<Value> {
                    let mut c = std::process::Command::new(&cmd);
                    c.arg("--home").arg(&home).args(extra).stdout(std::process::Stdio::piped());
                    if stdin.is_some() {
                        c.stdin(std::process::Stdio::piped());
                    }
                    let mut ch = c.spawn()?;
                    if let Some(s) = stdin {
                        ch.stdin.take().unwrap().write_all(s.as_bytes())?;
                    }
                    let o = ch.wait_with_output()?;
                    Ok(serde_json::from_slice(&o.stdout).unwrap_or(Value::Null))
                };
                let inbox = run(&["inbox", "--as", &me, "--json"], None)?;
                let msgs = inbox["messages"].as_array().cloned().unwrap_or_default();
                for msg in &msgs {
                    let text = msg["parts"][0]["text"].as_str().unwrap_or("");
                    let draft = json!({"to": {"kind": "agent", "agent": msg["from"]["agent"]}, "intent": "result", "text": format!("fake reply: {text}"), "inReplyTo": msg["id"], "threadId": msg["threadId"]});
                    run(&["send", "--as", &me, "--stdin", "--json"], Some(draft.to_string()))?;
                    run(&["ack", "--as", &me, msg["id"].as_str().unwrap_or("")], None)?;
                }
                say(format!("replied {}", msgs.len()));
                if let Some(ms) = std::env::var("FAKE_ACP_SLEEP_MS").ok().and_then(|v| v.parse().ok()) {
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                }
                send(json!({"jsonrpc": "2.0", "id": id, "result": {"stopReason": "end_turn"}}))?;
            }
            _ if !id.is_null() => send(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "unsupported"}}))?,
            _ => {}
        }
    }
    say("exit".into());
    Ok(())
}
