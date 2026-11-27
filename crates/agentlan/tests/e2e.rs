#![allow(dead_code)]
//! End-to-end tests that drive the real `agentlan` binary as separate processes
//! on 127.0.0.1 (Milestone 1 exit criteria).

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_agentlan");
static N: AtomicU32 = AtomicU32::new(0);

struct Env {
    root: PathBuf,
    port: u16,
    kids: Vec<Child>,
    extra_env: Vec<(&'static str, String)>,
}

impl Env {
    fn new() -> Env {
        // Unix: short /tmp path (socket paths are limited to ~104 bytes on macOS).
        // Windows: the system temp dir (IPC uses named pipes, no path limit).
        let name = format!("agl-{}-{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst));
        let root = if cfg!(unix) { PathBuf::from("/tmp").join(name) } else { std::env::temp_dir().join(name) };
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        Env { root, port, kids: vec![], extra_env: vec![] }
    }

    fn home(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn cmd(&self, home: &str, args: &[&str]) -> Command {
        let mut c = Command::new(BIN);
        c.arg("--home").arg(self.home(home)).args(args);
        c.env("AGENTLAN_HEARTBEAT_MS", "300").env("AGENTLAN_LEASE_MS", "2000").env("AGENTLAN_BACKOFF_MAX_MS", "400").env("AGENTLAN_LOG", "warn");
        for (k, v) in &self.extra_env {
            c.env(k, v);
        }
        c
    }

    fn run(&self, home: &str, args: &[&str]) -> Output {
        let out = self.cmd(home, args).output().unwrap();
        assert!(out.status.success(), "agentlan {args:?} failed: {}", String::from_utf8_lossy(&out.stderr));
        out
    }

    fn json(&self, home: &str, args: &[&str]) -> Value {
        let out = self.run(home, args);
        serde_json::from_slice(&out.stdout).unwrap_or_else(|e| panic!("bad json from {args:?}: {e}: {}", String::from_utf8_lossy(&out.stdout)))
    }

    fn spawn(&mut self, home: &str, args: &[&str]) -> usize {
        let log = std::fs::File::create(self.root.join(format!("{home}-{}.log", self.kids.len()))).unwrap();
        let child = self.cmd(home, args).stdout(Stdio::null()).stderr(log).spawn().unwrap();
        self.kids.push(child);
        self.kids.len() - 1
    }

    /// Spawn a streaming command and return a channel of its stdout JSON lines.
    fn stream(&mut self, home: &str, args: &[&str]) -> mpsc::Receiver<Value> {
        let mut child = self.cmd(home, args).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
        let out = child.stdout.take().unwrap();
        self.kids.push(child);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(Result::ok) {
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if tx.send(v).is_err() {
                        break;
                    }
                }
            }
        });
        rx
    }

    fn kill(&mut self, idx: usize) {
        let _ = self.kids[idx].kill();
        let _ = self.kids[idx].wait();
    }

    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// hub init + run, and join each node. Returns the hub child index.
    fn network(&mut self, nodes: &[(&str, &str)]) -> usize {
        self.run("hub", &["hub", "init", "--name", "e2e", "--listen", &self.addr()]);
        let codes: Vec<String> = nodes
            .iter()
            .map(|_| String::from_utf8(self.run("hub", &["hub", "invite", "--addr", &self.addr()]).stdout).unwrap().trim().to_string())
            .collect();
        let hub = self.spawn("hub", &["hub", "run"]);
        wait_port(self.port);
        for ((home, alias), code) in nodes.iter().zip(codes) {
            self.run(home, &["join", &code, "--alias", alias]);
        }
        hub
    }

    fn start_node(&mut self, home: &str) -> usize {
        self.start_node_with(home, &[])
    }

    /// `node run` with extra environment for that process only.
    fn start_node_with(&mut self, home: &str, env: &[(&str, &str)]) -> usize {
        let log = std::fs::File::create(self.root.join(format!("{home}-{}.log", self.kids.len()))).unwrap();
        let mut cmd = self.cmd(home, &["node", "run"]);
        for (k, v) in env {
            cmd.env(k, v);
        }
        self.kids.push(cmd.stdout(Stdio::null()).stderr(log).spawn().unwrap());
        let idx = self.kids.len() - 1;
        let h = self.home(home);
        wait_until("node IPC up", Duration::from_secs(10), || self.cmd_ok(&h));
        idx
    }

    fn cmd_ok(&self, home: &Path) -> bool {
        Command::new(BIN).arg("--home").arg(home).args(["status", "--json"]).output().map(|o| o.status.success()).unwrap_or(false)
    }

    fn status(&self, home: &str) -> Value {
        self.json(home, &["status", "--json"])
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for k in &mut self.kids {
            let _ = k.kill();
            let _ = k.wait();
        }
        // Nodes started in the background by an adapter's `connect` aren't our children.
        if let Ok(rd) = std::fs::read_dir(&self.root) {
            for d in rd.flatten().filter(|d| d.path().join("node").is_dir()) {
                let _ = Command::new(BIN).arg("--home").arg(d.path()).args(["node", "stop"]).output();
            }
        }
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.root);
        } else {
            eprintln!("test data kept in {}", self.root.display());
        }
    }
}

fn wait_port(port: u16) {
    wait_until("hub port open", Duration::from_secs(10), || std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
}

fn wait_until(what: &str, max: Duration, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while t.elapsed() < max {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for {what}");
}

fn expect(rx: &mpsc::Receiver<Value>, what: &str, max: Duration, pred: impl Fn(&Value) -> bool) -> Value {
    let t = Instant::now();
    while let Some(left) = max.checked_sub(t.elapsed()) {
        match rx.recv_timeout(left) {
            Ok(v) if pred(&v) => return v,
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    panic!("timed out waiting for {what}");
}

const T: Duration = Duration::from_secs(10);

/// Criteria 1, 2 and 4: hub + two nodes as separate processes; attach → roster for the
/// newcomer and a join event on the other node; cross-node delivery; retry deduplicated.
#[test]
fn cross_machine_announce_delivery_and_dedup() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code", "--project", "shop"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex", "--project", "shop"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");

    let rev = e.stream("n2", &["attach", "reviewer", "--json"]);
    expect(&rev, "reviewer attached", T, |v| v["event"] == "attached");
    wait_until("reviewer visible on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });

    // Newcomer gets the roster; the other node hears the join.
    let back = e.stream("n1", &["attach", "backend", "--json"]);
    let att = expect(&back, "backend attached", T, |v| v["event"] == "attached");
    let roster = att["data"]["roster"].as_array().unwrap();
    assert!(roster.iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online" && a["sameMachine"] == false), "roster: {roster:?}");
    expect(&rev, "join event for backend on n2", T, |v| v["event"] == "roster" && v["data"]["kind"] == "joined" && v["data"]["agent"]["handle"] == "backend@pc-one");

    // Cross-node message.
    let r1 = e.json("n1", &["send", "--as", "backend", "@reviewer@pc-two", "please review", "--subject", "PR 12", "--id", "msg_e2e_dedup", "--json"]);
    assert_eq!(r1["remote"]["status"], "accepted", "{r1}");
    assert_eq!(r1["remote"]["count"], 1);
    assert_eq!(r1["local"], 0);
    let wire_id = r1["id"].as_str().unwrap().to_string();
    assert_eq!(r1["clientId"], "msg_e2e_dedup");
    let m = expect(&rev, "message pushed to reviewer", T, |v| v["event"] == "message");
    assert_eq!(m["data"]["message"]["id"], wire_id.as_str());
    assert_eq!(m["data"]["message"]["from"]["handle"], "backend@pc-one");

    // Retry with the same id: deduplicated, delivered once.
    let r2 = e.json("n1", &["send", "--as", "backend", "@reviewer@pc-two", "please review", "--subject", "PR 12", "--id", "msg_e2e_dedup", "--json"]);
    assert_eq!(r2["duplicate"], true, "{r2}");
    std::thread::sleep(Duration::from_millis(500));
    let inbox = e.json("n2", &["inbox", "--as", "reviewer", "--json"]);
    let ids: Vec<&str> = inbox["messages"].as_array().unwrap().iter().map(|m| m["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec![wire_id.as_str()], "{inbox}");

    // The same retry key used by a *different* sender is a different message, not a duplicate.
    let r3 = e.json("n2", &["send", "--as", "reviewer", "@backend@pc-one", "lgtm", "--id", "msg_e2e_dedup", "--json"]);
    assert_eq!(r3["duplicate"], false, "{r3}");
    assert_eq!(r3["remote"]["status"], "accepted", "{r3}");
    assert_ne!(r3["id"].as_str().unwrap(), wire_id);
    let m = expect(&back, "reply pushed to backend", T, |v| v["event"] == "message");
    assert_eq!(m["data"]["message"]["parts"][0]["text"], "lgtm");
}

/// A second daemon on the same home is refused and does not disturb the running one;
/// oversized messages are rejected before anything is committed.
#[test]
fn single_instance_locks_and_size_limits() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend"]);
    e.run("n1", &["agent", "add", "qa", "--role", "qa"]);
    e.start_node("n1");
    wait_until("node ready", T, || e.status("n1")["hub"] == "ready");

    let second_node = e.cmd("n1", &["node", "run"]).output().unwrap();
    assert!(!second_node.status.success());
    assert!(String::from_utf8_lossy(&second_node.stderr).contains("already using this directory"));
    let second_hub = e.cmd("hub", &["hub", "run"]).output().unwrap();
    assert!(!second_hub.status.success());
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(e.status("n1")["hub"], "ready", "the running hub/node must be unaffected");

    // Via stdin: Linux caps a single argv entry at 128 KiB.
    let big = serde_json::json!({"to": {"kind": "handle", "handle": "qa@pc-one"}, "text": "x".repeat(300 * 1024)}).to_string();
    let mut child = e.cmd("n1", &["send", "--as", "backend", "--stdin"]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    {
        use std::io::Write;
        child.stdin.take().unwrap().write_all(big.as_bytes()).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("TOO_LARGE"), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(e.status("n1")["outbox"], 0);
}

/// Criterion 3: a same-machine message is delivered while no hub process is running
/// for the node's whole lifetime.
#[test]
fn same_machine_delivery_without_hub() {
    let mut e = Env::new();
    let hub = e.network(&[("n1", "pc-one")]);
    e.kill(hub); // the node below never sees a hub
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code"]);
    e.run("n1", &["agent", "add", "qa", "--role", "qa", "--runner", "codex"]);
    e.start_node("n1");
    assert_eq!(e.status("n1")["hub"], "offline");

    let qa = e.stream("n1", &["attach", "qa", "--json"]);
    expect(&qa, "qa attached", T, |v| v["event"] == "attached");
    let r = e.json("n1", &["send", "--as", "backend", "@qa@pc-one", "flaky test?", "--json"]);
    assert_eq!(r["local"], 1, "{r}");
    assert_eq!(r["remote"]["status"], "none");
    let m = expect(&qa, "local delivery to qa", T, |v| v["event"] == "message");
    assert_eq!(m["data"]["message"]["parts"][0]["text"], "flaky test?");
    let st = e.status("n1");
    assert_eq!(st["hub"], "offline");
    // Only the metadata journal waits for the hub; the message itself is done.
    assert_eq!(st["outbox"], 1, "{st}");
}

/// Criterion 5: a plain shell script using only the CLI with JSON can attach, be
/// announced, send and receive.
#[cfg(unix)] // uses `sh`
#[test]
fn shell_script_using_only_cli_json() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "ci-bot", "--role", "ci", "--runner", "script", "--mode", "script", "--project", "shop"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex", "--project", "shop"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    let rev = e.stream("n2", &["attach", "reviewer", "--json"]);
    expect(&rev, "reviewer attached", T, |v| v["event"] == "attached");

    let script = e.root.join("bot.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -e
A="$AGENTLAN_BIN --home $N1"
$A attach ci-bot --json > "$OUT/attach.jsonl" &
ATTACH=$!
sleep 1
printf '%s' '{"to":{"kind":"handle","handle":"reviewer@pc-two"},"intent":"status","subject":"CI green","text":"all 42 tests passed"}' \
  | $A send --as ci-bot --stdin --json > "$OUT/send.json"
touch "$OUT/sent"
$A wait --as ci-bot --timeout 15 --json > "$OUT/wait.json"
kill $ATTACH
"#,
    )
    .unwrap();
    let out = e.root.join("out");
    std::fs::create_dir_all(&out).unwrap();
    let mut bot = Command::new("sh")
        .arg(&script)
        .env("AGENTLAN_BIN", BIN)
        .env("N1", e.home("n1"))
        .env("OUT", &out)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // The script's agent is announced to the other machine…
    expect(&rev, "ci-bot announced on n2", T, |v| v["event"] == "roster" && v["data"]["agent"]["handle"] == "ci-bot@pc-one");
    // …and its message arrives.
    let m = expect(&rev, "ci-bot message on n2", T, |v| v["event"] == "message");
    assert_eq!(m["data"]["message"]["subject"], "CI green");
    assert_eq!(m["data"]["message"]["intent"], "status");
    wait_until("script sent", T, || out.join("sent").exists());

    // Reply to the script; its `wait` receives it.
    let r = e.json("n2", &["send", "--as", "reviewer", "@ci-bot@pc-one", "thanks!", "--intent", "result", "--json"]);
    assert_eq!(r["remote"]["status"], "accepted", "{r}");
    let status = bot.wait().unwrap();
    let mut err = String::new();
    use std::io::Read;
    bot.stderr.take().map(|mut s| s.read_to_string(&mut err));
    assert!(status.success(), "script failed: {err}");

    let send: Value = serde_json::from_str(&std::fs::read_to_string(out.join("send.json")).unwrap()).unwrap();
    assert_eq!(send["remote"]["status"], "accepted", "{send}");
    let wait: Value = serde_json::from_str(&std::fs::read_to_string(out.join("wait.json")).unwrap()).unwrap();
    assert_eq!(wait["message"]["from"]["handle"], "reviewer@pc-two", "{wait}");
    assert_eq!(wait["message"]["parts"][0]["text"], "thanks!");
    let attach = std::fs::read_to_string(out.join("attach.jsonl")).unwrap();
    assert!(attach.lines().next().unwrap_or("").contains("\"attached\""), "{attach}");
}

/// Outbox: a cross-machine send made while the hub is down is delivered after it returns.
#[test]
fn queued_while_hub_down_then_delivered() {
    let mut e = Env::new();
    let hub = e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    e.kill(hub);
    wait_until("n1 sees hub down", T, || e.status("n1")["hub"] == "offline");
    let r = e.json("n1", &["send", "--as", "backend", "@reviewer@pc-two", "while you were out", "--id", "msg_e2e_queued", "--json"]);
    assert_eq!(r["remote"]["status"], "queued", "{r}");
    let wire_id = r["id"].as_str().unwrap().to_string();
    e.spawn("hub", &["hub", "run"]);
    wait_until("n1 outbox flushed", Duration::from_secs(15), || {
        let s = e.status("n1");
        s["hub"] == "ready" && s["outbox"] == 0
    });
    wait_until("delivered to reviewer", T, || {
        e.json("n2", &["inbox", "--as", "reviewer", "--json"])["messages"].as_array().unwrap().iter().any(|m| m["id"] == wire_id.as_str())
    });
}

/// A spawned `agentlan mcp` speaking JSON-RPC (newline-delimited) over stdio.
struct Mcp {
    stdin: std::process::ChildStdin,
    rx: mpsc::Receiver<Value>,
    next: u64,
}

impl Mcp {
    fn spawn(e: &mut Env, home: &str, agent: &str, runner: &str) -> Mcp {
        let cmd = e.cmd(home, &["mcp", "--agent", agent, "--runner", runner]);
        Mcp::spawn_cmd(e, cmd)
    }

    /// `agentlan mcp` with no flags, as installed by `claude mcp add agentlan -- agentlan mcp`.
    fn spawn_plain(e: &mut Env, home: &str, alias: &str, cwd: &Path) -> Mcp {
        let mut cmd = e.cmd(home, &["mcp"]);
        cmd.env("AGENTLAN_ALIAS", alias).current_dir(cwd);
        Mcp::spawn_cmd(e, cmd)
    }

    fn spawn_cmd(e: &mut Env, mut cmd: Command) -> Mcp {
        let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let out = child.stdout.take().unwrap();
        e.kids.push(child);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(Result::ok) {
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if tx.send(v).is_err() {
                        break;
                    }
                }
            }
        });
        Mcp { stdin, rx, next: 1 }
    }

    fn write(&mut self, v: Value) {
        use std::io::Write;
        writeln!(self.stdin, "{v}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        self.write(serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        let r = expect(&self.rx, &format!("response to {method}"), Duration::from_secs(20), |v| v["id"] == id);
        assert!(r.get("error").is_none(), "{method} failed: {r}");
        r["result"].clone()
    }

    fn init(&mut self) -> Value {
        self.init_as("e2e")
    }

    fn init_as(&mut self, client: &str) -> Value {
        let r = self
            .request("initialize", serde_json::json!({"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": client, "version": "0"}}));
        self.write(serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        r
    }

    /// Fire a request without waiting for its response; returns the JSON-RPC id.
    fn start(&mut self, method: &str, params: Value) -> u64 {
        let id = self.next;
        self.next += 1;
        self.write(serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        id
    }

    fn finish(&mut self, id: u64) -> Value {
        let r = expect(&self.rx, &format!("response {id}"), Duration::from_secs(30), |v| v["id"] == id);
        r["result"].clone()
    }

    fn tool(&mut self, name: &str, args: Value) -> Value {
        let r = self.request("tools/call", serde_json::json!({"name": name, "arguments": args}));
        assert_ne!(r["isError"], true, "{name} returned an error: {r}");
        r
    }

    /// A tool call that must fail with this error code.
    fn tool_err(&mut self, name: &str, args: Value, code: &str) -> Value {
        let r = self.request("tools/call", serde_json::json!({"name": name, "arguments": args}));
        assert_eq!(r["isError"], true, "{name} should fail: {r}");
        assert_eq!(r["structuredContent"]["error"]["code"], code, "{name}: {r}");
        r
    }
}

/// Agents connect and assign themselves roles from inside their session: `agentlan mcp`
/// is installed with no agent or role; the first session on a PC joins with the server's
/// code; sessions and subagents each register their own identity; roles change live.
#[test]
fn agents_connect_and_register_themselves() {
    let mut e = Env::new();
    e.run("hub", &["hub", "init", "--name", "e2e", "--listen", &e.addr()]);
    let invite = |e: &Env| String::from_utf8(e.run("hub", &["hub", "invite", "--addr", &e.addr()]).stdout).unwrap().trim().to_string();
    let (code1, code2) = (invite(&e), invite(&e));
    e.spawn("hub", &["hub", "run"]);
    wait_port(e.port);
    let proj = e.root.join("shop");
    std::fs::create_dir_all(&proj).unwrap();
    std::fs::write(proj.join("CLAUDE.md"), "# Shop\n\nOur rules.\n").unwrap();

    // Session 1 on pc-one (Claude Code): nothing configured yet.
    let mut a = Mcp::spawn_plain(&mut e, "n1", "pc-one", &proj);
    let init = a.init_as("claude-code");
    assert!(init["instructions"].as_str().unwrap().contains("call `connect`"), "{init}");
    let tools = a.request("tools/list", serde_json::json!({}));
    let names: Vec<&str> = tools["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    for t in ["connect", "register", "leave", "setup", "send", "inbox"] {
        assert!(names.contains(&t), "missing tool {t}: {names:?}");
    }
    let w = a.tool("whoami", serde_json::json!({}));
    assert_eq!(w["structuredContent"]["joined"], false, "{w}");
    a.tool_err("send", serde_json::json!({"to": "all", "text": "hi"}), "NOT_REGISTERED");
    a.tool_err("register", serde_json::json!({"role": "backend"}), "NOT_JOINED");
    a.tool_err("connect", serde_json::json!({}), "NOT_JOINED");
    a.tool_err("connect", serde_json::json!({"code": &code1[..code1.len() - 9]}), "BAD_CODE");

    // Join with the code (this starts the node in the background), then self-register.
    let c = a.tool("connect", serde_json::json!({"code": code1}));
    assert_eq!(c["structuredContent"]["joinedNow"], true, "{c}");
    assert_eq!(c["structuredContent"]["network"], "e2e");
    assert_eq!(c["structuredContent"]["hub"], "ready", "{c}");
    let r = a.tool("register", serde_json::json!({"role": "backend", "description": "orders API"}));
    // Registering again for the same role (e.g. after `connect` with a role, as the setup
    // instructions suggest) returns this session's identity instead of adding backend-2.
    let again = a.tool("register", serde_json::json!({"role": "backend"}));
    assert_eq!(again["structuredContent"]["registered"]["handle"], "backend@pc-one", "{again}");
    assert_eq!(again["structuredContent"]["identities"], serde_json::json!(["backend"]), "{again}");
    let me = &r["structuredContent"]["registered"];
    assert_eq!(me["handle"], "backend@pc-one", "{r}");
    assert_eq!(me["project"], "shop", "project defaults to the folder: {r}");
    assert_eq!(me["runner"], "claude-code", "runner detected from the MCP client: {r}");
    assert!(me["agentId"].as_str().unwrap().starts_with("agt_"));
    let backend_id = me["agentId"].as_str().unwrap().to_string();
    // Later sessions on this PC need no code; a used code can't join another PC.
    a.tool("connect", serde_json::json!({}));
    let mut thief = Mcp::spawn_plain(&mut e, "n3", "pc-three", &proj);
    thief.init();
    let j = thief.tool_err("connect", serde_json::json!({"code": code1}), "JOIN_FAILED");
    assert!(j["structuredContent"]["error"]["msg"].as_str().unwrap().contains("one PC"), "{j}");

    // A subagent in the same session registers its own identity; calls then need `as`.
    let sub = a.tool("register", serde_json::json!({"role": "tester", "name": "backend-tests"}));
    assert_eq!(sub["structuredContent"]["registered"]["handle"], "backend-tests@pc-one", "{sub}");
    assert_eq!(sub["structuredContent"]["identities"], serde_json::json!(["backend", "backend-tests"]));
    a.tool_err("inbox", serde_json::json!({}), "AMBIGUOUS_IDENTITY");
    a.tool("inbox", serde_json::json!({"as": "backend-tests"}));

    // Another Claude session on the same PC taking the same role gets its own name.
    let mut b = Mcp::spawn_plain(&mut e, "n1", "pc-one", &proj);
    b.init_as("claude-code");
    let rb = b.tool("connect", serde_json::json!({"role": "backend"}));
    assert_eq!(rb["structuredContent"]["registered"]["handle"], "backend-2@pc-one", "{rb}");
    b.tool_err("register", serde_json::json!({"role": "x", "name": "backend"}), "AGENT_BUSY");
    // User-made worker profiles can't be claimed by an agent.
    e.run("n1", &["agent", "add", "builder", "--role", "build", "--runner", "codex", "--mode", "worker"]);
    b.tool_err("register", serde_json::json!({"role": "build", "name": "builder"}), "RESERVED");

    // A Codex session on a second PC joins and registers in one call.
    let mut r2 = Mcp::spawn_plain(&mut e, "n2", "pc-two", &proj);
    r2.init_as("codex-mcp-client");
    let rr = r2.tool("connect", serde_json::json!({"code": code2, "role": "reviewer", "description": "reviews PRs"}));
    assert_eq!(rr["structuredContent"]["registered"]["handle"], "reviewer@pc-two", "{rr}");
    assert_eq!(rr["structuredContent"]["registered"]["runner"], "codex");
    let mut roster = Value::Null;
    wait_until("pc-two sees backend with its role and description", T, || {
        roster = r2.tool("list_agents", serde_json::json!({}));
        roster["structuredContent"]["agents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["handle"] == "backend@pc-one" && x["role"] == "backend" && x["description"] == "orders API" && x["state"] == "online")
    });

    // Project names match by repository name: backend's project is "shop" (a folder with
    // no git remote), a sender may say the full github.com/acme/shop.
    let pj = r2.tool("send", serde_json::json!({"to": "project:github.com/acme/shop", "intent": "status", "text": "ci green"}));
    assert_eq!(pj["structuredContent"]["sent"]["remote"]["status"], "accepted", "{pj}");
    assert!(pj["structuredContent"]["sent"]["remote"]["count"].as_u64().unwrap() >= 1, "{pj}");
    let bk = a.tool("inbox", serde_json::json!({"as": "backend"}));
    let ci = bk["structuredContent"]["messages"].as_array().unwrap().iter().find(|m| m["parts"][0]["text"] == "ci green").cloned().expect("project-addressed mail");
    a.tool("ack", serde_json::json!({"as": "backend", "messageId": ci["id"]}));

    // Messages flow between self-registered identities (by role).
    let sent = a.tool("send", serde_json::json!({"as": "backend", "to": "role:reviewer", "subject": "PR 7", "text": "please review"}));
    assert_eq!(sent["structuredContent"]["sent"]["remote"]["status"], "accepted", "{sent}");
    let got = r2.tool("wait_for_message", serde_json::json!({"timeoutS": 15}));
    assert_eq!(got["structuredContent"]["messages"][0]["from"]["handle"], "backend@pc-one", "{got}");

    // An agent changes its own role; the id stays and other PCs see the change.
    let up = a.tool("register", serde_json::json!({"as": "backend", "role": "api"}));
    assert_eq!(up["structuredContent"]["registered"]["agentId"], backend_id.as_str(), "{up}");
    wait_until("pc-two sees the new role", T, || {
        let v = r2.tool("list_agents", serde_json::json!({}));
        v["structuredContent"]["agents"].as_array().unwrap().iter().any(|x| x["handle"] == "backend@pc-one" && x["role"] == "api")
    });

    // The subagent leaves; the session is back to a single identity (no `as` needed).
    a.tool("leave", serde_json::json!({"as": "backend-tests"}));
    let w = a.tool("whoami", serde_json::json!({}));
    assert_eq!(w["structuredContent"]["agent"]["handle"], "backend@pc-one", "{w}");

    // Hooks find this session's identity through the process tree (no --agent). Here two
    // adapters (sessions) share one host process (this test), so the hook can't tell which
    // session it belongs to and stays silent; with one left, it finds it.
    assert!(hook(&e, "n1", "session-start", "auto", "s0").is_none(), "ambiguous host: no context");
    // A role matching an offline profile ("backend-tests", project shop) from another project
    // gets a new name instead of that agent's identity and mail.
    let other = b.tool("register", serde_json::json!({"role": "backend-tests", "project": "other-repo"}));
    assert_eq!(other["structuredContent"]["registered"]["handle"], "backend-tests-2@pc-one", "no cross-project reuse: {other}");
    drop(b);
    wait_until("session b ended", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|x| x["handle"] == "backend-2@pc-one" && x["state"] == "offline")
    });
    let h = hook(&e, "n1", "session-start", "auto", "s1").expect("session-start context");
    assert!(h["hookSpecificOutput"]["additionalContext"].as_str().unwrap().contains("backend@pc-one"), "{h}");

    // setup writes the project instructions into CLAUDE.md, keeping the user's content.
    let st =
        a.tool("setup", serde_json::json!({"roles": [{"name": "backend", "description": "API and DB"}, "reviewer"], "notes": "Run tests before handoff."}));
    assert_eq!(st["structuredContent"]["files"][0]["result"], "appended", "{st}");
    let md = std::fs::read_to_string(proj.join("CLAUDE.md")).unwrap();
    assert!(md.starts_with("# Shop\n\nOur rules.\n") && md.contains("`backend`: API and DB") && md.contains("call `register`"), "{md}");
    assert!(!md.contains("AGENTLAN1-"), "join codes are never written");
    let again = a.tool("setup", serde_json::json!({"roles": ["backend", "reviewer"]}));
    assert_eq!(again["structuredContent"]["files"][0]["result"], "updated");
    assert_eq!(std::fs::read_to_string(proj.join("CLAUDE.md")).unwrap().matches("agentlan:begin").count(), 1);

    // Profiles persist: a later session reusing the name keeps the recorded id.
    drop(a);
    let mut a2 = Mcp::spawn_plain(&mut e, "n1", "pc-one", &proj);
    a2.init_as("claude-code");
    let mut back = Value::Null;
    wait_until("old session released backend", T, || {
        let r = a2.request("tools/call", serde_json::json!({"name": "register", "arguments": {"role": "api", "name": "backend"}}));
        back = r.clone();
        r["isError"] != true
    });
    assert_eq!(back["structuredContent"]["registered"]["agentId"], backend_id.as_str(), "{back}");
}

/// The MCP adapter end to end: Claude-style and Codex-style adapters on two PCs exchange a
/// request and a reply; each sees the same AMF/1 message rendered for its runner.
#[test]
fn mcp_adapters_claude_and_codex_roundtrip() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code", "--project", "shop"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex", "--project", "shop"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");

    let mut rev = Mcp::spawn(&mut e, "n2", "reviewer", "codex");
    let init_rev = rev.init();
    assert!(init_rev["instructions"].as_str().unwrap().contains("reviewer@pc-two"), "{init_rev}");
    wait_until("reviewer online on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });

    let mut back = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    let init = back.init();
    let instr = init["instructions"].as_str().unwrap();
    assert!(instr.contains("You are connected to AgentLAN as backend@pc-one"), "{instr}");
    assert!(instr.contains("reviewer@pc-two"), "roster in instructions: {instr}");
    let tools = back.request("tools/list", serde_json::json!({}));
    let names: Vec<&str> = tools["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    for t in ["whoami", "list_agents", "list_machines", "send", "reply", "inbox", "wait_for_message", "ack"] {
        assert!(names.contains(&t), "missing tool {t}: {names:?}");
    }

    // Claude → Codex request.
    let sent = back.tool("send", serde_json::json!({"to": "@reviewer@pc-two", "intent": "request", "subject": "PR 12", "text": "please review src/auth.rs"}));
    assert_eq!(sent["structuredContent"]["sent"]["remote"]["status"], "accepted", "{sent}");
    let req_id = sent["structuredContent"]["sent"]["id"].as_str().unwrap().to_string();

    // Codex receives it as JSON (content text mirrors structuredContent).
    let got = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 15}));
    assert_eq!(got["structuredContent"]["messages"][0]["subject"], "PR 12", "{got}");
    assert_eq!(got["structuredContent"]["trust"], "untrusted-peer");
    let text: Value = serde_json::from_str(got["content"][0]["text"].as_str().unwrap()).expect("codex text is JSON");
    assert_eq!(text["messages"][0]["id"], req_id.as_str());

    // Codex replies; recipient and thread come from the original.
    let rep = rev.tool("reply", serde_json::json!({"messageId": req_id, "text": "lgtm, one nit on token expiry"}));
    assert_eq!(rep["structuredContent"]["sent"]["remote"]["status"], "accepted", "{rep}");
    // Replying marks the original handled: nothing left in the reviewer's inbox.
    assert_eq!(rep["structuredContent"]["acked"], true, "{rep}");
    let left = rev.tool("inbox", serde_json::json!({}));
    assert!(left["structuredContent"]["messages"].as_array().unwrap().is_empty(), "{left}");

    // Claude reads it wrapped as untrusted peer content.
    let mut inbox = Value::Null;
    wait_until("reply in backend inbox", T, || {
        inbox = back.tool("inbox", serde_json::json!({}));
        inbox["structuredContent"]["messages"].as_array().is_some_and(|a| !a.is_empty())
    });
    let m = &inbox["structuredContent"]["messages"][0];
    assert_eq!(m["inReplyTo"], req_id.as_str());
    assert_eq!(m["subject"], "Re: PR 12");
    // An interim reply with ack: false leaves the message open.
    let interim = back.tool("reply", serde_json::json!({"messageId": m["id"], "text": "looking at it", "ack": false}));
    assert_eq!(interim["structuredContent"]["acked"], false, "{interim}");
    let still = back.tool("inbox", serde_json::json!({}));
    assert!(still["structuredContent"]["messages"].as_array().unwrap().iter().any(|x| x["id"] == m["id"]), "{still}");
    let txt = inbox["content"][0]["text"].as_str().unwrap();
    assert!(txt.contains("<agentlan_message from=\"reviewer@pc-two\"") && txt.contains("untrusted=\"true\""), "{txt}");
    let acked = back.tool("ack", serde_json::json!({"messageId": m["id"]}));
    assert_eq!(acked["structuredContent"]["unread"]["count"], 0, "{acked}");
}

fn inbox_subjects(e: &Env, home: &str, agent: &str) -> Vec<String> {
    e.json(home, &["inbox", "--as", agent, "--json"])["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["subject"].as_str().unwrap_or("").to_string())
        .collect()
}

fn send_draft(e: &Env, home: &str, as_agent: &str, draft: Value) -> Value {
    let mut child = e.cmd(home, &["send", "--as", as_agent, "--stdin", "--json"]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    {
        use std::io::Write;
        child.stdin.take().unwrap().write_all(draft.to_string().as_bytes()).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "send failed: {}", String::from_utf8_lossy(&out.stderr));
    serde_json::from_slice(&out.stdout).unwrap()
}

/// Role, project, machine and pick-one fan-out across two PCs: local legs are delivered by
/// the node, remote legs by the hub, other projects and the sender are excluded.
#[test]
fn fanout_role_project_machine_and_pick_one() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    for (home, name, role, project) in [
        ("n1", "backend", "backend", "shop"),
        ("n1", "qa", "qa", "shop"),
        ("n1", "devops", "devops", "infra"),
        ("n2", "reviewer", "reviewer", "shop"),
        ("n2", "qa2", "qa", "shop"),
    ] {
        e.run(home, &["agent", "add", name, "--role", role, "--project", project]);
    }
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    // Broadcasts go to *attached* agents only.
    let _s: Vec<_> = [("n1", "backend"), ("n1", "qa"), ("n1", "devops"), ("n2", "reviewer"), ("n2", "qa2")]
        .iter()
        .map(|(h, a)| {
            let rx = e.stream(h, &["attach", a, "--json"]);
            expect(&rx, "attached", T, |v| v["event"] == "attached");
            rx
        })
        .collect();
    wait_until("remote agents online on n1", T, || {
        let agents = e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().clone();
        ["reviewer@pc-two", "qa2@pc-two"].iter().all(|h| agents.iter().any(|a| a["handle"] == *h && a["state"] == "online"))
    });

    // Project broadcast: local qa + remote reviewer, qa2; not devops (infra), not the sender.
    let r = send_draft(&e, "n1", "backend", serde_json::json!({"to": {"kind": "project", "project": "shop"}, "subject": "proj"}));
    assert_eq!(r["local"], 1, "{r}");
    assert_eq!(r["remote"], serde_json::json!({"status": "accepted", "count": 2}), "{r}");

    // Role broadcast: qa on both machines.
    let r = send_draft(&e, "n1", "backend", serde_json::json!({"to": {"kind": "role", "role": "qa"}, "subject": "role-qa"}));
    assert_eq!((r["local"].as_u64(), r["remote"]["count"].as_u64()), (Some(1), Some(1)), "{r}");

    // pick:one prefers a local match and skips the remote leg entirely.
    let r = send_draft(&e, "n1", "backend", serde_json::json!({"to": {"kind": "role", "role": "qa", "pick": "one"}, "subject": "pick-local"}));
    assert_eq!(r["local"], 1, "{r}");
    assert_eq!(r["remote"]["status"], "none", "{r}");

    // pick:one with no local match: exactly one remote recipient.
    let r = send_draft(&e, "n2", "qa2", serde_json::json!({"to": {"kind": "role", "role": "backend", "pick": "one"}, "subject": "pick-remote"}));
    assert_eq!((r["local"].as_u64(), r["remote"]["count"].as_u64()), (Some(0), Some(1)), "{r}");

    // Machine target: every attached agent on pc-two, nothing local.
    let r = send_draft(&e, "n1", "backend", serde_json::json!({"to": {"kind": "machine", "machine": "pc-two"}, "subject": "machine"}));
    assert_eq!((r["local"].as_u64(), r["remote"]["count"].as_u64()), (Some(0), Some(2)), "{r}");

    wait_until("remote deliveries settled", T, || inbox_subjects(&e, "n2", "reviewer").len() == 2 && inbox_subjects(&e, "n2", "qa2").len() == 3);
    let mut rv = inbox_subjects(&e, "n2", "reviewer");
    rv.sort();
    assert_eq!(rv, vec!["machine", "proj"]);
    let mut q2 = inbox_subjects(&e, "n2", "qa2");
    q2.sort();
    assert_eq!(q2, vec!["machine", "proj", "role-qa"]);
    let mut q = inbox_subjects(&e, "n1", "qa");
    q.sort();
    assert_eq!(q, vec!["pick-local", "proj", "role-qa"]);
    assert_eq!(inbox_subjects(&e, "n1", "backend"), vec!["pick-remote"], "sender excluded from its own broadcasts");
    assert!(inbox_subjects(&e, "n1", "devops").is_empty(), "other project excluded");
}


/// Request/reply over the CLI: a reply-waiter returns only the reply to its own request,
/// and unrelated mail that arrives meanwhile is left unseen for normal waits.
#[test]
fn request_reply_ignores_unrelated_mail() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");

    // A role target needs an online agent; nobody is attached yet, so it is refused.
    let none = e.cmd("n1", &["request", "--as", "backend", "role:reviewer", "anyone?", "--wait", "1"]).output().unwrap();
    assert!(!none.status.success() && String::from_utf8_lossy(&none.stderr).contains("NOT_DELIVERED"));

    // Direct mail reaches the reviewer even while it is offline.
    let req = e.cmd("n1", &["request", "--as", "backend", "@reviewer@pc-two", "is SKEW=30s right?", "--wait", "20", "--json"]).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let q = e.json("n2", &["wait", "--as", "reviewer", "--timeout", "10", "--json"]);
    let qid = q["message"]["id"].as_str().expect("reviewer received the request").to_string();
    assert_eq!(q["message"]["intent"], "request");
    assert!(q["message"]["replyBy"].is_string());

    // Unrelated mail first, then the actual reply.
    e.run("n2", &["send", "--as", "reviewer", "@backend@pc-one", "unrelated note", "--subject", "noise"]);
    std::thread::sleep(Duration::from_millis(300));
    e.run("n2", &["send", "--as", "reviewer", "@backend@pc-one", "yes, 30s covers clock drift", "--reply-to", &qid]);

    let out = req.wait_with_output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let r: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(r["status"], "replied", "{r}");
    assert_eq!(r["reply"]["inReplyTo"], qid.as_str());
    assert_eq!(r["reply"]["threadId"], q["message"]["threadId"], "a `send --reply-to` continues the request's conversation");
    assert_eq!(r["reply"]["parts"][0]["text"], "yes, 30s covers clock drift");
    assert_eq!(r["autoAck"], true, "a reply (plain `send --reply-to` is chat) is marked handled once returned: {r}");
    assert_eq!(q["autoAck"], false, "a request stays open until it's answered: {q}");

    // The unrelated message was not consumed by the request, and isn't an answer.
    let w = e.json("n1", &["wait", "--as", "backend", "--timeout", "3", "--json"]);
    assert_eq!(w["message"]["subject"], "noise", "{w}");
    assert_eq!(w["autoAck"], false, "{w}");
    let ib = e.json("n1", &["inbox", "--as", "backend", "--json"]);
    let subjects: Vec<&str> = ib["messages"].as_array().unwrap().iter().map(|m| m["subject"].as_str().unwrap_or("")).collect();
    assert_eq!(subjects, ["noise"], "only the unanswered note is left: {ib}");
}

/// MCP `request` round trip, and a cancelled `wait_for_message` does not lose the
/// message that arrives afterwards.
#[test]
fn mcp_request_tool_and_cancelled_wait() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    let mut back = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    back.init();
    let mut rev = Mcp::spawn(&mut e, "n2", "reviewer", "codex");
    rev.init();
    wait_until("reviewer online on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });

    // backend asks (blocking tool call in flight)…
    let rid = back.start("tools/call", serde_json::json!({"name": "request", "arguments": {"to": "role:reviewer", "text": "approve PR 12?", "waitS": 20}}));
    // …reviewer receives it and replies.
    let got = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 15}));
    let qid = got["structuredContent"]["messages"][0]["id"].as_str().unwrap().to_string();
    rev.tool("reply", serde_json::json!({"messageId": qid, "text": "approved"}));
    let res = back.finish(rid);
    assert_eq!(res["structuredContent"]["status"], "replied", "{res}");
    assert_eq!(res["structuredContent"]["reply"]["parts"][0]["text"], "approved");
    // The answer is marked handled once the asker has it; the question was not (it needed a reply).
    assert_eq!(res["structuredContent"]["autoAck"], true, "{res}");
    assert_eq!(got["structuredContent"]["autoAck"], false, "a request isn't an answer: {got}");
    // (marked handled in the background, just after the tool returns)
    wait_until("answer marked handled", T, || {
        back.tool("inbox", serde_json::json!({}))["structuredContent"]["messages"].as_array().is_some_and(|a| a.is_empty())
    });
    // A handled answer can still be replied to (follow-up on the same thread).
    let aid = res["structuredContent"]["reply"]["id"].as_str().unwrap().to_string();
    let fu = back.tool("reply", serde_json::json!({"messageId": aid, "text": "thanks, merging", "intent": "status"}));
    assert!(fu["structuredContent"]["sent"]["id"].is_string(), "reply to a handled answer: {fu}");
    let got_fu = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 10}));
    assert_eq!(got_fu["structuredContent"]["messages"][0]["inReplyTo"], aid.as_str(), "{got_fu}");

    // Cancelling one wait leaves a concurrent request on the same adapter untouched.
    let rid2 = back.start("tools/call", serde_json::json!({"name": "request", "arguments": {"to": "@reviewer@pc-two", "text": "second?", "waitS": 20}}));
    let wid0 = back.start("tools/call", serde_json::json!({"name": "wait_for_message", "arguments": {"timeoutS": 30}}));
    std::thread::sleep(Duration::from_millis(300));
    back.write(serde_json::json!({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": wid0, "reason": "test"}}));
    let got2 = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 15}));
    let q2 = got2["structuredContent"]["messages"][0]["id"].as_str().unwrap().to_string();
    rev.tool("reply", serde_json::json!({"messageId": q2, "text": "second answer"}));
    let res2 = back.finish(rid2);
    assert_eq!(res2["structuredContent"]["status"], "replied", "request survived the other wait's cancel: {res2}");

    // Cancel a pending wait, then deliver: the next wait must still get the message.
    let wid = rev.start("tools/call", serde_json::json!({"name": "wait_for_message", "arguments": {"timeoutS": 30}}));
    std::thread::sleep(Duration::from_millis(300));
    rev.write(serde_json::json!({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": wid, "reason": "test"}}));
    std::thread::sleep(Duration::from_millis(500));
    back.tool("send", serde_json::json!({"to": "@reviewer@pc-two", "subject": "after-cancel", "text": "still there?"}));
    let again = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 10}));
    assert_eq!(again["structuredContent"]["messages"][0]["subject"], "after-cancel", "{again}");

    // An answer that arrives through wait_for_message is marked handled too.
    let ac = again["structuredContent"]["messages"][0]["id"].as_str().unwrap().to_string();
    rev.tool("reply", serde_json::json!({"messageId": ac, "text": "yes"}));
    let ans = back.tool("wait_for_message", serde_json::json!({"timeoutS": 10}));
    assert_eq!(ans["structuredContent"]["messages"][0]["inReplyTo"], ac.as_str(), "{ans}");
    assert_eq!(ans["structuredContent"]["autoAck"], true, "{ans}");
    // (marked handled in the background, just after the tool returns)
    wait_until("answer marked handled", T, || {
        back.tool("inbox", serde_json::json!({}))["structuredContent"]["messages"].as_array().is_some_and(|a| a.is_empty())
    });
}

/// Run a Claude Code hook the way Claude does: JSON on stdin; stdout is JSON or empty.
fn hook(e: &Env, home: &str, event: &str, agent: &str, session: &str) -> Option<Value> {
    let mut child = e.cmd(home, &["hook", "claude", event, "--agent", agent]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    {
        use std::io::Write;
        let inp = serde_json::json!({"session_id": session, "hook_event_name": event, "cwd": "/tmp", "stop_hook_active": false});
        child.stdin.take().unwrap().write_all(inp.to_string().as_bytes()).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "hook {event} failed: {}", String::from_utf8_lossy(&out.stderr));
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() { None } else { Some(serde_json::from_str(&s).unwrap_or_else(|_| panic!("hook output is not JSON: {s}"))) }
}

/// Claude Code hooks: identity + peers at session start, pointer-only unread notices
/// (no peer-written text), no repeats, and Stop continuation only for worker profiles.
#[test]
fn claude_hooks_pointer_only_and_worker_only_stop() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code", "--project", "shop"]);
    e.run("n1", &["agent", "add", "bot", "--role", "fixer", "--runner", "claude-code", "--mode", "worker", "--project", "shop"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex", "--project", "shop"]);

    // Node not running yet: hooks must stay silent and succeed.
    assert!(hook(&e, "n1", "session-start", "backend", "s0").is_none());

    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    let rev = e.stream("n2", &["attach", "reviewer", "--json"]);
    expect(&rev, "reviewer attached", T, |v| v["event"] == "attached");
    wait_until("reviewer online on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });

    let ss = hook(&e, "n1", "session-start", "backend", "s1").expect("session-start context");
    assert_eq!(ss["hookSpecificOutput"]["hookEventName"], "SessionStart");
    let c = ss["hookSpecificOutput"]["additionalContext"].as_str().unwrap();
    assert!(c.contains("you are backend@pc-one") && c.contains("reviewer@pc-two"), "{c}");
    assert!(hook(&e, "n1", "user-prompt-submit", "backend", "s1").is_none(), "no unread → no output");

    // Mail arrives; the notice names sender/intent/id but never the subject.
    e.run("n2", &["send", "--as", "reviewer", "@backend@pc-one", "body text", "--subject", "SECRET-SUBJECT", "--intent", "request"]);
    e.run("n2", &["send", "--as", "reviewer", "@bot@pc-one", "fix it", "--subject", "job"]);
    wait_until("mail stored on n1", T, || e.status("n1")["outbox"] == 0 && hook(&e, "n1", "user-prompt-submit", "backend", "probe").is_some());
    let up = hook(&e, "n1", "user-prompt-submit", "backend", "s1").expect("pointer");
    let t = up["hookSpecificOutput"]["additionalContext"].as_str().unwrap();
    assert!(t.contains("1 unread") && t.contains("reviewer@pc-two") && t.contains("request"), "{t}");
    assert!(!t.contains("SECRET-SUBJECT") && !t.contains("body text"), "peer text leaked: {t}");

    // post-tool-use points out each new message once.
    assert!(hook(&e, "n1", "post-tool-use", "backend", "s2").is_some());
    assert!(hook(&e, "n1", "post-tool-use", "backend", "s2").is_none(), "no repeat for the same mail");

    // Stop: interactive profiles may always stop; workers continue up to the budget.
    assert!(hook(&e, "n1", "stop", "backend", "s3").is_none());
    for i in 0..3 {
        let st = hook(&e, "n1", "stop", "bot", "s4").unwrap_or_else(|| panic!("worker stop block #{i}"));
        assert_eq!(st["decision"], "block");
        assert!(!st["reason"].as_str().unwrap().contains("fix it"));
    }
    assert!(hook(&e, "n1", "stop", "bot", "s4").is_none(), "continuation budget exhausted");
    // Tool calls made while the worker keeps going must not reset the budget…
    hook(&e, "n1", "post-tool-use", "bot", "s4");
    assert!(hook(&e, "n1", "stop", "bot", "s4").is_none(), "post-tool-use must not reset the Stop budget");
    // …but a human prompt does.
    hook(&e, "n1", "user-prompt-submit", "bot", "s4");
    assert!(hook(&e, "n1", "stop", "bot", "s4").is_some(), "a human turn resets the budget");

    // Peeking never consumed the mail.
    let w = e.json("n1", &["wait", "--as", "backend", "--timeout", "3", "--json"]);
    assert_eq!(w["message"]["subject"], "SECRET-SUBJECT");

    // Settings snippet: Stop only for worker profiles.
    let p_bot = String::from_utf8(e.run("n1", &["hooks", "print", "claude", "--agent", "bot"]).stdout).unwrap();
    let p_back = String::from_utf8(e.run("n1", &["hooks", "print", "claude", "--agent", "backend"]).stdout).unwrap();
    assert!(p_bot.contains("\"Stop\"") && !p_back.contains("\"Stop\""));
    assert!(serde_json::from_str::<Value>(&p_back).unwrap()["hooks"]["PostToolUse"][0]["matcher"] == "*");
}


/// Loop guard: a thread pauses after its automated-message budget, the pause survives a
/// node restart, resuming needs a human at a terminal, and per-pair rate limits apply.
#[cfg(unix)] // drives a pseudo-terminal via python3
#[test]
fn loop_guard_pauses_threads_and_limits_pairs() {
    let mut e = Env::new();
    e.extra_env = vec![("AGENTLAN_LIMIT_THREAD", "3".into()), ("AGENTLAN_LIMIT_PAIR_HOUR", "6".into())];
    e.network(&[("n1", "pc-one")]);
    e.run("n1", &["agent", "add", "a", "--role", "dev"]);
    e.run("n1", &["agent", "add", "b", "--role", "dev"]);
    let node = e.start_node("n1");

    let send = |e: &Env, thread: &str| e.cmd("n1", &["send", "--as", "a", "@b@pc-one", "ping", "--thread", thread]).output().unwrap();
    for _ in 0..3 {
        assert!(send(&e, "thr_loop").status.success());
    }
    let paused = send(&e, "thr_loop");
    assert!(!paused.status.success());
    assert!(String::from_utf8_lossy(&paused.stderr).contains("PAUSED"), "{}", String::from_utf8_lossy(&paused.stderr));
    assert_eq!(e.status("n1")["pausedThreads"][0]["threadId"], "thr_loop");

    // The pause is persisted: still paused after a node restart.
    e.kill(node);
    e.start_node("n1");
    assert!(String::from_utf8_lossy(&send(&e, "thr_loop").stderr).contains("PAUSED"));

    // Resuming refuses without a terminal (an agent can run the CLI too)…
    let no_tty = e.cmd("n1", &["thread", "resume", "thr_loop"]).stdin(Stdio::null()).output().unwrap();
    assert!(!no_tty.status.success() && String::from_utf8_lossy(&no_tty.stderr).contains("interactive terminal"));
    // …and works for a human at a (pseudo-)terminal who confirms.
    let mut pty = Command::new("python3")
        .args(["-c", "import pty,sys; sys.exit(pty.spawn(sys.argv[1:]) >> 8)", BIN, "--home"])
        .arg(e.home("n1"))
        .args(["thread", "resume", "thr_loop"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write;
        std::thread::sleep(Duration::from_millis(500));
        pty.stdin.take().unwrap().write_all(b"y\n").unwrap();
    }
    let out = pty.wait_with_output().unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).contains("resumed thread:thr_loop"), "{}", String::from_utf8_lossy(&out.stdout));
    assert!(send(&e, "thr_loop").status.success(), "thread usable again after resume");

    // Ping-pong without threads (each plain send starts a new thread): the pair budget
    // stops it, however the peer is addressed (handle or agent id), and pauses the pair.
    let b_id = e.json("n1", &["agent", "ls", "--json"]).as_array().unwrap().iter().find(|p| p["name"] == "b").unwrap()["agentId"].as_str().unwrap().to_string();
    let plain = |e: &Env, to: &str| e.cmd("n1", &["send", "--as", "a", to, "ping"]).output().unwrap();
    assert!(plain(&e, &b_id).status.success(), "5th a→b (by id)");
    assert!(plain(&e, "@b@pc-one").status.success(), "6th a→b (by handle)");
    let stopped = plain(&e, &b_id);
    let err = String::from_utf8_lossy(&stopped.stderr).to_string();
    assert!(err.contains("PAUSED") && err.contains(&format!("pair:")) && err.contains(&b_id), "{err}");
    assert!(e.status("n1")["pausedThreads"].as_array().unwrap().iter().any(|p| p["key"].as_str().unwrap().starts_with("pair:")));
    // The reverse direction is its own pair and still allowed.
    assert!(e.cmd("n1", &["send", "--as", "b", "@a@pc-one", "pong"]).output().unwrap().status.success());
}

/// Only the request's actual recipient can satisfy it: a third agent quoting the request
/// id in inReplyTo is treated as ordinary mail.
#[test]
fn request_reply_only_from_the_real_recipient() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer"]);
    e.run("n2", &["agent", "add", "intruder", "--role", "other"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");

    let req = e.cmd("n1", &["request", "--as", "backend", "@reviewer@pc-two", "approve?", "--wait", "20", "--json"]).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let q = e.json("n2", &["wait", "--as", "reviewer", "--timeout", "10", "--json"]);
    let qid = q["message"]["id"].as_str().unwrap().to_string();
    e.run("n2", &["send", "--as", "intruder", "@backend@pc-one", "forged yes", "--reply-to", &qid]);
    std::thread::sleep(Duration::from_millis(500));
    e.run("n2", &["send", "--as", "reviewer", "@backend@pc-one", "real answer", "--reply-to", &qid]);
    let out = req.wait_with_output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let r: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(r["reply"]["parts"][0]["text"], "real answer", "{r}");
    assert_eq!(r["reply"]["from"]["handle"], "reviewer@pc-two");
    // The forged message is just mail.
    let w = e.json("n1", &["wait", "--as", "backend", "--timeout", "3", "--json"]);
    assert_eq!(w["message"]["parts"][0]["text"], "forged yes");
}

/// Group sends (project broadcast) skip recipients whose pair is paused — locally at the
/// node and remotely at the hub.
#[test]
fn group_sends_respect_paused_pairs() {
    let mut e = Env::new();
    e.extra_env = vec![("AGENTLAN_LIMIT_PAIR_HOUR", "2".into())];
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "a", "--role", "dev", "--project", "p"]);
    e.run("n1", &["agent", "add", "b", "--role", "dev", "--project", "p"]);
    e.run("n2", &["agent", "add", "c", "--role", "dev", "--project", "p"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    let _b = e.stream("n1", &["attach", "b", "--json"]);
    let _c = e.stream("n2", &["attach", "c", "--json"]);
    wait_until("c online on n1", T, || e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|x| x["handle"] == "c@pc-two" && x["state"] == "online"));

    let broadcast = |e: &Env| send_draft(e, "n1", "a", serde_json::json!({"to": {"kind": "project", "project": "p"}, "subject": "all hands"}));
    let r = broadcast(&e);
    assert_eq!((r["local"].as_u64(), r["remote"]["count"].as_u64()), (Some(1), Some(1)), "{r}");

    // Exhaust and pause a→b (local) and a→c (remote) with direct sends.
    for to in ["@b@pc-one", "@c@pc-two"] {
        e.run("n1", &["send", "--as", "a", to, "x"]);
        let third = e.cmd("n1", &["send", "--as", "a", to, "x"]).output().unwrap();
        assert!(String::from_utf8_lossy(&third.stderr).contains("PAUSED"), "{to}: {}", String::from_utf8_lossy(&third.stderr));
    }
    let r = broadcast(&e);
    assert_eq!(r["local"], 0, "paused local pair skipped: {r}");
    assert_eq!(r["remote"]["count"], 0, "paused remote pair excluded by the hub: {r}");
}

/// The one-liners: `agentlan up` on the server PC prints a connect command, and
/// `agentlan connect <code>` on another PC joins and runs — then they can talk.
#[test]
fn up_and_connect_one_liners() {
    let mut e = Env::new();
    let addr = e.addr();
    let mut up = e
        .cmd("srv", &["up", "--name", "e2e", "--listen", &addr, "--advertise", &addr, "--alias", "pc-srv"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let out = up.stdout.take().unwrap();
    e.kids.push(up);
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let t0 = Instant::now();
    let code = loop {
        let line = rx.recv_timeout(Duration::from_secs(15)).expect("`up` printed a connect command");
        if let Some(rest) = line.trim().strip_prefix("agentlan connect ") {
            break rest.trim().to_string();
        }
        assert!(t0.elapsed() < Duration::from_secs(15));
    };
    assert!(code.starts_with("AGENTLAN1-"));

    e.spawn("pc2", &["connect", &code, "--alias", "pc-two"]);
    let (srv, pc2) = (e.home("srv"), e.home("pc2"));
    wait_until("both nodes up", T, || e.cmd_ok(&srv) && e.cmd_ok(&pc2));
    wait_until("both nodes ready", T, || e.status("srv")["hub"] == "ready" && e.status("pc2")["hub"] == "ready");

    e.run("srv", &["agent", "add", "lead", "--role", "lead"]);
    e.run("pc2", &["agent", "add", "helper", "--role", "helper"]);
    // Agents added while the node runs are registered live and show up on other PCs.
    wait_until("helper (added live) visible on the server PC", T, || {
        e.json("srv", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "helper@pc-two")
    });
    let r = e.json("srv", &["send", "--as", "lead", "@helper@pc-two", "hello from up", "--json"]);
    assert_eq!(r["remote"]["status"], "accepted", "{r}");
    wait_until("delivered", T, || {
        e.json("pc2", &["inbox", "--as", "helper", "--json"])["messages"].as_array().unwrap().iter().any(|m| m["parts"][0]["text"] == "hello from up")
    });

    // Running `connect` again on a joined PC needs no code (it just runs the node);
    // here it refuses up front — no false "running" message — because a node is running.
    let again = e.cmd("pc2", &["connect"]).output().unwrap();
    let out = String::from_utf8_lossy(&again.stdout);
    assert!(again.status.success() && out.contains("already running"), "{out}");
    // A second `up` leaves the running node alone and fails only because the server's
    // port is taken.
    let again_up = e.cmd("srv", &["up"]).output().unwrap();
    assert!(!again_up.status.success(), "second up must not start a second server");

    // `node stop` on the server PC stops only its node; the server keeps serving pc2.
    e.run("srv", &["node", "stop"]);
    wait_until("srv node down", T, || !e.cmd_ok(&srv));
    e.run("pc2", &["agent", "add", "helper2", "--role", "helper"]);
    let r = e.json("pc2", &["send", "--as", "helper", "@helper2@pc-two", "still here", "--json"]);
    assert_eq!(r["local"], 1, "{r}");
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(e.status("pc2")["hub"], "ready", "server still up after the srv node stopped");
}

/// Worker wake over ACP, with a fake ACP agent: direct mail wakes an idle worker and its
/// reply comes back; the next wake resumes the same session; a single-recipient request to
/// a role with nobody online reaches the wake-enabled worker (through the hub, and through
/// the node for a same-PC sender); broadcasts don't wake; a messages-only wake refuses
/// other actions; the hourly wake budget holds.
#[test]
fn worker_wake_over_acp() {
    let mut e = Env::new();
    e.extra_env.push(("AGENTLAN_WAKE_DEBOUNCE_MS", "200".into()));
    e.extra_env.push(("AGENTLAN_WAKE_PER_HOUR", "4".into()));
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "lead", "--role", "lead"]);
    e.run("n2", &["agent", "add", "builder", "--role", "builder", "--mode", "worker", "--project", "shop"]);
    e.run("n2", &["agent", "add", "helper", "--role", "helper"]);
    let log = e.root.join("fake-acp.log");
    let work = e.root.join("builder-proj");
    std::fs::create_dir_all(&work).unwrap();
    let fake = format!("\"{BIN}\" dev fake-acp --log \"{}\"", log.display());
    e.run("n2", &["wake", "enable", "builder", "--agent", "custom", "--command", &fake, "--cwd", work.to_str().unwrap(), "--timeout", "60", "--env", "AGENTLAN_FAKE_MARK=per-worker"]);
    // Lockdown variables can't be overridden per worker.
    let bad_env = e.cmd("n2", &["wake", "enable", "builder", "--agent", "custom", "--command", &fake, "--env", "CODEX_CONFIG={}"]).output().unwrap();
    assert!(!bad_env.status.success() && String::from_utf8_lossy(&bad_env.stderr).contains("can't be overridden"));
    // Only worker profiles can be woken.
    let bad = e.cmd("n2", &["wake", "enable", "helper", "--agent", "custom", "--command", &fake]).output().unwrap();
    assert!(!bad.status.success() && String::from_utf8_lossy(&bad.stderr).contains("not a worker"));
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    wait_until("builder known on pc-one", T, || e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "builder@pc-two"));
    let log_lines = || std::fs::read_to_string(&log).unwrap_or_default();
    let lead_replies = |e: &Env| -> Vec<String> {
        e.json("n1", &["inbox", "--as", "lead", "--json"])["messages"].as_array().unwrap().iter().filter_map(|m| m["parts"][0]["text"].as_str().map(String::from)).collect()
    };

    // 1. Direct mail to the idle worker wakes it; its reply reaches the sender.
    e.run("n1", &["send", "--as", "lead", "@builder@pc-two", "build the shop"]);
    wait_until("reply from the woken worker", Duration::from_secs(20), || lead_replies(&e).iter().any(|t| t == "fake reply: build the shop"));
    let l = log_lines();
    assert!(l.contains("session/new "), "{l}");
    assert!(l.contains("env AGENTLAN_FAKE_MARK=per-worker"), "the worker's --env reaches the agent: {l}");
    assert!(l.contains("permission no"), "messages-only wake rejects a file edit: {l}");
    // The prompt points at the mail (who, how many) but never carries peer-written text.
    let prompt = l.lines().find(|x| x.starts_with("prompt ")).unwrap();
    assert!(prompt.contains("you are builder@pc-two") && prompt.contains("1 unread") && prompt.contains("from lead@pc-one"), "{prompt}");
    assert!(!prompt.contains("build the shop"), "pointer-only prompt: {prompt}");
    let sid = l.lines().find_map(|x| x.strip_prefix("session/new ")).unwrap().to_string();
    wait_until("wake recorded", T, || e.json("n2", &["wake", "ls", "--json"])["workers"][0]["config"]["sessionId"] == sid.as_str());

    // 2. The next wake resumes the same session.
    e.run("n1", &["send", "--as", "lead", "@builder@pc-two", "and test it"]);
    wait_until("second reply", Duration::from_secs(20), || lead_replies(&e).iter().any(|t| t == "fake reply: and test it"));
    assert!(log_lines().contains(&format!("session/resume {sid}")), "{}", log_lines());

    // Changing a worker's access never resumes a session started with other permissions.
    let wdir = work.to_str().unwrap();
    e.run("n2", &["wake", "enable", "builder", "--agent", "custom", "--command", &fake, "--cwd", wdir, "--timeout", "60", "--access", "work"]);
    assert!(e.json("n2", &["wake", "ls", "--json"])["workers"][0]["config"]["sessionId"].is_null(), "access change keeps no session");
    e.run("n2", &["wake", "enable", "builder", "--agent", "custom", "--command", &fake, "--cwd", wdir, "--timeout", "60"]);

    // 3. A request to the role from another PC, nobody online: the hub picks the
    //    wake-enabled worker, and the reply comes back to the waiting request.
    let r = e.json("n1", &["request", "--as", "lead", "role:builder", "ship it?", "--wait", "25", "--json"]);
    assert_eq!(r["reply"]["parts"][0]["text"], "fake reply: ship it?", "{r}");
    assert_eq!(r["sent"]["remote"]["status"], "accepted", "request reports the hub's verdict, not 'queued': {r}");

    // 4. The same from this PC: the node picks its own wake-enabled worker.
    let r = e.json("n2", &["request", "--as", "helper", "role:builder", "local ask", "--wait", "25", "--json"]);
    assert_eq!(r["reply"]["parts"][0]["text"], "fake reply: local ask", "{r}");
    let wakes = log_lines().matches("prompt ").count();
    assert_eq!(wakes, 4, "{}", log_lines());

    // 5. Broadcasts to the role don't wake anyone.
    e.run("n1", &["send", "--as", "lead", "role:builder", "fyi"]);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(log_lines().matches("prompt ").count(), 4, "a broadcast woke the worker");

    // 6. The hourly budget (4 here) stops the next wake and records why.
    e.run("n1", &["send", "--as", "lead", "@builder@pc-two", "one more"]);
    wait_until("budget recorded", T, || {
        e.json("n2", &["wake", "ls", "--json"])["workers"][0]["config"]["last"]["outcome"].as_str().is_some_and(|o| o.starts_with("skipped: reached 4"))
    });
    assert_eq!(log_lines().matches("prompt ").count(), 4);
    // A woken worker's MCP server offers only the messaging tools (no setup, connect,
    // register or leave), and refuses the others even if called by name.
    let cmd = e.cmd("n2", &["mcp", "--agent", "builder", "--worker"]);
    let mut w = Mcp::spawn_cmd(&mut e, cmd);
    w.init();
    let tl = w.request("tools/list", serde_json::json!({}));
    let mut names: Vec<String> = tl["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect();
    names.sort();
    assert_eq!(names, ["ack", "inbox", "list_agents", "list_machines", "reply", "request", "send", "wait_for_message", "whoami"], "{names:?}");
    w.tool_err("setup", serde_json::json!({"notes": "x"}), "NOT_AVAILABLE");
    drop(w);
    wait_until("worker mcp detached", T, || {
        e.json("n2", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "builder@pc-two" && a["state"] == "offline")
    });
    // The worker never stays online after a wake.
    let b = e.json("n2", &["ls", "--json"])["agents"].as_array().unwrap().iter().find(|a| a["handle"] == "builder@pc-two").cloned().unwrap();
    assert_eq!(b["state"], "offline", "{b}");
}

/// Wait up to `max` for a JSON-RPC notification with this method; None if it never came.
fn notification(rx: &mpsc::Receiver<Value>, method: &str, max: Duration) -> Option<Value> {
    let t = Instant::now();
    while let Some(left) = max.checked_sub(t.elapsed()) {
        match rx.recv_timeout(left) {
            Ok(v) if v["method"] == method => return Some(v),
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    None
}

/// Live delivery to an open Claude Code session: new mail is pushed as a channel
/// notification carrying a pointer only (never the message text); mail a waiting call
/// already took isn't pushed.
#[test]
fn live_delivery_pushes_a_channel_pointer() {
    let mut e = Env::new();
    let code = {
        e.run("hub", &["hub", "init", "--name", "e2e", "--listen", &e.addr()]);
        String::from_utf8(e.run("hub", &["hub", "invite", "--addr", &e.addr()]).stdout).unwrap().trim().to_string()
    };
    e.spawn("hub", &["hub", "run"]);
    wait_port(e.port);
    let proj = e.root.join("proj");
    std::fs::create_dir_all(&proj).unwrap();
    let mut a = Mcp::spawn_plain(&mut e, "n1", "pc-one", &proj);
    let init = a.init_as("claude-code");
    assert!(init["capabilities"]["experimental"]["claude/channel"].is_object(), "channel capability declared: {init}");
    a.tool("connect", serde_json::json!({"code": code, "role": "lead"}));
    let mut b = Mcp::spawn_plain(&mut e, "n1", "pc-one", &proj);
    b.init_as("e2e");
    b.tool("register", serde_json::json!({"role": "peer"}));

    // Taken by a waiting call: no push.
    let w = a.start("tools/call", serde_json::json!({"name": "wait_for_message", "arguments": {"timeoutS": 15}}));
    std::thread::sleep(Duration::from_millis(300));
    b.tool("send", serde_json::json!({"to": "@lead@pc-one", "text": "first, while you wait"}));
    let got = a.finish(w);
    assert_eq!(got["structuredContent"]["messages"][0]["parts"][0]["text"], "first, while you wait", "{got}");
    assert!(notification(&a.rx, "notifications/claude/channel", Duration::from_secs(3)).is_none(), "no push for mail already handed over");

    // New mail while not waiting: one channel notification with a pointer.
    b.tool("send", serde_json::json!({"to": "@lead@pc-one", "intent": "request", "text": "SECRET BODY: please review"}));
    let n = notification(&a.rx, "notifications/claude/channel", Duration::from_secs(10)).expect("channel notification");
    let content = n["params"]["content"].as_str().unwrap();
    assert!(content.contains("1 new message for lead") && content.contains("peer@pc-one") && content.contains("intent request"), "{content}");
    assert!(!content.contains("SECRET BODY"), "pointer only: {content}");
    assert_eq!(n["params"]["meta"]["from"], "peer@pc-one", "{n}");

    // The adapter reported it to the node: it's in the activity log, labelled honestly.
    wait_until("live delivery in the log", T, || {
        log_rows(&e, "n1", &["--json"]).iter().any(|r| r["kind"] == "live" && r["body"]["ok"] == true && r["body"]["how"] == "claude channel (pointer sent)")
    });
    let txt = String::from_utf8(e.run("n1", &["log"]).stdout).unwrap();
    assert!(txt.contains("⚡ live → lead@pc-one: 1 new (claude channel (pointer sent))"), "{txt}");
}

/// `agentlan log --json` (or `--network`) as parsed rows.
fn log_rows(e: &Env, home: &str, args: &[&str]) -> Vec<Value> {
    let mut a = vec!["log"];
    a.extend_from_slice(args);
    let out = e.run(home, &a);
    String::from_utf8_lossy(&out.stdout).lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
}

/// The activity log: each PC's timeline of a request/reply across PCs, filters, the
/// server's network view, and peer text that can't drive the terminal.
#[test]
fn log_shows_the_timeline() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    let mut back = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    back.init();
    let mut rev = Mcp::spawn(&mut e, "n2", "reviewer", "codex");
    rev.init();
    wait_until("reviewer online on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });
    let rid = back.start("tools/call", serde_json::json!({"name": "request", "arguments": {"to": "@reviewer@pc-two", "subject": "PR 12\u{1b}[2J", "text": "approve PR 12?", "waitS": 20}}));
    let got = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 15}));
    let qid = got["structuredContent"]["messages"][0]["id"].as_str().unwrap().to_string();
    rev.tool("reply", serde_json::json!({"messageId": qid, "text": "approved"}));
    let res = back.finish(rid);
    let thread = res["structuredContent"]["sent"]["threadId"].as_str().unwrap().to_string();

    // pc-one: the request going out, the answer coming back, seen and handled.
    wait_until("pc-one's timeline", T, || {
        let rows = log_rows(&e, "n1", &["--json"]);
        let has = |k: &str, f: &dyn Fn(&Value) -> bool| rows.iter().any(|r| r["kind"] == k && f(r));
        has("node", &|r| r["body"]["status"] == "started")
            && has("session", &|r| r["body"]["status"] == "attached" && r["body"]["handle"] == "backend@pc-one")
            && has("sent", &|r| r["msgId"] == qid.as_str() && r["body"]["to"]["handle"] == "reviewer@pc-two" && r["body"]["remote"] == true)
            && has("received", &|r| r["body"]["from"] == "reviewer@pc-two" && r["body"]["inReplyTo"] == qid.as_str())
            && has("state", &|r| r["body"]["state"] == "acked" && r["body"]["from"] == "reviewer@pc-two")
    });
    // pc-two: the request arriving, seen, handled by the reply.
    wait_until("pc-two's timeline", T, || {
        let rows = log_rows(&e, "n2", &["--json"]);
        rows.iter().any(|r| r["kind"] == "received" && r["msgId"] == qid.as_str())
            && rows.iter().any(|r| r["kind"] == "state" && r["msgId"] == qid.as_str() && r["body"]["state"] == "acked")
            && rows.iter().any(|r| r["kind"] == "sent" && r["body"]["inReplyTo"] == qid.as_str())
    });

    // The server's answer to a send is recorded: taken for the request, rejected for nobody.
    back.tool("send", serde_json::json!({"to": "@ghost@pc-two", "subject": "nobody home", "text": "hello?"}));
    wait_until("server answers in the log", T, || {
        let rows = log_rows(&e, "n1", &["--json"]);
        rows.iter().any(|r| r["kind"] == "remote" && r["msgId"] == qid.as_str() && r["body"]["result"]["status"] == "accepted")
            && rows.iter().any(|r| r["kind"] == "remote" && r["body"]["subject"] == "nobody home" && r["body"]["result"]["status"] == "rejected")
    });

    // Human-readable, with the peer's escape sequence stripped.
    let txt = String::from_utf8(e.run("n1", &["log", "--body"]).stdout).unwrap();
    assert!(txt.contains("backend@pc-one → reviewer@pc-two  request  \"PR 12[2J\"  (via server)"), "{txt}");
    assert!(txt.contains("✓✓ handled by backend@pc-one") && txt.contains("approve PR 12?"), "{txt}");
    assert!(txt.contains("↗ the server took it for 1 recipient(s)") && txt.contains("✗ the server rejected it  \"nobody home\""), "{txt}");
    assert!(!txt.contains('\u{1b}'), "no escape sequences reach the terminal: {txt:?}");

    // Filters.
    let only = log_rows(&e, "n2", &["--json", "--thread", &thread]);
    assert!(!only.is_empty() && only.iter().all(|r| r["threadId"] == thread.as_str()), "{only:?}");
    let about = log_rows(&e, "n1", &["--json", "--agent", "reviewer"]);
    assert!(!about.is_empty() && about.iter().all(|r| r.to_string().contains("reviewer")), "{about:?}");
    assert_eq!(log_rows(&e, "n1", &["--json", "-n", "2"]).len(), 2);

    // The server's network view: both messages with each recipient's delivery state.
    wait_until("network view", T, || {
        let rows = log_rows(&e, "hub", &["--network", "--json"]);
        rows.iter().any(|r| r["kind"] == "message" && r["msgId"] == qid.as_str() && r["body"]["recipients"][0] == "reviewer@pc-two")
            && rows.iter().any(|r| r["kind"] == "delivery" && r["msgId"] == qid.as_str() && r["body"]["state"] == "acked")
            && rows.iter().any(|r| r["kind"] == "roster")
    });
    let not_server = e.cmd("n1", &["log", "--network"]).output().unwrap();
    assert!(!not_server.status.success() && String::from_utf8_lossy(&not_server.stderr).contains("isn't the server"));

    // Follow: new activity shows up while it runs.
    let mut f = e.cmd("n1", &["log", "--follow", "--json", "-n", "1"]).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let out = f.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel::<Value>();
    std::thread::spawn(move || {
        for l in BufReader::new(out).lines().map_while(Result::ok) {
            if let Ok(v) = serde_json::from_str::<Value>(&l) {
                let _ = tx.send(v);
            }
        }
    });
    std::thread::sleep(Duration::from_millis(700));
    back.tool("send", serde_json::json!({"to": "@reviewer@pc-two", "subject": "followed", "text": "hi"}));
    let t0 = Instant::now();
    let mut found = false;
    while t0.elapsed() < T {
        if let Ok(v) = rx.recv_timeout(Duration::from_millis(500))
            && v["kind"] == "sent"
            && v["body"]["subject"] == "followed"
        {
            found = true;
            break;
        }
    }
    let _ = f.kill();
    let _ = f.wait();
    assert!(found, "--follow printed the new send");
}

/// After an upgrade the old node keeps running. A newer adapter restarts it on the new
/// binary when nothing uses it, only reports it when a session does, and leaves a newer
/// node alone.
#[test]
fn outdated_node_is_restarted_only_when_idle() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code"]);
    e.run("n1", &["agent", "add", "ci-bot", "--role", "ci", "--runner", "script"]);
    let me = env!("CARGO_PKG_VERSION");
    let note = |w: &Value| w["structuredContent"]["nodeNote"].as_str().unwrap_or("").to_string();

    // 1. Older and idle: restarted on this binary.
    let old = e.start_node_with("n1", &[("AGENTLAN_DEV_NODE_VERSION", "0.0.1")]);
    assert_eq!(e.status("n1")["version"], "0.0.1");
    let ai = e.kids.len();
    let mut a = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    a.init();
    let w = a.tool("whoami", serde_json::json!({}));
    if cfg!(windows) && note(&w).contains("can't start a node that outlives it") {
        // This host's job object forbids breakaway (some CI runners): correctly left alone.
        assert_eq!(e.status("n1")["version"], "0.0.1");
        e.kill(ai);
        e.kill(old);
    } else {
        assert!(note(&w).contains("Updated this PC's AgentLAN node") && note(&w).contains("0.0.1"), "{w}");
        assert_eq!(e.status("n1")["version"], me, "the node now runs this binary");
        assert!(e.kids[old].try_wait().unwrap().is_some(), "the old node process exited");
        let w2 = a.tool("whoami", serde_json::json!({}));
        assert_eq!(note(&w2), "", "the update note is shown once: {w2}");
        e.kill(ai);
        e.run("n1", &["node", "stop"]); // the restarted node isn't one of the harness's children
    }

    // 2. Older, but a session is attached: left running and reported.
    let old2 = e.start_node_with("n1", &[("AGENTLAN_DEV_NODE_VERSION", "0.0.1")]);
    let _bot = e.stream("n1", &["attach", "ci-bot", "--json"]);
    wait_until("ci-bot attached", T, || e.status("n1")["sessions"].as_array().is_some_and(|a| a.len() == 1));
    let bi = e.kids.len();
    let mut b = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    b.init();
    let w = b.tool("whoami", serde_json::json!({}));
    assert!(note(&w).contains("still running 0.0.1") && note(&w).contains("agentlan node stop"), "{w}");
    assert_eq!(e.status("n1")["version"], "0.0.1", "not restarted under a session");
    let txt = String::from_utf8(e.run("n1", &["status"]).stdout).unwrap();
    assert!(txt.contains("node 0.0.1") && txt.contains("still running 0.0.1"), "{txt}");
    // The user follows the note (stops the old node; the adapter's next call starts a
    // current one): the note goes away instead of going stale.
    e.kill(bi - 1); // the attach stream
    e.run("n1", &["node", "stop"]);
    let w = b.tool("whoami", serde_json::json!({}));
    assert_eq!(e.status("n1")["version"], me, "{w}");
    assert_eq!(note(&w), "", "no stale note after the node was updated: {w}");
    e.kill(bi);
    e.run("n1", &["node", "stop"]);
    let _ = old2;

    // 2b. Older, no session, but a script is waiting on it (`agentlan wait` doesn't attach).
    let old3 = e.start_node_with("n1", &[("AGENTLAN_DEV_NODE_VERSION", "0.0.1")]);
    let wi = e.spawn("n1", &["wait", "--as", "ci-bot", "--timeout", "30"]);
    wait_until("the script's wait is registered", T, || e.status("n1")["waiters"] == 1);
    let di = e.kids.len();
    let mut d = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    d.init();
    let w = d.tool("whoami", serde_json::json!({}));
    // (A host that forbids breakaway, like some Windows CI runners, stops even earlier.)
    let cant = cfg!(windows) && note(&w).contains("can't start a node that outlives it");
    assert!(note(&w).contains("is in use") || cant, "{w}");
    assert_eq!(e.status("n1")["version"], "0.0.1", "not restarted under a waiting script");
    assert!(e.kids[wi].try_wait().unwrap().is_none(), "the script is still waiting");
    e.kill(di);
    e.kill(wi);
    e.kill(old3);

    // 3. Newer than this binary: left alone, nothing to report.
    let newer = e.start_node_with("n1", &[("AGENTLAN_DEV_NODE_VERSION", "99.0.0")]);
    let ci = e.kids.len();
    let mut c = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    c.init();
    let w = c.tool("whoami", serde_json::json!({}));
    assert_eq!(note(&w), "", "{w}");
    assert_eq!(e.status("n1")["version"], "99.0.0");
    e.kill(ci);
    e.kill(newer);
}

/// A node inside `agentlan up` is never stopped by a newer adapter (that would leave a new
/// node next to an old server): it's reported, with the restart to do.
#[test]
fn outdated_node_inside_up_is_only_reported() {
    let mut e = Env::new();
    let addr = e.addr();
    let up = e
        .cmd("srv", &["up", "--name", "e2e", "--listen", &addr, "--advertise", &addr, "--alias", "pc-srv"])
        .env("AGENTLAN_DEV_NODE_VERSION", "0.0.1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    e.kids.push(up);
    let srv = e.home("srv");
    wait_until("server node up", T, || e.cmd_ok(&srv));
    let st = e.status("srv");
    assert_eq!((st["version"].as_str(), st["withHub"].as_bool()), (Some("0.0.1"), Some(true)), "{st}");
    e.run("srv", &["agent", "add", "lead", "--role", "lead", "--runner", "claude-code"]);
    let mut a = Mcp::spawn(&mut e, "srv", "lead", "claude-code");
    a.init();
    let w = a.tool("whoami", serde_json::json!({}));
    let note = w["structuredContent"]["nodeNote"].as_str().unwrap_or("");
    assert!(note.contains("restart `agentlan up`"), "{w}");
    assert_eq!(e.status("srv")["version"], "0.0.1", "left running");
}

/// One raw HTTP/1.1 exchange: (status line, headers, body).
fn http(port: u16, req: &str) -> (String, String, String) {
    use std::io::{Read, Write};
    let mut c = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.write_all(req.as_bytes()).unwrap();
    let mut out = String::new();
    let _ = c.read_to_string(&mut out);
    let (head, body) = out.split_once("\r\n\r\n").unwrap_or((&out, ""));
    let (status, headers) = head.split_once("\r\n").unwrap_or((head, ""));
    (status.to_string(), headers.to_string(), body.to_string())
}

/// The dashboard: served on 127.0.0.1 only, data only with the access key, only for its
/// own Host (no DNS rebinding), GET only, under a strict CSP.
#[test]
fn dashboard_serves_only_this_pc() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code"]);
    e.start_node("n1");
    let mut d = e.cmd("n1", &["dashboard"]).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    let mut first = String::new();
    BufReader::new(d.stdout.take().unwrap()).read_line(&mut first).unwrap();
    e.kids.push(d);
    let url = first.split_whitespace().find(|w| w.starts_with("http://127.0.0.1:")).expect("printed a link").to_string();
    let (addr, key) = url.trim_start_matches("http://").split_once("/#").unwrap();
    let port: u16 = addr.rsplit(':').next().unwrap().parse().unwrap();
    let get = |path: &str, host: &str, auth: Option<&str>| {
        let a = auth.map(|k| format!("Authorization: Bearer {k}\r\n")).unwrap_or_default();
        http(port, &format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n{a}\r\n"))
    };
    let (st, hd, body) = get("/", addr, None);
    assert!(st.contains("200") && hd.contains("Content-Security-Policy: default-src 'none'; script-src 'self'") && body.contains("/app.js"), "{st}\n{hd}");
    assert!(get("/app.js", addr, None).2.contains("textContent"), "the page script");
    assert!(get("/api/activity", addr, None).0.contains("401"), "no key, no data");
    assert!(get("/api/activity", addr, Some("0000")).0.contains("401"), "wrong key");
    assert!(get("/api/activity", &format!("evil.example:{port}"), Some(key)).0.contains("403"), "foreign Host");
    let (st, _, _) = http(port, &format!("POST /api/activity HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {key}\r\nContent-Length: 0\r\n\r\n"));
    assert!(st.contains("405"), "{st}");
    let mut b = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    b.init();
    let (st, _, body) = get("/api/activity?limit=50", addr, Some(key));
    let v: Value = serde_json::from_str(&body).unwrap();
    assert!(st.contains("200") && v["rows"].as_array().is_some_and(|r| r.iter().any(|r| r["kind"] == "node")) && v["cursor"].as_i64().unwrap() > 0, "{body}");
    wait_until("agents via the dashboard", T, || {
        let (st, _, body) = get("/api/agents", addr, Some(key));
        st.contains("200") && body.contains("backend@pc-one")
    });
    let (st, _, body) = get("/api/status", addr, Some(key));
    assert!(st.contains("200") && body.contains("\"network\":false") && body.contains("pc-one"), "{body}");
}
