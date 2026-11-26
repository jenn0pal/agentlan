//! End-to-end tests that drive the real `ailink` binary as separate processes
//! on 127.0.0.1 (Milestone 1 exit criteria).

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_ailink");
static N: AtomicU32 = AtomicU32::new(0);

struct Env {
    root: PathBuf,
    port: u16,
    kids: Vec<Child>,
}

impl Env {
    fn new() -> Env {
        // Short path: Unix socket paths are limited to ~104 bytes on macOS.
        let root = PathBuf::from(format!("/tmp/ailk-{}-{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst)));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        Env { root, port, kids: vec![] }
    }

    fn home(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn cmd(&self, home: &str, args: &[&str]) -> Command {
        let mut c = Command::new(BIN);
        c.arg("--home").arg(self.home(home)).args(args);
        c.env("AILINK_HEARTBEAT_MS", "300").env("AILINK_LEASE_MS", "2000").env("AILINK_BACKOFF_MAX_MS", "400").env("AILINK_LOG", "warn");
        c
    }

    fn run(&self, home: &str, args: &[&str]) -> Output {
        let out = self.cmd(home, args).output().unwrap();
        assert!(out.status.success(), "ailink {args:?} failed: {}", String::from_utf8_lossy(&out.stderr));
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

    fn start_node(&mut self, home: &str) {
        self.spawn(home, &["node", "run"]);
        let h = self.home(home);
        wait_until("node IPC up", Duration::from_secs(10), || self.cmd_ok(&h));
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

    let big = "x".repeat(300 * 1024);
    let out = e.cmd("n1", &["send", "--as", "backend", "@qa@pc-one", &big]).output().unwrap();
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
A="$AILINK_BIN --home $N1"
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
        .env("AILINK_BIN", BIN)
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
