# AgentLAN

[![CI](https://github.com/jenn0pal/agentlan/actions/workflows/ci.yml/badge.svg)](https://github.com/jenn0pal/agentlan/actions/workflows/ci.yml)

**A LAN intercom for AI coding agents.** AgentLAN links Claude Code, Codex and your own scripts running on different Mac and Windows PCs, so they can find each other and talk. Every agent has a recorded identity, a role and a project, and the network announces it when the agent connects. Agents on the same PC talk directly through a local daemon, without touching the network.

> **Status: alpha (v0.1).**
> - **Built and tested on macOS:** the core system and the MCP adapter.
> - **Windows:** CI builds it and runs the test suite there, including the single-machine end-to-end tests, and the release ships a Windows binary.
> - **Not yet tested across two physical machines.** End-to-end tests run every process on one machine over loopback.
>
> Expect breaking changes before 1.0.

**Quick start**

1. Install on every PC:
   ```sh
   curl -fsSL https://raw.githubusercontent.com/jenn0pal/agentlan/main/install.sh | sh
   ```
   On Windows: `irm https://raw.githubusercontent.com/jenn0pal/agentlan/main/install.ps1 | iex`
2. Run `agentlan up` on the PC you pick as the server.
3. Run the `agentlan connect …` line it prints on every other PC.

Details are in [Install](#install-one-line) and [Run it](#run-it-one-command-per-pc).

## Features

**Connecting**
- **One Rust binary.** Any PC can be the server. Every PC runs a small daemon, and each AI agent gets an MCP adapter.
- **Identity and roster.** Each agent has a permanent `agt_…` id, a role, a project, a runner and its machine. A newcomer receives the roster, its own project first, with same-machine peers flagged.
- **Machine details.** The registry records each PC's OS, CPU, RAM and addresses (shared by default; set `"shareHardware": false` in `node/node.json` to turn it off), so agents can choose peers sensibly.

**Messaging**
- **Same-PC routing.** Messages between agents on one PC go through the local daemon. They never cross the network and keep working when the server is down.
- **Durable cross-PC messaging.**
  - Messages are stored and forwarded, and offline mail waits up to 24 hours.
  - Delivery is in order per sender.
  - Retry keys stop retries from duplicating messages.
  - A queued outbox drains when the server comes back.
- **AMF/1, one runner-agnostic message format.** Its parts use the same shape as the [A2A](https://a2a-protocol.org) protocol. Small translators at the edge render it per runner:
  - Claude Code sees peer messages wrapped and marked untrusted;
  - Codex gets JSON;
  - scripts use the CLI with JSON in and out.

**Security and networking**
- **Encrypted, authenticated links.** Every link uses the [Noise](https://noiseprotocol.org) XX handshake with a pinned server key and one-time join codes.
- **Tailscale-aware.** `hub invite --tailscale` advertises this PC's MagicDNS name, so agents can link across sites over your tailnet.

## How it works

```
  PC-A (macOS)                           PC-B (any PC: server)               PC-C (Windows)
┌───────────────────────────┐          ┌────────────────────────┐          ┌───────────────────────────┐
│ Claude Code     Codex     │          │ agentlan hub           │          │ Codex          script     │
│  └ agentlan mcp └ agentlan mcp       │  registry · presence   │          │  └ agentlan mcp └ CLI JSON│
│        │ local IPC │      │  Noise   │  routing · mailboxes   │  Noise   │        │ local IPC │      │
│   ┌──────────────────┐    │  over    └───────────▲────────────┘  over    │   ┌──────────────────┐    │
│   │  agentlan node   │────┼──── TCP ─────────────┴───────────── TCP ─────┼───│  agentlan node   │    │
│   │  (local router)  │    │                                              │   │  (local router)  │    │
│   └──────────────────┘    │                                              │   └──────────────────┘    │
└───────────────────────────┘                                              └───────────────────────────┘
   same-PC messages stay inside the daemon; cross-PC messages go node → hub → node
```

For the full design, see [`PROPOSAL.md`](PROPOSAL.md). For an animated walkthrough, open [`process-flow.html`](process-flow.html) in a browser.

## Install (one line)

**macOS / Linux**
```sh
curl -fsSL https://raw.githubusercontent.com/jenn0pal/agentlan/main/install.sh | sh
```

**Windows (PowerShell)**
```powershell
irm https://raw.githubusercontent.com/jenn0pal/agentlan/main/install.ps1 | iex
```

The installer downloads the prebuilt binary for your machine from [Releases](https://github.com/jenn0pal/agentlan/releases) and verifies its SHA-256 checksum. Then:
- **macOS / Linux:** it installs into `~/.local/bin`. If that folder isn't on your PATH yet, it prints the one line to add it.
- **Windows:** it installs into `%LOCALAPPDATA%\Programs\agentlan` and adds that to your user PATH. New terminals pick it up.

Prebuilt binaries cover macOS (Apple Silicon and Intel), Linux (x64 and ARM64; static, so any distro) and Windows x64.

**Options:**
- **Pin a version:** `AGENTLAN_VERSION=v0.1.0`
- **Choose where it goes:** `AGENTLAN_INSTALL_DIR=…`
- **Build from source instead:** `AGENTLAN_FROM_SOURCE=1` (both installers; needs Rust)

**From source (Rust 1.89 or newer):** `cargo install --locked --git https://github.com/jenn0pal/agentlan agentlan`

## Run it (one command per PC)

**1. On the PC you pick as the server** (any Mac or Windows PC):
```sh
agentlan up
```
It sets everything up the first time, starts the server and this PC's node, and prints a line like:
```
  agentlan connect AGENTLAN1-eyJuZXQiOi…
```

**2. On every other PC**, paste that line:
```sh
agentlan connect AGENTLAN1-eyJuZXQiOi…
```
After the first time, plain `agentlan connect` starts the node again. Over Tailscale, use `agentlan up --tailscale` to advertise the server's MagicDNS name.

**3. Add an agent and talk:**
```sh
agentlan agent add backend --role backend --runner claude-code    # on any PC; picked up live
agentlan ls                                                       # who's online: roles, projects, machines
agentlan send --as backend @reviewer@win-pc "PR 12 is ready"
agentlan inbox --as reviewer
```

**Keep it running.** Leave `agentlan up` / `agentlan connect` running in a terminal. On macOS and Linux you can also background it with `nohup agentlan up > ~/agentlan.log 2>&1 &`. Installing it as a login service comes in Phase 3.

**If the server is a Windows PC**, allow `agentlan` through Windows Firewall when prompted (TCP 47820, Private networks). The other PCs only connect out.

<details>
<summary>Advanced: the individual commands behind <code>up</code> / <code>connect</code></summary>

```sh
# server PC
agentlan hub init --name home-lab && agentlan hub run    # server only
agentlan hub invite [--tailscale | --addr HOST:PORT]     # one-time join code per PC (15 min)
# each PC
agentlan join AGENTLAN1-… && agentlan node run           # node only
```
</details>

### Build and test

```sh
cargo build --release      # → target/release/agentlan
cargo test --workspace     # unit tests + end-to-end tests (real processes on 127.0.0.1)
```

## Connect your AI agents (MCP)

```sh
# Claude Code (per project)
claude mcp add -s local agentlan -- agentlan mcp --agent backend --runner claude-code
# Codex CLI (picks the profile from runner + git remote)
codex mcp add agentlan -- agentlan mcp --agent auto --runner codex
```

**Tools:** `whoami`, `list_agents`, `list_machines`, `send`, `reply`, `inbox`, `wait_for_message`, `ack`.

**Scripts and other agents** use the CLI:

```sh
agentlan attach ci-bot --json &        # announce + stream events as JSON lines
echo '{"to":{"kind":"role","role":"qa"},"intent":"status","subject":"CI green"}' \
  | agentlan send --as ci-bot --stdin --json
agentlan wait --as ci-bot --json
```

## Over Tailscale

AgentLAN works across a tailnet: the same PCs, or PCs at different sites, with no ports opened to the internet.

- **Choosing addresses.** `hub invite --tailscale` advertises only the MagicDNS name and the 100.x address. The default `hub invite` lists LAN addresses first, then Tailscale, and skips VM/Docker bridges, WSL/Hyper-V switches and other VPN tunnels. The chosen addresses are printed to stderr.
- **Tailscale access rules** must allow TCP 47820 to the server PC. The default allow-all policy does.

## Security model

- **Network membership.** PCs join with a one-time code that contains the server's full public key, so a fake server is rejected during the handshake. The server then pins each PC's key.
- **Peer messages are untrusted data.** They are never instructions. The protocol has no "execute" verb. Adapters mark peer content as untrusted, and a peer cannot close Claude's wrapper early.
- **Auto-wake is off by default.** Agents can't start work in another agent's session unless that profile is a dedicated worker. See §7.5 of the proposal for the impact analysis.
- **Same OS user.** On macOS and Linux, the local IPC socket lives in a private (0700) directory, so only your user can reach it. On Windows, restricting the named pipe to the current user is still to do. Any process running as your user can act as your agents.

## Configuration

| | |
|---|---|
| **Data directory** | The per-user app-data folder by default (`--home DIR` or `AGENTLAN_HOME` overrides it). It holds `hub/` (`hub.key`, `hub.json`, `hub.db`) and `node/` (`node.key`, `node.json`, `profiles.json`, `node.db`, plus the IPC socket). |
| **Environment variables** | `AGENTLAN_LOG` (log level), `AGENTLAN_HEARTBEAT_MS`, `AGENTLAN_LEASE_MS`, `AGENTLAN_BACKOFF_MAX_MS` |

## Project layout

| Crate | Purpose |
|---|---|
| `agentlan-proto` | AMF/1 message format, registry records, wire frames, IPC protocol, join codes (no I/O) |
| `agentlan-net` | Noise XX handshake and chunked, encrypted framing |
| `agentlan-store` | SQLite helpers (WAL, `synchronous=FULL`) |
| `agentlan-hub` | Server: registry, sequenced events, fenced sessions, transactional routing, in-order delivery pump |
| `agentlan-node` | Per-PC daemon: local router, durable inbox/outbox, server link, interface classification |
| `agentlan-mcp` | MCP stdio adapter ([rmcp](https://crates.io/crates/rmcp)) with per-runner translators |
| `agentlan` | The single binary: CLI, `hub`, `node`, `mcp` |

## Roadmap

**Phase 2 (in progress)**
- Request/reply that waits for the answer.
- Claude Code hooks.
- Opt-in wake for worker agents.
- Loop-guard budgets.
- Tests for role and project broadcasts.

**Phase 3**
- Background services: macOS LaunchAgent and Windows startup task.
- mDNS discovery.
- Moving the server to another PC, and server backups.
- `doctor` command and a terminal dashboard.
- Installers and code signing.

**Still to verify:** Windows builds, and a real test across two machines (LAN and Tailscale).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
