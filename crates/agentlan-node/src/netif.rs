//! Which of this machine's addresses other PCs can actually use to reach it.
//!
//! - **Lan**: physical Wi-Fi / Ethernet.
//! - **Tailscale**: the tailnet (100.64.0.0/10), plus the MagicDNS name when the
//!   Tailscale CLI is available. Stable across networks, works beyond the LAN.
//! - **Other**: virtual bridges (VMs, Docker, WSL/Hyper-V), other VPN tunnels and
//!   link-local addresses. Unreachable or unintended for peers; excluded by default.

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::process::Command;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddrKind {
    Lan,
    Tailscale,
    Other,
}

#[derive(Clone, Debug)]
pub struct LocalAddr {
    pub iface: String,
    pub ip: Ipv4Addr,
    pub kind: AddrKind,
}

/// Tailscale's CGNAT range: 100.64.0.0/10.
pub fn is_tailnet(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    o[0] == 100 && (64..=127).contains(&o[1])
}

const VIRTUAL_PREFIXES: &[&str] = &[
    // macOS: VM / Internet Sharing bridges, other VPN tunnels, AirDrop/peer links
    "bridge", "utun", "awdl", "llw", "anpi", "gif", "stf", "ap",
    // Linux: containers, VMs, VPNs
    "docker", "br-", "veth", "virbr", "vmnet", "vboxnet", "lxc", "lxd", "cni", "flannel", "zt", "wg", "tun", "tap",
    // Windows: Hyper-V / WSL switches, VM host-only adapters
    "vethernet", "virtualbox", "vmware", "hyper-v", "wsl",
];

/// Classify one interface address (pure; unit-tested).
pub fn classify(iface: &str, ip: Ipv4Addr) -> AddrKind {
    if ip.is_loopback() || ip.is_link_local() || ip.is_unspecified() {
        return AddrKind::Other;
    }
    if is_tailnet(ip) {
        return AddrKind::Tailscale;
    }
    let name = iface.to_ascii_lowercase();
    if name.contains("tailscale") {
        return AddrKind::Tailscale;
    }
    if VIRTUAL_PREFIXES.iter().any(|p| name.starts_with(p)) {
        return AddrKind::Other;
    }
    AddrKind::Lan
}

/// All IPv4 addresses of this machine, classified (LAN first, then Tailscale, then other).
pub fn local_addrs() -> Vec<LocalAddr> {
    let mut v: Vec<LocalAddr> = if_addrs::get_if_addrs()
        .map(|list| {
            list.into_iter()
                .filter_map(|i| match i.ip() {
                    IpAddr::V4(ip) => Some(LocalAddr { kind: classify(&i.name, ip), iface: i.name, ip }),
                    IpAddr::V6(_) => None,
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort_by_key(|a| match a.kind {
        AddrKind::Lan => 0,
        AddrKind::Tailscale => 1,
        AddrKind::Other => 2,
    });
    v.dedup_by(|a, b| a.ip == b.ip);
    v
}

#[derive(Clone, Debug, PartialEq)]
pub struct TailscaleSelf {
    /// MagicDNS name without the trailing dot, e.g. `studio-mbp.tail1234.ts.net`.
    pub dns_name: Option<String>,
    pub ipv4: Vec<Ipv4Addr>,
}

fn cli_candidates() -> Vec<PathBuf> {
    let mut c: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).map(|d| d.join(if cfg!(windows) { "tailscale.exe" } else { "tailscale" })).collect())
        .unwrap_or_default();
    c.push("/Applications/Tailscale.app/Contents/MacOS/Tailscale".into());
    c.push("/opt/homebrew/bin/tailscale".into());
    c.push("/usr/local/bin/tailscale".into());
    c.push(r"C:\Program Files\Tailscale\tailscale.exe".into());
    c
}

/// Parse `tailscale status --json` (pure; unit-tested). None unless the backend is running.
pub fn parse_status(json: &str) -> Option<TailscaleSelf> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    if v["BackendState"].as_str() != Some("Running") {
        return None;
    }
    let me = &v["Self"];
    let dns_name = me["DNSName"].as_str().map(|s| s.trim_end_matches('.').to_string()).filter(|s| !s.is_empty());
    let ipv4 = me["TailscaleIPs"].as_array().map(|a| a.iter().filter_map(|x| x.as_str()?.parse::<Ipv4Addr>().ok()).collect()).unwrap_or_default();
    Some(TailscaleSelf { dns_name, ipv4 })
}

/// Ask the local Tailscale CLI for this machine's MagicDNS name and tailnet IPs.
pub fn tailscale_self() -> Option<TailscaleSelf> {
    for cli in cli_candidates() {
        if !cli.is_file() {
            continue;
        }
        let out = Command::new(&cli).args(["status", "--json"]).output().ok()?;
        if out.status.success() {
            return parse_status(&String::from_utf8_lossy(&out.stdout));
        }
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InviteScope {
    /// LAN addresses first, then Tailscale (if present). Virtual adapters excluded.
    Default,
    /// Tailscale only: MagicDNS name first, then tailnet IPs.
    Tailscale,
}

/// Addresses to put in a join code, most preferred first.
pub fn invite_addrs(port: u16, scope: InviteScope) -> Vec<String> {
    let addrs = local_addrs();
    let ts = tailscale_self();
    let mut tailnet: Vec<String> = Vec::new();
    if let Some(name) = ts.as_ref().and_then(|t| t.dns_name.clone()) {
        tailnet.push(format!("{name}:{port}"));
    }
    let mut tail_ips: Vec<Ipv4Addr> = ts.map(|t| t.ipv4).unwrap_or_default();
    tail_ips.extend(addrs.iter().filter(|a| a.kind == AddrKind::Tailscale).map(|a| a.ip));
    for ip in tail_ips {
        let s = format!("{ip}:{port}");
        if !tailnet.contains(&s) {
            tailnet.push(s);
        }
    }
    match scope {
        InviteScope::Tailscale => tailnet,
        InviteScope::Default => {
            let mut v: Vec<String> = addrs.iter().filter(|a| a.kind == AddrKind::Lan).map(|a| format!("{}:{port}", a.ip)).collect();
            v.extend(tailnet);
            v
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    #[test]
    fn classification() {
        assert_eq!(classify("en0", ip("192.168.1.23")), AddrKind::Lan);
        assert_eq!(classify("Wi-Fi", ip("10.0.0.5")), AddrKind::Lan);
        assert_eq!(classify("Ethernet", ip("192.168.1.31")), AddrKind::Lan);
        assert_eq!(classify("utun5", ip("100.93.12.7")), AddrKind::Tailscale);
        assert_eq!(classify("Tailscale", ip("100.101.2.3")), AddrKind::Tailscale);
        assert_eq!(classify("tailscale0", ip("100.64.0.1")), AddrKind::Tailscale);
        assert_eq!(classify("bridge100", ip("192.168.64.1")), AddrKind::Other);
        assert_eq!(classify("docker0", ip("172.17.0.1")), AddrKind::Other);
        assert_eq!(classify("vEthernet (WSL)", ip("172.20.0.1")), AddrKind::Other);
        assert_eq!(classify("utun3", ip("10.8.0.2")), AddrKind::Other, "other VPN tunnels are excluded");
        assert_eq!(classify("en0", ip("169.254.10.1")), AddrKind::Other);
        assert!(!is_tailnet(ip("100.63.255.255")) && !is_tailnet(ip("100.128.0.0")));
    }

    #[test]
    fn tailscale_status_parsing() {
        let j = r#"{"BackendState":"Running","Self":{"DNSName":"studio-mbp.tail1234.ts.net.","TailscaleIPs":["100.93.1.2","fd7a:115c::1"]}}"#;
        let t = parse_status(j).unwrap();
        assert_eq!(t.dns_name.as_deref(), Some("studio-mbp.tail1234.ts.net"));
        assert_eq!(t.ipv4, vec![ip("100.93.1.2")]);
        assert!(parse_status(r#"{"BackendState":"Stopped","Self":{}}"#).is_none());
    }
}
