//! Static X25519 keys, fingerprints and machine ids.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use data_encoding::BASE32_NOPAD;
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct StaticKey {
    pub private: Vec<u8>,
    pub public: Vec<u8>,
}

impl StaticKey {
    pub fn generate() -> StaticKey {
        let kp = snow::Builder::new(super::params()).generate_keypair().expect("keypair");
        StaticKey { private: kp.private, public: kp.public }
    }

    /// Load `<path>` (two base32 lines: private, public) or create it with
    /// owner-only permissions.
    pub fn load_or_create(path: &Path) -> Result<StaticKey> {
        if path.exists() {
            return Self::load(path);
        }
        let k = Self::generate();
        let body = format!("{}\n{}\n", b32(&k.private), b32(&k.public));
        write_private(path, body.as_bytes())?;
        Ok(k)
    }

    pub fn load(path: &Path) -> Result<StaticKey> {
        let s = std::fs::read_to_string(path).with_context(|| format!("reading key {}", path.display()))?;
        let mut lines = s.lines();
        let private = from_b32(lines.next().ok_or_else(|| anyhow!("empty key file"))?)?;
        let public = from_b32(lines.next().ok_or_else(|| anyhow!("key file missing public key"))?)?;
        Ok(StaticKey { private, public })
    }
}

/// Write a file readable only by the current user (0600 on Unix). On Windows the
/// per-user app-data directory ACL applies (explicit ACLs: Phase 0 item).
pub fn write_private(path: &Path, data: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
        f.write_all(data)?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, data)?;
    }
    Ok(())
}

pub fn b32(bytes: &[u8]) -> String {
    BASE32_NOPAD.encode(bytes)
}

pub fn from_b32(s: &str) -> Result<Vec<u8>> {
    BASE32_NOPAD.decode(s.trim().as_bytes()).map_err(|e| anyhow!("bad base32: {e}"))
}

fn digest_b32(public: &[u8]) -> String {
    b32(&Sha256::digest(public))
}

/// Human-readable fingerprint, e.g. `SHA256:QM9VLH2K…`.
pub fn fingerprint(public: &[u8]) -> String {
    format!("SHA256:{}", &digest_b32(public)[..20])
}

/// Machine id derived from the machine's *first* public key (fixed at enrolment).
pub fn machine_id(public: &[u8]) -> String {
    format!("mch_{}", &digest_b32(public)[..16])
}
