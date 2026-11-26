//! Noise XX handshake and the encrypted, chunked frame transport used between
//! nodes and the hub.
//!
//! Layers: TCP → Noise transport messages `[u16 BE len][ciphertext ≤ 65535]` →
//! application frames `[u32 BE len][JSON]` (≤ `MAX_FRAME`), split across as many
//! Noise messages as needed (plaintext ≤ 65519 bytes each).

pub mod keys;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use snow::StatelessTransportState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use ailink_proto::MAX_FRAME;

pub const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
const NOISE_MAX: usize = 65535;
const TAG: usize = 16;
const PLAIN_CHUNK: usize = NOISE_MAX - TAG;
const HANDSHAKE_MAX: usize = 1024;
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

fn params() -> snow::params::NoiseParams {
    NOISE_PARAMS.parse().expect("valid noise params")
}

async fn write_hs(s: &mut TcpStream, msg: &[u8]) -> Result<()> {
    s.write_all(&(msg.len() as u16).to_be_bytes()).await?;
    s.write_all(msg).await?;
    Ok(())
}

async fn read_hs(s: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len = [0u8; 2];
    s.read_exact(&mut len).await?;
    let n = u16::from_be_bytes(len) as usize;
    if n > HANDSHAKE_MAX {
        bail!("handshake message too large ({n} bytes)");
    }
    let mut buf = vec![0u8; n];
    s.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Run the initiator side (the node). If `expected_remote` is given, the hub's
/// static key is checked after message 2 — before our own static key is sent.
pub async fn connect_initiator(
    mut stream: TcpStream,
    local_private: &[u8],
    expected_remote: Option<&[u8]>,
) -> Result<(FrameReader, FrameWriter, Vec<u8>)> {
    let fut = async {
        let mut hs = snow::Builder::new(params()).local_private_key(local_private)?.build_initiator()?;
        let mut buf = vec![0u8; HANDSHAKE_MAX];
        let mut payload = vec![0u8; HANDSHAKE_MAX];
        let n = hs.write_message(&[], &mut buf)?;
        write_hs(&mut stream, &buf[..n]).await?;
        let m2 = read_hs(&mut stream).await?;
        hs.read_message(&m2, &mut payload)?;
        let remote = hs.get_remote_static().ok_or_else(|| anyhow!("hub sent no static key"))?.to_vec();
        if let Some(exp) = expected_remote {
            if exp != remote.as_slice() {
                bail!(
                    "HUB_KEY_MISMATCH: server key {} does not match the pinned key {}",
                    keys::fingerprint(&remote),
                    keys::fingerprint(exp)
                );
            }
        }
        let n = hs.write_message(&[], &mut buf)?;
        write_hs(&mut stream, &buf[..n]).await?;
        let ts = hs.into_stateless_transport_mode()?;
        Ok::<_, anyhow::Error>((ts, remote))
    };
    let (ts, remote) = tokio::time::timeout(HANDSHAKE_TIMEOUT, fut).await.context("handshake timed out")??;
    let (r, w) = split(stream, ts);
    Ok((r, w, remote))
}

/// Run the responder side (the hub). Returns the peer's static public key; the
/// caller decides whether that key is enrolled.
pub async fn accept_responder(mut stream: TcpStream, local_private: &[u8]) -> Result<(FrameReader, FrameWriter, Vec<u8>)> {
    let fut = async {
        let mut hs = snow::Builder::new(params()).local_private_key(local_private)?.build_responder()?;
        let mut buf = vec![0u8; HANDSHAKE_MAX];
        let mut payload = vec![0u8; HANDSHAKE_MAX];
        let m1 = read_hs(&mut stream).await?;
        hs.read_message(&m1, &mut payload)?;
        let n = hs.write_message(&[], &mut buf)?;
        write_hs(&mut stream, &buf[..n]).await?;
        let m3 = read_hs(&mut stream).await?;
        hs.read_message(&m3, &mut payload)?;
        let remote = hs.get_remote_static().ok_or_else(|| anyhow!("node sent no static key"))?.to_vec();
        let ts = hs.into_stateless_transport_mode()?;
        Ok::<_, anyhow::Error>((ts, remote))
    };
    let (ts, remote) = tokio::time::timeout(HANDSHAKE_TIMEOUT, fut).await.context("handshake timed out")??;
    let (r, w) = split(stream, ts);
    Ok((r, w, remote))
}

fn split(stream: TcpStream, ts: StatelessTransportState) -> (FrameReader, FrameWriter) {
    let _ = stream.set_nodelay(true);
    let ts = Arc::new(ts);
    let (rd, wr) = stream.into_split();
    (
        FrameReader { rd, ts: ts.clone(), nonce: 0, plain: Vec::new(), cipher: vec![0u8; NOISE_MAX] },
        FrameWriter { wr, ts, nonce: 0, cipher: vec![0u8; NOISE_MAX] },
    )
}

pub struct FrameWriter {
    wr: OwnedWriteHalf,
    ts: Arc<StatelessTransportState>,
    nonce: u64,
    cipher: Vec<u8>,
}

impl FrameWriter {
    pub async fn send_bytes(&mut self, frame: &[u8]) -> Result<()> {
        if frame.len() > MAX_FRAME {
            bail!("frame of {} bytes exceeds MAX_FRAME", frame.len());
        }
        let mut plain = Vec::with_capacity(frame.len() + 4);
        plain.extend_from_slice(&(frame.len() as u32).to_be_bytes());
        plain.extend_from_slice(frame);
        let mut out = Vec::with_capacity(plain.len() + (plain.len() / PLAIN_CHUNK + 1) * (TAG + 2));
        for chunk in plain.chunks(PLAIN_CHUNK) {
            let n = self.ts.write_message(self.nonce, chunk, &mut self.cipher)?;
            self.nonce += 1;
            out.extend_from_slice(&(n as u16).to_be_bytes());
            out.extend_from_slice(&self.cipher[..n]);
        }
        self.wr.write_all(&out).await?;
        Ok(())
    }

    pub async fn send<T: Serialize>(&mut self, v: &T) -> Result<()> {
        let bytes = serde_json::to_vec(v)?;
        self.send_bytes(&bytes).await
    }

    pub async fn shutdown(&mut self) {
        let _ = self.wr.shutdown().await;
    }
}

pub struct FrameReader {
    rd: OwnedReadHalf,
    ts: Arc<StatelessTransportState>,
    nonce: u64,
    plain: Vec<u8>,
    cipher: Vec<u8>,
}

impl FrameReader {
    /// Read and decrypt one Noise message into the plaintext buffer.
    /// Returns false on a clean EOF at a message boundary.
    async fn fill(&mut self) -> Result<bool> {
        let mut len = [0u8; 2];
        match self.rd.read_exact(&mut len).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        let n = u16::from_be_bytes(len) as usize;
        if n < TAG {
            bail!("noise message too short");
        }
        self.rd.read_exact(&mut self.cipher[..n]).await?;
        let mut tmp = vec![0u8; n];
        let m = self.ts.read_message(self.nonce, &self.cipher[..n], &mut tmp).map_err(|e| anyhow!("decrypt failed: {e}"))?;
        self.nonce += 1;
        self.plain.extend_from_slice(&tmp[..m]);
        Ok(true)
    }

    /// Next application frame, or `None` on clean EOF.
    pub async fn recv_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        while self.plain.len() < 4 {
            if !self.fill().await? {
                if self.plain.is_empty() {
                    return Ok(None);
                }
                bail!("connection closed mid-frame");
            }
        }
        let len = u32::from_be_bytes(self.plain[..4].try_into().unwrap()) as usize;
        if len > MAX_FRAME {
            bail!("frame of {len} bytes exceeds MAX_FRAME");
        }
        while self.plain.len() < 4 + len {
            if !self.fill().await? {
                bail!("connection closed mid-frame");
            }
        }
        let frame = self.plain[4..4 + len].to_vec();
        self.plain.drain(..4 + len);
        Ok(Some(frame))
    }

    pub async fn recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>> {
        match self.recv_bytes().await? {
            None => Ok(None),
            Some(b) => Ok(Some(serde_json::from_slice(&b).context("malformed frame")?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn handshake_and_large_frames() {
        let hub = keys::StaticKey::generate();
        let node = keys::StaticKey::generate();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let hub_priv = hub.private.clone();
        let srv = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let (mut r, mut w, remote) = accept_responder(s, &hub_priv).await.unwrap();
            let f = r.recv_bytes().await.unwrap().unwrap();
            w.send_bytes(&f).await.unwrap();
            remote
        });
        let s = TcpStream::connect(addr).await.unwrap();
        let (mut r, mut w, remote) = connect_initiator(s, &node.private, Some(&hub.public)).await.unwrap();
        assert_eq!(remote, hub.public);
        let big: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        w.send_bytes(&big).await.unwrap();
        assert_eq!(r.recv_bytes().await.unwrap().unwrap(), big);
        assert_eq!(srv.await.unwrap(), node.public);
    }

    #[tokio::test]
    async fn pinned_key_mismatch_aborts_before_msg3() {
        let hub = keys::StaticKey::generate();
        let other = keys::StaticKey::generate();
        let node = keys::StaticKey::generate();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let hub_priv = hub.private.clone();
        let srv = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            accept_responder(s, &hub_priv).await.is_err()
        });
        let s = TcpStream::connect(addr).await.unwrap();
        let err = connect_initiator(s, &node.private, Some(&other.public)).await.err().unwrap();
        assert!(err.to_string().contains("HUB_KEY_MISMATCH"));
        assert!(srv.await.unwrap(), "responder must not complete the handshake");
    }
}
