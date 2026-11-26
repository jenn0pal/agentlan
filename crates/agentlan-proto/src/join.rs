//! Join codes: `AGENTLAN1-<base64url(json)>` carrying hub addresses, the full hub
//! public key and a one-time enrolment token.

use data_encoding::BASE64URL_NOPAD;
use serde::{Deserialize, Serialize};

use crate::Ts;

pub const PREFIX: &str = "AGENTLAN1-";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JoinCode {
    pub net: String,
    pub addrs: Vec<String>,
    /// Hub static public key, base32 (no padding).
    pub hub_key: String,
    pub token: String,
    pub expires: Ts,
}

impl JoinCode {
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("join code serializes");
        format!("{PREFIX}{}", BASE64URL_NOPAD.encode(&json))
    }

    pub fn decode(s: &str) -> Result<JoinCode, String> {
        let body = s.trim().strip_prefix(PREFIX).ok_or("not an AGENTLAN1 join code")?;
        let bytes = BASE64URL_NOPAD.decode(body.as_bytes()).map_err(|e| format!("bad join code: {e}"))?;
        serde_json::from_slice(&bytes).map_err(|e| format!("bad join code: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let j = JoinCode { net: "lab".into(), addrs: vec!["127.0.0.1:47820".into()], hub_key: "ABC".into(), token: "t".into(), expires: crate::now() };
        assert_eq!(JoinCode::decode(&j.encode()).unwrap(), j);
        assert!(JoinCode::decode("nope").is_err());
    }
}
