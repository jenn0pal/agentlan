//! SQLite helpers shared by hub and node: WAL + `synchronous=FULL`, and a tiny
//! idempotent schema runner. Each database is owned by a single writer thread.

use std::path::Path;

use anyhow::{Context, Result};
pub use rusqlite;
use rusqlite::{Connection, OptionalExtension, params};

/// Open (or create) a database with the durability settings from the design.
pub fn open(path: &Path) -> Result<Connection> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(conn)
}

/// Apply `schema` (a batch of `CREATE … IF NOT EXISTS` statements).
pub fn migrate(conn: &Connection, schema: &str) -> Result<()> {
    conn.execute_batch(schema).context("applying schema")?;
    conn.execute_batch("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);")?;
    Ok(())
}

pub fn meta_get(conn: &Connection, k: &str) -> Result<Option<String>> {
    Ok(conn.query_row("SELECT v FROM meta WHERE k=?1", params![k], |r| r.get(0)).optional()?)
}

pub fn meta_set(conn: &Connection, k: &str, v: &str) -> Result<()> {
    conn.execute("INSERT INTO meta(k,v) VALUES(?1,?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v", params![k, v])?;
    Ok(())
}

pub fn meta_u64(conn: &Connection, k: &str) -> Result<u64> {
    Ok(meta_get(conn, k)?.and_then(|v| v.parse().ok()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_and_meta() {
        let dir = std::env::temp_dir().join(format!("agentlan-store-{}", std::process::id()));
        let c = open(&dir.join("t.db")).unwrap();
        migrate(&c, "CREATE TABLE IF NOT EXISTS x(a INTEGER);").unwrap();
        meta_set(&c, "seq", "5").unwrap();
        assert_eq!(meta_u64(&c, "seq").unwrap(), 5);
        let mode: String = c.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
        assert_eq!(mode, "wal");
        std::fs::remove_dir_all(dir).ok();
    }
}
