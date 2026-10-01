use serde::{Deserialize, Serialize};

/// Per-test timing values, read from `config.json` → `module_specific` so
/// operators can tune the harness without a rebuild. Every field is defaulted;
/// missing keys are backfilled into `config.json`. CLI args (--ip/--port/--pin)
/// still win over the file — this struct holds no connection settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// How long to wait for the engine to sever an unauthenticated connection.
    pub sever_wait_ms: u32,
    /// How long to wait for the invalid-PIN auth reply.
    pub invalid_pin_reply_ms: u32,
    /// How long to wait for the valid-PIN auth reply.
    pub valid_pin_reply_ms: u32,
    /// How long to wait for any engine keepalive before treating a reconnect
    /// as alive.
    pub keepalive_probe_ms: u32,
    /// Pause after the reconnect handshake before the test sends.
    pub post_reconnect_settle_ms: u32,
    /// How long to drain the socket for stray frames before the test send.
    pub pre_test_drain_ms: u32,
    /// How long to wait after the test send to confirm the connection is alive.
    pub post_send_alive_check_ms: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sever_wait_ms: 2000,
            invalid_pin_reply_ms: 2000,
            valid_pin_reply_ms: 15000,
            keepalive_probe_ms: 1500,
            post_reconnect_settle_ms: 300,
            pre_test_drain_ms: 100,
            post_send_alive_check_ms: 500,
        }
    }
}

impl Config {
    pub fn load_or_default() -> Self {
        Self::load_or_default_from("config.json")
    }

    pub fn load_or_default_from<P: AsRef<std::path::Path>>(path: P) -> Self {
        let mut cfg = Self::default();
        if let Ok(s) = std::fs::read_to_string(&path) {
            if let Ok(root) = serde_json::from_str::<serde_json::Value>(&s) {
                match root.get("module_specific") {
                    Some(ms) if ms.is_object() => {
                        cfg = serde_json::from_value(ms.clone()).unwrap_or(cfg);
                    }
                    _ => {
                        cfg = serde_json::from_str::<Config>(&s).unwrap_or(cfg);
                    }
                }
            }
        }
        // Backfill missing keys into config.json (module_specific object),
        // preserving any existing top-level fields such as ip/port/pin.
        let mut root: serde_json::Value = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        root["module_specific"] = serde_json::to_value(&cfg).unwrap_or(serde_json::Value::Null);
        if let Ok(pretty) = serde_json::to_string_pretty(&root) {
            let _ = std::fs::write(&path, pretty);
        }
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_yields_defaults_and_backfills() {
        let dir = std::env::temp_dir().join(format!("stress_config_test_{}", uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        let cfg = Config::load_or_default_from(&path);
        assert_eq!(cfg.sever_wait_ms, 2000);
        assert_eq!(cfg.invalid_pin_reply_ms, 2000);
        assert_eq!(cfg.valid_pin_reply_ms, 15000);
        assert_eq!(cfg.keepalive_probe_ms, 1500);
        assert_eq!(cfg.post_reconnect_settle_ms, 300);
        assert_eq!(cfg.pre_test_drain_ms, 100);
        assert_eq!(cfg.post_send_alive_check_ms, 500);
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["module_specific"]["sever_wait_ms"], 2000);
        assert_eq!(written["module_specific"]["valid_pin_reply_ms"], 15000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_module_specific_fills_missing_with_defaults() {
        let dir = std::env::temp_dir().join(format!("stress_config_test2_{}", uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        std::fs::write(
            &path,
            r#"{"engine_ip":"127.0.0.1","module_specific":{"sever_wait_ms":5000}}"#,
        )
        .unwrap();
        let cfg = Config::load_or_default_from(&path);
        assert_eq!(cfg.sever_wait_ms, 5000);
        assert_eq!(cfg.valid_pin_reply_ms, 15000);
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["engine_ip"], "127.0.0.1");
        assert_eq!(written["module_specific"]["sever_wait_ms"], 5000);
        assert_eq!(written["module_specific"]["valid_pin_reply_ms"], 15000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn uuid() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}