use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

pub use cockatiel_proto::proto;

use proto::container_for_engine::Payload as EnginePayload;
use proto::container_for_module::Payload as ModulePayload;
use proto::*;

mod config;

use config::Config;

/// Engine address, overridable via --ip/--port (the benchmark points modules at
/// a fake engine on a random port).
static ENGINE_URL: OnceLock<String> = OnceLock::new();
fn engine_url() -> &'static str {
    ENGINE_URL.get_or_init(|| "ws://127.0.0.1:9734".to_string())
}

/// PIN for the valid-auth test, overridable via --pin (defaults to the engine's).
static CLI_PIN: OnceLock<i32> = OnceLock::new();
fn cli_pin() -> i32 {
    *CLI_PIN.get_or_init(|| 685689)
}

const WRONG_PIN: i32 = 000000;

struct TestResults {
    passed: u32,
    failed: u32,
    skipped: u32,
}

impl TestResults {
    fn new() -> Self {
        Self { passed: 0, failed: 0, skipped: 0 }
    }

    fn pass(&mut self, name: &str) {
        self.passed += 1;
        println!("  \x1b[32mPASS\x1b[0m {}", name);
    }

    fn fail(&mut self, name: &str, reason: &str) {
        self.failed += 1;
        println!("  \x1b[31mFAIL\x1b[0m {}: {}", name, reason);
    }

    fn skip(&mut self, name: &str, reason: &str) {
        self.skipped += 1;
        println!("  \x1b[33mSKIP\x1b[0m {}: {}", name, reason);
    }

    fn summary(&self) {
        println!("\n{}", "=".repeat(60));
        println!(
            "Results: \x1b[32m{}\x1b[0m passed, \x1b[31m{}\x1b[0m failed, \x1b[33m{}\x1b[0m skipped",
            self.passed, self.failed, self.skipped
        );
        println!("{}", "=".repeat(60));
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Build a rustls client config that trusts exactly the engine's self-signed
/// certificate (cert pinning). Any other chain is rejected.
fn pinned_tls_config(cert_pem_path: &str) -> Result<rustls::ClientConfig, String> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cert_bytes = std::fs::read(cert_pem_path)
        .map_err(|e| format!("read TLS cert {}: {}", cert_pem_path, e))?;
    let mut reader = std::io::BufReader::new(cert_bytes.as_slice());
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|e| format!("parse TLS cert: {}", e))?;
    let mut roots = rustls::RootCertStore::empty();
    for c in certs {
        roots
            .add(c)
            .map_err(|e| format!("add pinned cert: {}", e))?;
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// WSS when COCKATIEL_TLS_CERT points at the engine's self-signed cert (pinned
/// as the trust root); plain ws:// otherwise (the fake-engine path).
async fn connect_ws() -> Result<WsStream, String> {
    let url = engine_url();
    let (scheme, connector): (&str, Option<tokio_tungstenite::Connector>) =
        match std::env::var("COCKATIEL_TLS_CERT") {
            Ok(path) if !path.trim().is_empty() => {
                let cfg = pinned_tls_config(&path)?;
                ("wss", Some(tokio_tungstenite::Connector::Rustls(std::sync::Arc::new(cfg))))
            }
            _ => ("ws", None),
        };
    let hostport = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("wss://"))
        .unwrap_or(url);
    let url = format!("{}://{}", scheme, hostport);
    let result = match &connector {
        Some(c) => tokio_tungstenite::connect_async_tls_with_config(&url, None, false, Some(c.clone())).await,
        None => connect_async(&url).await,
    };
    let (ws, _) = result.map_err(|e| format!("Connection failed: {}", e))?;
    Ok(ws)
}

async fn send_container(ws: &mut WsStream, container: ContainerForEngine) -> Result<(), String> {
    let mut buf = Vec::new();
    container.encode(&mut buf).map_err(|e| format!("Encode error: {}", e))?;
    ws.send(WsMessage::Binary(buf))
        .await
        .map_err(|e| format!("Send error: {}", e))
}

async fn receive_container(ws: &mut WsStream, timeout_ms: u32) -> Result<ContainerForModule, String> {
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms as u64), ws.next()).await;
    match result {
        Ok(Some(Ok(WsMessage::Binary(data)))) => {
            ContainerForModule::decode(data.as_ref()).map_err(|e| format!("Decode error: {}", e))
        }
        Ok(Some(Ok(WsMessage::Close(_)))) => Err("Connection closed by server".into()),
        Ok(Some(Ok(_))) => Err("Received non-binary message".into()),
        Ok(Some(Err(e))) => Err(format!("WebSocket error: {}", e)),
        Ok(None) => Err("Stream ended".into()),
        Err(_) => Err("Timeout".into()),
    }
}

/// Wait for connection to be severed (either close frame or stream end)
async fn wait_for_sever(ws: &mut WsStream, timeout_ms: u32) -> bool {
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms as u64), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(WsMessage::Close(_)) | Ok(WsMessage::Frame(_)) => return true,
                Err(_) => return true,
                _ => continue,
            }
        }
        true // stream ended
    })
    .await;

    result.unwrap_or(false)
}

fn make_container(module_name: &str, uuid: &str, auth_token: &str, payload: EnginePayload) -> ContainerForEngine {
    ContainerForEngine {
        version: 2,
        auth_token: auth_token.to_string(),
        module_name: module_name.to_string(),
        module_instance_uuid7: uuid.to_string(),
        payload: Some(payload),
    }
}

fn make_connection_request(pin: i32, uuid: &str) -> ContainerForEngine {
    make_container(
        "stress-test",
        uuid,
        "",
        EnginePayload::ConnectionRequest(ConnectionRequest {
            pin,
            process_position: ProcessPosition::Connection as i32,
            priority: 100,
            module_instance_uuid7: uuid.to_string(),
        }),
    )
}

fn dummy_chat_message() -> ChatMessage {
    ChatMessage {
        platform: "test".into(),
        raw_data: b"{}".to_vec(),
        raw_message: "test message".into(),
        user_uuid7: "00000000-0000-7000-0000-000000000001".into(),
        command: None,
        user_data: None,
        channel_id: String::new(),
    }
}

// ── Test: Unauthenticated message types ──────────────────────────────
//
// Each message type in the Container oneof (except ConnectionRequest)
// should cause the engine to sever the connection immediately when
// sent without auth.

async fn test_unauthed_message_type(
    results: &mut TestResults,
    config: &Config,
    name: &str,
    payload: EnginePayload,
) {
    let test_id = uuid::Uuid::now_v7().to_string();
    let mut ws = match connect_ws().await {
        Ok(ws) => ws,
        Err(e) => {
            results.fail(name, &format!("Failed to connect: {}", e));
            return;
        }
    };

    let container = make_container("stress-test", &test_id, "", payload);

    if let Err(e) = send_container(&mut ws, container).await {
        results.fail(name, &format!("Failed to send: {}", e));
        return;
    }

    let severed = wait_for_sever(&mut ws, config.sever_wait_ms).await;
    if severed {
        results.pass(name);
    } else {
        results.fail(name, "Connection was NOT severed (expected sever)");
    }
}

// ── Test: Auth flow ──────────────────────────────────────────────────

async fn test_auth_invalid_pin(results: &mut TestResults, config: &Config) {
    let test_id = uuid::Uuid::now_v7().to_string();
    let mut ws = match connect_ws().await {
        Ok(ws) => ws,
        Err(e) => {
            results.fail("auth_invalid_pin", &format!("Connect failed: {}", e));
            return;
        }
    };

    let req = make_connection_request(WRONG_PIN, &test_id);
    if let Err(e) = send_container(&mut ws, req).await {
        results.fail("auth_invalid_pin", &format!("Send failed: {}", e));
        return;
    }

    match receive_container(&mut ws, config.invalid_pin_reply_ms).await {
        Ok(container) => match container.payload {
            Some(ModulePayload::ConnectionRequestReturn(ret)) => {
                if ret.new_port == 0 {
                    results.pass("auth_invalid_pin");
                } else {
                    results.fail(
                        "auth_invalid_pin",
                        &format!("Expected new_port: 0, got: {}", ret.new_port),
                    );
                }
            }
            other => {
                results.fail(
                    "auth_invalid_pin",
                    &format!("Expected ConnectionRequestReturn, got: {:?}", other),
                );
            }
        },
        Err(e) => {
            // Connection severed is also acceptable
            if e.contains("closed") || e.contains("ended") {
                results.pass("auth_invalid_pin");
            } else {
                results.fail("auth_invalid_pin", &format!("Unexpected: {}", e));
            }
        }
    }
}

/// Authenticate with a valid PIN and return the issued `(auth_token, instance_uuid7)`.
/// The token is cryptographically bound to the instance uuid (JWT `sub` claim), so a
/// later reconnect must present the SAME uuid or `verify_token` fails.
async fn test_auth_valid_pin(results: &mut TestResults, config: &Config) -> Option<(String, String)> {
    let test_id = uuid::Uuid::now_v7().to_string();
    let mut ws = match connect_ws().await {
        Ok(ws) => ws,
        Err(e) => {
            results.fail("auth_valid_pin", &format!("Connect failed: {}", e));
            return None;
        }
    };

    let req = make_connection_request(cli_pin(), &test_id);
    if let Err(e) = send_container(&mut ws, req).await {
        results.fail("auth_valid_pin", &format!("Send failed: {}", e));
        return None;
    }

    // The engine will prompt for auth on the terminal. For automated testing,
    // pre-register this module in modules.json with auto_auth=true.
    // If not registered, the prompt will block and we'll timeout here.
    match receive_container(&mut ws, config.valid_pin_reply_ms).await {
        Ok(container) => match container.payload {
            Some(ModulePayload::ConnectionRequestReturn(ret)) => {
                if ret.new_port == 0 && !container.auth_token.is_empty() {
                    results.pass("auth_valid_pin");
                    Some((container.auth_token, test_id))
                } else {
                    results.fail(
                        "auth_valid_pin",
                        &format!(
                            "Unexpected response: new_port={}, auth_token_empty={}",
                            ret.new_port,
                            container.auth_token.is_empty()
                        ),
                    );
                    None
                }
            }
            other => {
                results.fail(
                    "auth_valid_pin",
                    &format!("Expected ConnectionRequestReturn, got: {:?}", other),
                );
                None
            }
        },
        Err(e) => {
            results.fail("auth_valid_pin", &format!("No response: {}", e));
            None
        }
    }
}

/// Reconnect an authenticated session by presenting the minted token with the
/// SAME instance uuid it was issued for. The engine's `verify_token` binds the
/// JWT `sub` claim to the uuid, so a fresh `Uuid::now_v7()` here would always
/// be severed. The engine also requires the FIRST message to be a
/// ConnectionRequest (carrying the token) — a bare Log is rejected before any
/// token check. Returns the instance uuid on success so the caller can reuse
/// the authenticated session for the authed_* tests.
async fn test_reconnect_with_token(
    results: &mut TestResults,
    config: &Config,
    auth_token: &str,
    instance_uuid: &str,
) -> Option<String> {
    let mut ws = match connect_ws().await {
        Ok(ws) => ws,
        Err(e) => {
            results.fail("reconnect_with_token", &format!("Connect failed: {}", e));
            return None;
        }
    };

    let mut req = make_connection_request(cli_pin(), instance_uuid);
    req.auth_token = auth_token.to_string();

    if let Err(e) = send_container(&mut ws, req).await {
        results.fail("reconnect_with_token", &format!("Send failed: {}", e));
        return None;
    }

    // A successful reconnect keeps the connection open. The engine sends no
    // response on reconnect, so a timeout (or an unexpected message) means the
    // session is authenticated and alive; a close/stream-end means it was
    // severed.
    let result = tokio::time::timeout(Duration::from_millis(config.keepalive_probe_ms as u64), ws.next()).await;
    match result {
        Ok(None) => {
            results.fail("reconnect_with_token", "Connection closed (reconnect rejected)");
            None
        }
        Ok(Some(Ok(WsMessage::Close(_)))) => {
            results.fail("reconnect_with_token", "Connection was severed (invalid auth?)");
            None
        }
        Ok(Some(Err(_))) => {
            results.fail("reconnect_with_token", "WebSocket error");
            None
        }
        _ => {
            // Timeout or unexpected message — connection still alive.
            results.pass("reconnect_with_token");
            Some(instance_uuid.to_string())
        }
    }
}

async fn test_reconnect_invalid_token(results: &mut TestResults, config: &Config) {
    let test_id = uuid::Uuid::now_v7().to_string();
    let mut ws = match connect_ws().await {
        Ok(ws) => ws,
        Err(e) => {
            results.fail(
                "reconnect_invalid_token",
                &format!("Connect failed: {}", e),
            );
            return;
        }
    };

    let container = make_container(
        "stress-test",
        &test_id,
        "definitely-not-a-valid-token",
        EnginePayload::Log(Log {
            log: "should be severed".into(),
            blob: vec![],
        }),
    );

    if let Err(e) = send_container(&mut ws, container).await {
        results.fail(
            "reconnect_invalid_token",
            &format!("Send failed: {}", e),
        );
        return;
    }

    let severed = wait_for_sever(&mut ws, config.sever_wait_ms).await;
    if severed {
        results.pass("reconnect_invalid_token");
    } else {
        results.fail("reconnect_invalid_token", "Connection was NOT severed");
    }
}

// ── Test: Authenticated message types ────────────────────────────────
//
// After auth, each Container payload type should be processed (not severed).
// Ban, Commendation, Reprimand, Flag are sub-messages (not Container payloads)
// so they can't be sent directly — they'd need to be wrapped in a future
// UserData/Command/Commands message. For now we test the actual Container oneof variants.

async fn test_authed_send(
    results: &mut TestResults,
    config: &Config,
    name: &str,
    auth_token: &str,
    uuid: &str,
    payload: EnginePayload,
) {
    let mut ws = match connect_ws().await {
        Ok(ws) => ws,
        Err(e) => {
            results.fail(name, &format!("Connect failed: {}", e));
            return;
        }
    };

    // First, re-establish the authenticated session. The engine requires the
    // FIRST message to be a ConnectionRequest carrying the minted token for the
    // instance uuid it was issued to (a bare Log is rejected before any token
    // check, and a fresh uuid would fail verify_token's `sub` binding).
    let mut reconnect = make_connection_request(cli_pin(), uuid);
    reconnect.auth_token = auth_token.to_string();
    if let Err(e) = send_container(&mut ws, reconnect).await {
        results.fail(name, &format!("Reconnect send failed: {}", e));
        return;
    }

    // Wait for auth to be processed
    sleep(Duration::from_millis(config.post_reconnect_settle_ms as u64)).await;

    // Drain any messages the engine may have sent
    let result = tokio::time::timeout(Duration::from_millis(config.pre_test_drain_ms as u64), ws.next()).await;
    if let Ok(Some(Ok(WsMessage::Close(_)))) = result {
        results.fail(name, "Connection severed during drain");
        return;
    }

    // Now send the actual test message
    let container = make_container("stress-test", uuid, auth_token, payload);
    if let Err(e) = send_container(&mut ws, container).await {
        results.fail(name, &format!("Send failed: {}", e));
        return;
    }

    // Check connection is still alive
    let result = tokio::time::timeout(Duration::from_millis(config.post_send_alive_check_ms as u64), ws.next()).await;
    match result {
        Ok(Some(Ok(WsMessage::Close(_)))) => {
            results.fail(name, "Connection severed after send (auth may have failed)");
        }
        Ok(Some(Err(_))) => {
            results.fail(name, "WebSocket error after send");
        }
        _ => {
            // Timeout or other — connection still alive
            results.pass(name);
        }
    }
}

// ── Main test runner ─────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let config = Config::load_or_default();

    // Parse --ip / --port / --pin overrides (the benchmark points this at a
    // fake engine on a random port).
    let args: Vec<String> = std::env::args().collect();
    let mut ip = "127.0.0.1".to_string();
    let mut port = 9734u16;
    let mut pin = 685689i32;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--ip" || args[i] == "-i" {
            if let Some(v) = args.get(i + 1) {
                ip = v.clone();
                i += 2;
            } else {
                i += 1;
            }
        } else if args[i] == "--port" || args[i] == "-p" {
            if let Some(v) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                port = v;
                i += 2;
            } else {
                i += 1;
            }
        } else if args[i] == "--pin" {
            if let Some(v) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                pin = v;
                i += 2;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    let _ = ENGINE_URL.set(format!("ws://{}:{}", ip, port));
    let _ = CLI_PIN.set(pin);

    // ── Live-engine guard ─────────────────────────────────────────────
    // Phase 3+ (authed/ingest) presents minted tokens and injects real messages
    // into the engine's live pipeline (authed_MessagePreProcess ingests,
    // authed_SendToPlatforms posts to platforms), which is only safe against the
    // fake engine. Require an explicit opt-in --live-ok (or STRESS_TEST_LIVE_OK=1)
    // before running any authed phase, regardless of target.
    let live_ok = std::env::var("STRESS_TEST_LIVE_OK")
        .map(|v| v == "1")
        .unwrap_or(false)
        || args.iter().any(|a| a == "--live-ok");

    println!("\n{}", "=".repeat(60));
    println!("  COCKATIEL ENGINE STRESS TEST");
    println!("  Target: {}", engine_url());
    println!("  Live-engine opt-in (--live-ok): {}", live_ok);
    println!("{}\n", "=".repeat(60));

    let mut results = TestResults::new();

    // ── Phase 1: Unauthenticated message types ────────────────────────
    // All Container oneof variants (except ConnectionRequest) should sever.
    println!("\n--- Phase 1: Unauthenticated message types (expect sever) ---\n");

    // Timeline pollution note: Phase 1 opens ~9 unauthenticated connections,
    // each of which the REAL engine archives as a `module_reject` timeline
    // event. The fake engine records none (it has no timeline DB). The live
    // engine path is gated behind --live-ok; expect the pollution there.
    if live_ok {
        println!("\x1b[33mNote:\x1b[0m against a live engine each unauth connect below archives a");
        println!("      `module_reject` timeline event (~9 total). That pollution is inherent");
        println!("      to these negative tests; the fake engine records none of it.");
    }

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_AuthVerify",
        EnginePayload::AuthVerify(AuthVerify {
            cur_auth: "some-token".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_Command",
        EnginePayload::Command(Command {
            command_name: "test_cmd".into(),
            command_flag: "".into(),
            command_description: "test".into(),
            command_flags: vec![],
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_Commands",
        EnginePayload::Commands(Commands { commands: vec![], alert_on_unknown_command: false }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_Log",
        EnginePayload::Log(Log {
            log: "test log".into(),
            blob: vec![],
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_Err",
        EnginePayload::Err(Err {
            log: "test error".into(),
            blob: vec![],
            trace: "stack trace".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_SendToPlatforms",
        EnginePayload::SendToPlatforms(SendToPlatforms {
            msg: "test".into(),
            level: PlatformSendLevel::All as i32,
            module_uuid7: "test".into(),
            pid: "".into(),
            platform: "all".into(),
            channel_id: String::new(),
            actor_platform: "stress-test".into(),
            actor_handle: "stress-test".into(),
            actor_uuid7: "".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_MessagePreProcess",
        EnginePayload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(dummy_chat_message()),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_MessageInProcess",
        EnginePayload::MessageInProcess(MessageInProcess {
            message_uuid7: String::new(),
            raw_message: Some(dummy_chat_message()),
            processed_message: "processed".into(),
            abandon_message: false,
            audio: vec![],
            audio_type: String::new(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        &config,
        "unauth_MessagePostProcess",
        EnginePayload::MessagePostProcess(MessagePostProcess {
            message_uuid7: String::new(),
            raw_message: Some(dummy_chat_message()),
            processed_message: "processed".into(),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
    .await;

    // ── Phase 2: Auth flow ────────────────────────────────────────────
    println!("\n--- Phase 2: Auth flow ---\n");

    test_auth_invalid_pin(&mut results, &config).await;

    let auth = test_auth_valid_pin(&mut results, &config).await;
    let (auth_token, auth_instance_uuid) = match auth {
        Some(v) => v,
        None => {
            results.skip("reconnect_with_token", "No auth token from previous test");
            results.skip(
                "reconnect_invalid_token",
                "No auth token from previous test",
            );
            println!("\n--- Skipping authenticated tests (no auth token) ---");
            results.summary();
            return;
        }
    };

    // ── Phase 3: Reconnection ─────────────────────────────────────────
    println!("\n--- Phase 3: Reconnection with token ---\n");

    // Live-engine guard: Phase 3+ exercises authenticated sessions and Phase 4
    // injects real payloads into the engine's live pipeline. Refuse to run any
    // of it unless the operator opted in explicitly.
    if !live_ok {
        println!("\n\x1b[31m{}", "=".repeat(60));
        println!("  REFUSING to run authed/ingest phases (Phase 3+).");
        println!("  The authed phases inject real messages into the engine's live");
        println!("  pipeline (authed_MessagePreProcess ingests, authed_SendToPlatforms");
        println!("  posts to platforms) and are only safe against the fake engine.");
        println!("  Re-run with --live-ok (or STRESS_TEST_LIVE_OK=1) to opt in.");
        println!("  {}\x1b[0m", "=".repeat(60));
        results.skip("phase3_reconnect_with_token", "live guard: --live-ok not set");
        results.skip(
            "phase3_reconnect_invalid_token",
            "live guard: --live-ok not set",
        );
        results.skip("phase4_authed_send_tests", "live guard: --live-ok not set");
        results.summary();
        return;
    }

    println!("\x1b[33m{}", "=".repeat(60));
    println!("  WARNING: --live-ok set — running authed/ingest phases against");
    println!("  {}", engine_url());
    println!("  These inject real messages into the engine's live pipeline and");
    println!("  are ONLY safe against the fake engine. Proceed at your own risk.");
    println!("  {}\x1b[0m", "=".repeat(60));

    // The reconnect MUST reuse the uuid the token was minted for: the token's
    // JWT `sub` is bound to it, so a fresh uuid would always be severed.
    let assigned_uuid =
        test_reconnect_with_token(&mut results, &config, &auth_token, &auth_instance_uuid).await;
    test_reconnect_invalid_token(&mut results, &config).await;

    let assigned_uuid = match assigned_uuid {
        Some(u) => u,
        None => {
            results.skip(
                "authed_send_tests",
                "Reconnect failed — no authenticated session established",
            );
            println!(
                "\n\x1b[31m--- Skipping Phase 4 authenticated message tests: reconnect FAILED above ---\x1b[0m"
            );
            results.summary();
            return;
        }
    };

    // ── Phase 4: Authenticated message types ──────────────────────────
    println!("\n--- Phase 4: Authenticated message types (expect processed) ---\n");

    test_authed_send(
        &mut results,
        &config,
        "authed_Log",
        &auth_token,
        &assigned_uuid,
        EnginePayload::Log(Log {
            log: "authed stress test log".into(),
            blob: vec![],
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        &config,
        "authed_Err",
        &auth_token,
        &assigned_uuid,
        EnginePayload::Err(Err {
            log: "authed stress test error".into(),
            blob: vec![],
            trace: "test stack trace".into(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        &config,
        "authed_MessagePreProcess",
        &auth_token,
        &assigned_uuid,
        EnginePayload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(dummy_chat_message()),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        &config,
        "authed_MessageInProcess",
        &auth_token,
        &assigned_uuid,
        EnginePayload::MessageInProcess(MessageInProcess {
            message_uuid7: String::new(),
            raw_message: Some(dummy_chat_message()),
            processed_message: "in-processed".into(),
            abandon_message: false,
            audio: vec![],
            audio_type: String::new(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        &config,
        "authed_MessagePostProcess",
        &auth_token,
        &assigned_uuid,
        EnginePayload::MessagePostProcess(MessagePostProcess {
            message_uuid7: String::new(),
            raw_message: Some(dummy_chat_message()),
            processed_message: "post-processed".into(),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        &config,
        "authed_Commands",
        &auth_token,
        &assigned_uuid,
        EnginePayload::Commands(Commands {
            alert_on_unknown_command: false,
            commands: vec![Command {
                command_name: "test_cmd".into(),
                command_flag: "!test".into(),
                command_description: "A test command".into(),
                command_flags: vec![Flag {
                    flag_name: "verbose".into(),
                    flag_description: "Enable verbose output".into(),
                    limiting_type: FlagLimitType::Any as i32,
                    min_val: 0.0,
                    max_val: 1.0,
                    options: vec![],
                    value: String::new(),
                }],
            }],
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        &config,
        "authed_SendToPlatforms",
        &auth_token,
        &assigned_uuid,
        // Note: the engine now requires the actor to be a DB-verified moderator,
        // so a bare authed SendToPlatforms may be rejected unless the actor
        // resolves to a verified moderator in the user database.
        EnginePayload::SendToPlatforms(SendToPlatforms {
            msg: "Hello from stress test!".into(),
            level: PlatformSendLevel::All as i32,
            module_uuid7: assigned_uuid.clone(),
            channel_id: String::new(),
            pid: "".into(),
            platform: "all".into(),
            actor_platform: "stress-test".into(),
            actor_handle: "stress-test".into(),
            actor_uuid7: "".into(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        &config,
        "authed_AuthVerify",
        &auth_token,
        &assigned_uuid,
        EnginePayload::AuthVerify(AuthVerify {
            cur_auth: auth_token.clone(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        &config,
        "authed_ConnectionRequest",
        &auth_token,
        &assigned_uuid,
        EnginePayload::ConnectionRequest(ConnectionRequest {
            pin: cli_pin(),
            process_position: ProcessPosition::Connection as i32,
            priority: 200,
            module_instance_uuid7: assigned_uuid.clone(),
        }),
    )
    .await;

    // ── Summary ───────────────────────────────────────────────────────
    results.summary();
}
