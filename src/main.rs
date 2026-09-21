use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

pub use cockatiel_proto::proto;

use proto::container::Payload;
use proto::*;

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

async fn connect_ws() -> Result<WsStream, String> {
    let (ws, _) = connect_async(engine_url())
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;
    Ok(ws)
}

async fn send_container(ws: &mut WsStream, container: Container) -> Result<(), String> {
    let mut buf = Vec::new();
    container.encode(&mut buf).map_err(|e| format!("Encode error: {}", e))?;
    ws.send(WsMessage::Binary(buf.into()))
        .await
        .map_err(|e| format!("Send error: {}", e))
}

async fn receive_container(ws: &mut WsStream, timeout_ms: u64) -> Result<Container, String> {
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), ws.next()).await;
    match result {
        Ok(Some(Ok(WsMessage::Binary(data)))) => {
            Container::decode(data.as_ref()).map_err(|e| format!("Decode error: {}", e))
        }
        Ok(Some(Ok(WsMessage::Close(_)))) => Err("Connection closed by server".into()),
        Ok(Some(Ok(_))) => Err("Received non-binary message".into()),
        Ok(Some(Err(e))) => Err(format!("WebSocket error: {}", e)),
        Ok(None) => Err("Stream ended".into()),
        Err(_) => Err("Timeout".into()),
    }
}

/// Wait for connection to be severed (either close frame or stream end)
async fn wait_for_sever(ws: &mut WsStream, timeout_ms: u64) -> bool {
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
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

fn make_container(module_name: &str, uuid: &str, auth_token: &str, payload: Payload) -> Container {
    Container {
        version: 1,
        auth_token: auth_token.to_string(),
        module_name: module_name.to_string(),
        module_instance_uuid7: uuid.to_string(),
        payload: Some(payload),
    }
}

fn make_connection_request(pin: i32, uuid: &str) -> Container {
    make_container(
        "stress-test",
        uuid,
        "",
        Payload::ConnectionRequest(ConnectionRequest {
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
    }
}

// ── Test: Unauthenticated message types ──────────────────────────────
//
// Each message type in the Container oneof (except ConnectionRequest)
// should cause the engine to sever the connection immediately when
// sent without auth.

async fn test_unauthed_message_type(results: &mut TestResults, name: &str, payload: Payload) {
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

    let severed = wait_for_sever(&mut ws, 2000).await;
    if severed {
        results.pass(name);
    } else {
        results.fail(name, "Connection was NOT severed (expected sever)");
    }
}

// ── Test: Auth flow ──────────────────────────────────────────────────

async fn test_auth_invalid_pin(results: &mut TestResults) {
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

    match receive_container(&mut ws, 2000).await {
        Ok(container) => match container.payload {
            Some(Payload::ConnectionRequestReturn(ret)) => {
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

async fn test_auth_valid_pin(results: &mut TestResults) -> Option<String> {
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
    match receive_container(&mut ws, 15000).await {
        Ok(container) => match container.payload {
            Some(Payload::ConnectionRequestReturn(ret)) => {
                if ret.new_port == 0 && !container.auth_token.is_empty() {
                    results.pass("auth_valid_pin");
                    Some(container.auth_token)
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

async fn test_reconnect_with_token(
    results: &mut TestResults,
    auth_token: &str,
) -> Option<String> {
    let test_id = uuid::Uuid::now_v7().to_string();
    let mut ws = match connect_ws().await {
        Ok(ws) => ws,
        Err(e) => {
            results.fail("reconnect_with_token", &format!("Connect failed: {}", e));
            return None;
        }
    };

    // Send any message type with auth token — Log is simplest
    let container = make_container(
        "stress-test",
        &test_id,
        auth_token,
        Payload::Log(Log {
            log: "stress test reconnection".into(),
            blob: vec![],
        }),
    );

    if let Err(e) = send_container(&mut ws, container).await {
        results.fail("reconnect_with_token", &format!("Send failed: {}", e));
        return None;
    }

    // If authenticated, connection stays open. Check we weren't severed immediately.
    let result = tokio::time::timeout(Duration::from_millis(1500), ws.next()).await;
    match result {
        Ok(None) => {
            results.pass("reconnect_with_token");
            Some(test_id)
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
            // Timeout or other message — connection still alive
            results.pass("reconnect_with_token");
            Some(test_id)
        }
    }
}

async fn test_reconnect_invalid_token(results: &mut TestResults) {
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
        Payload::Log(Log {
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

    let severed = wait_for_sever(&mut ws, 2000).await;
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
    name: &str,
    auth_token: &str,
    uuid: &str,
    payload: Payload,
) {
    let mut ws = match connect_ws().await {
        Ok(ws) => ws,
        Err(e) => {
            results.fail(name, &format!("Connect failed: {}", e));
            return;
        }
    };

    // First, reconnect with auth token via a Log message
    let reconnect = make_container(
        "stress-test",
        uuid,
        auth_token,
        Payload::Log(Log {
            log: "authed send test".into(),
            blob: vec![],
        }),
    );
    if let Err(e) = send_container(&mut ws, reconnect).await {
        results.fail(name, &format!("Reconnect send failed: {}", e));
        return;
    }

    // Wait for auth to be processed
    sleep(Duration::from_millis(300)).await;

    // Drain any messages the engine may have sent
    let result = tokio::time::timeout(Duration::from_millis(100), ws.next()).await;
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
    let result = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
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

    println!("\n{}", "=".repeat(60));
    println!("  COCKATIEL ENGINE STRESS TEST");
    println!("  Target: {}", engine_url());
    println!("{}\n", "=".repeat(60));

    let mut results = TestResults::new();

    // ── Phase 1: Unauthenticated message types ────────────────────────
    // All Container oneof variants (except ConnectionRequest) should sever.
    println!("\n--- Phase 1: Unauthenticated message types (expect sever) ---\n");

    test_unauthed_message_type(
        &mut results,
        "unauth_ConnectionRequestReturn",
        Payload::ConnectionRequestReturn(ConnectionRequestReturn {
            new_port: 9999,
            module_instance_uuid7: "test".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_AuthNew",
        Payload::AuthNew(AuthNew {
            new_auth: "new-token".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_AuthVerify",
        Payload::AuthVerify(AuthVerify {
            cur_auth: "some-token".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_Command",
        Payload::CommandPayload(Command {
            command_name: "test_cmd".into(),
            command_flag: "".into(),
            command_description: "test".into(),
            command_flags: vec![],
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_Commands",
        Payload::CommandsPayload(Commands { commands: vec![] }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_UserData",
        Payload::UserData(UserData {
            uuid: "test-user".into(),
            username: "tester".into(),
            is_sponsor: false,
            is_moderator: false,
            is_admin: false,
            is_owner: false,
            bans: vec![],
            commendations: vec![],
            styling: None,
            platform_ids: Default::default(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_Log",
        Payload::Log(Log {
            log: "test log".into(),
            blob: vec![],
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_Err",
        Payload::Err(Err {
            log: "test error".into(),
            blob: vec![],
            trace: "stack trace".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_Shutdown",
        Payload::Shutdown(Shutdown {
            reason: "test shutdown".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_SendToPlatforms",
        Payload::SendToPlatforms(SendToPlatforms {
            msg: "test".into(),
            level: PlatformSendLevel::All as i32,
            module_uuid7: "test".into(),
            pid: "".into(),
            platform: "all".into(),
            actor_platform: "stress-test".into(),
            actor_handle: "stress-test".into(),
            actor_uuid7: "".into(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_TimelineEvent",
        Payload::TimelineEvent(TimelineEvent {
            timeline_id_uuid7: "test".into(),
            event_type: EventCategory::Log as i32,
            command_flag: "".into(),
            data_blob: vec![],
            error_message: "".into(),
            raw_flags: "".into(),
            message_origin: "test".into(),
            stream_origin: "".into(),
            raw_message: "".into(),
            processed_message: "".into(),
            user_uuid7: "".into(),
            version: 1,
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_MessagePreProcess",
        Payload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(dummy_chat_message()),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
    .await;

    test_unauthed_message_type(
        &mut results,
        "unauth_MessageInProcess",
        Payload::MessageInProcess(MessageInProcess {
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
        "unauth_MessagePostProcess",
        Payload::MessagePostProcess(MessagePostProcess {
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

    test_auth_invalid_pin(&mut results).await;

    let auth_token = test_auth_valid_pin(&mut results).await;
    let auth_token = match auth_token {
        Some(t) => t,
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

    let assigned_uuid = test_reconnect_with_token(&mut results, &auth_token).await;
    test_reconnect_invalid_token(&mut results).await;

    let assigned_uuid = match assigned_uuid {
        Some(u) => u,
        None => {
            results.skip("authed_send_tests", "No UUID from reconnect");
            println!("\n--- Skipping authenticated message tests (no UUID) ---");
            results.summary();
            return;
        }
    };

    // ── Phase 4: Authenticated message types ──────────────────────────
    println!("\n--- Phase 4: Authenticated message types (expect processed) ---\n");

    test_authed_send(
        &mut results,
        "authed_Log",
        &auth_token,
        &assigned_uuid,
        Payload::Log(Log {
            log: "authed stress test log".into(),
            blob: vec![],
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_Err",
        &auth_token,
        &assigned_uuid,
        Payload::Err(Err {
            log: "authed stress test error".into(),
            blob: vec![],
            trace: "test stack trace".into(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_TimelineEvent",
        &auth_token,
        &assigned_uuid,
        Payload::TimelineEvent(TimelineEvent {
            timeline_id_uuid7: uuid::Uuid::now_v7().to_string(),
            event_type: EventCategory::UserMessage as i32,
            command_flag: "".into(),
            data_blob: b"test data".to_vec(),
            error_message: "".into(),
            raw_flags: "".into(),
            message_origin: "stress-test".into(),
            stream_origin: "test-stream".into(),
            raw_message: "raw".into(),
            processed_message: "processed".into(),
            user_uuid7: "user-1".into(),
            version: 1,
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_MessagePreProcess",
        &auth_token,
        &assigned_uuid,
        Payload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(dummy_chat_message()),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_MessageInProcess",
        &auth_token,
        &assigned_uuid,
        Payload::MessageInProcess(MessageInProcess {
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
        "authed_MessagePostProcess",
        &auth_token,
        &assigned_uuid,
        Payload::MessagePostProcess(MessagePostProcess {
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
        "authed_Commands",
        &auth_token,
        &assigned_uuid,
        Payload::CommandsPayload(Commands {
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
                }],
            }],
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_SendToPlatforms",
        &auth_token,
        &assigned_uuid,
        // Note: the engine now requires the actor to be a DB-verified moderator,
        // so a bare authed SendToPlatforms may be rejected unless the actor
        // resolves to a verified moderator in the user database.
        Payload::SendToPlatforms(SendToPlatforms {
            msg: "Hello from stress test!".into(),
            level: PlatformSendLevel::All as i32,
            module_uuid7: assigned_uuid.clone(),
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
        "authed_Shutdown",
        &auth_token,
        &assigned_uuid,
        Payload::Shutdown(Shutdown {
            reason: "test shutdown from stress test".into(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_AuthNew",
        &auth_token,
        &assigned_uuid,
        Payload::AuthNew(AuthNew {
            new_auth: "new-token-test".into(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_AuthVerify",
        &auth_token,
        &assigned_uuid,
        Payload::AuthVerify(AuthVerify {
            cur_auth: auth_token.clone(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_ConnectionRequest",
        &auth_token,
        &assigned_uuid,
        Payload::ConnectionRequest(ConnectionRequest {
            pin: cli_pin(),
            process_position: ProcessPosition::Connection as i32,
            priority: 200,
            module_instance_uuid7: assigned_uuid.clone(),
        }),
    )
    .await;

    test_authed_send(
        &mut results,
        "authed_UserData",
        &auth_token,
        &assigned_uuid,
        Payload::UserData(UserData {
            uuid: "test-user".into(),
            username: "tester".into(),
            is_sponsor: false,
            is_moderator: false,
            is_admin: false,
            is_owner: false,
            bans: vec![],
            commendations: vec![],
            styling: Some(UserStylingTemplate {
                css_properties: Default::default(),
            }),
            platform_ids: Default::default(),
        }),
    )
    .await;

    // ── Summary ───────────────────────────────────────────────────────
    results.summary();
}
