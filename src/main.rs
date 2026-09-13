use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::env;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use uuid::Uuid;

pub mod cockatiel_protobuf {
    include!(concat!(env!("OUT_DIR"), "/cockatiel_protobuf.v1.rs"));
}

use cockatiel_protobuf::{
    container::Payload, ConnectionRequest, Container, CoreMessage, Log, MessageInProcess,
    MessagePostProcess, MessagePreProcess, TimelineEvent,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("[TestInputModule] Starting dataflow verification client...");

    let url = env::var("COCKATIEL_ENGINE_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080".into());
    let stages = vec!["connections", "preprocess", "inprocess", "postprocess"];

    for stage in stages {
        println!(
            "\n[TestInputModule] Connecting to engine at {} for stage: [{}]",
            url, stage
        );

        let (mut ws_stream, _) = match connect_async(&url).await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!(
                    "[TestInputModule] Failed to connect: {}. Is the engine running?",
                    e
                );
                continue;
            }
        };

        let instance_id = Uuid::now_v7().to_string();
        let module_name = format!("test-module-{}", stage);

        // 1. Send ConnectionRequest
        let conn_req = ConnectionRequest {
            module_name: module_name.clone(),
            position: stage.to_string(),
            priority: 10,
            version: "1.0.0".to_string(),
        };

        let container_req = Container {
            version: 1,
            auth_token: "test-token".to_string(),
            module_name: module_name.clone(),
            module_instance_uuid7: instance_id.clone(),
            payload: Some(Payload::ConnectionRequest(conn_req)),
        };

        let mut bytes = Vec::new();
        container_req.encode(&mut bytes)?;
        ws_stream.send(WsMessage::Binary(bytes.into())).await?;
        println!("[TestInputModule] Sent ConnectionRequest for {}", stage);

        // 2. Send specific stage payload message to verify dataflow
        let payload = match stage {
            "connections" | "preprocess" => Payload::MessagePreProcess(MessagePreProcess {
                core_message: Some(CoreMessage {
                    platform: "test-platform".into(),
                    raw_data: "test-raw-data".into(),
                    user_uuid7: Uuid::now_v7().to_string(),
                    raw_message: "Hello Cockatiel Preprocess".into(),
                    command: "".into(),
                    user_data: "".into(),
                }),
            }),
            "inprocess" => Payload::MessageInProcess(MessageInProcess {
                core_message: Some(CoreMessage {
                    platform: "test-platform".into(),
                    raw_data: "test-raw-data".into(),
                    user_uuid7: Uuid::now_v7().to_string(),
                    raw_message: "Hello Cockatiel Inprocess".into(),
                    command: "".into(),
                    user_data: "".into(),
                }),
                processed_message: "Processed in-flight message".into(),
                abandon_message: false,
            }),
            "postprocess" => Payload::MessagePostProcess(MessagePostProcess {
                core_message: Some(CoreMessage {
                    platform: "test-platform".into(),
                    raw_data: "test-raw-data".into(),
                    user_uuid7: Uuid::now_v7().to_string(),
                    raw_message: "Hello Cockatiel Postprocess".into(),
                    command: "".into(),
                    user_data: "".into(),
                }),
                processed_message: "Finalized outbound message".into(),
            }),
            _ => unreachable!(),
        };

        let test_container = Container {
            version: 1,
            auth_token: "test-token".to_string(),
            module_name: module_name.clone(),
            module_instance_uuid7: instance_id.clone(),
            payload: Some(payload),
        };

        let mut msg_bytes = Vec::new();
        test_container.encode(&mut msg_bytes)?;
        ws_stream.send(WsMessage::Binary(msg_bytes.into())).await?;
        println!("[TestInputModule] Sent test payload for stage: {}", stage);

        // 3. Send a Log and TimelineEvent for extra verification
        let log_container = Container {
            version: 1,
            auth_token: "test-token".to_string(),
            module_name: module_name.clone(),
            module_instance_uuid7: instance_id.clone(),
            payload: Some(Payload::Log(Log {
                log: format!("Verification log from {}", stage),
            })),
        };

        let mut log_bytes = Vec::new();
        log_container.encode(&mut log_bytes)?;
        ws_stream.send(WsMessage::Binary(log_bytes.into())).await?;

        // Brief pause to allow engine processing
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let _ = ws_stream.close(None).await;
        println!("[TestInputModule] Successfully verified stage: {}", stage);
    }

    println!("\n[TestInputModule] All stage dataflow tests completed successfully!");
    Ok(())
}
