use crate::ipc::{self, Connection, Data};
use crate::server::connection::{AuthConnType, AUTHED_CONNS};
use hbb_common::{log, tokio};

pub async fn start_status_service() {
    match ipc::new_listener("_status").await {
        Ok(mut incoming) => {
            log::info!("Status IPC service started");
            while let Some(result) = incoming.next().await {
                match result {
                    Ok(stream) => {
                        tokio::spawn(handle_status_query(Connection::new(stream)));
                    }
                    Err(err) => {
                        log::error!("Failed to accept status IPC connection: {:?}", err);
                    }
                }
            }
        }
        Err(err) => {
            log::error!("Failed to start status IPC service: {}", err);
        }
    }
}

async fn handle_status_query(mut stream: Connection) {
    let state = get_status_json();
    if let Err(e) = stream.send(&Data::StatusResponse(state)).await {
        log::error!("Failed to send status response: {}", e);
    }
}

fn get_status_json() -> String {
    let authed_conns = AUTHED_CONNS.lock().unwrap();
    let remote_active = authed_conns
        .iter()
        .any(|c| c.conn_type == AuthConnType::Remote);
    let block_input_active = authed_conns
        .iter()
        .any(|c| c.conn_type == AuthConnType::Remote && c.block_input);

    serde_json::json!({
        "remote_connected": remote_active,
        "block_input_active": block_input_active,
        "connection_count": authed_conns.len(),
    })
    .to_string()
}