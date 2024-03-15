use async_channel::Sender;
use futures::{sink::SinkExt, StreamExt};
use serde_resp::{bulk, bulk_null, err_str, RESP};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;

use super::{codec::RespCodec, commands::*, core_listener::ServerMode};
use crate::prelude::*;

#[derive(DebugExtras)]
#[allow(unused)]
pub struct ConnectionActor {
    pub socket: SocketAddr,
    #[debug_ignore]
    tcp_stream: Framed<TcpStream, RespCodec>,
    #[debug_ignore]
    server_mode: ServerMode,
}

impl ConnectionActor {
    pub fn new(socket: SocketAddr, tcp_stream: TcpStream, server_mode: ServerMode) -> Self {
        let tcp_stream = Framed::new(tcp_stream, RespCodec);
        Self {
            socket,
            tcp_stream,
            server_mode,
        }
    }

    #[instrument(skip(store_access_tx))]
    pub async fn run_actor(&mut self, store_access_tx: Sender<StoreCommand>) -> anyhow::Result<()> {
        loop {
            let command = self
                .next_command()
                .await
                .context("next command from client")?;

            let response_message = match command {
                RedisMessage::Ping(_) => RedisMessage::Pong.into(),
                RedisMessage::Echo(echo_string) => bulk!(echo_string.into_bytes()),
                RedisMessage::Set(set_data) => {
                    let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                    store_access_tx
                        .send(StoreCommand::Set(set_data, reply_channel_tx))
                        .await
                        .context("sending set store command")?;

                    match reply_channel_rx.await.context("waiting for reply for set") {
                        Ok(_) => RedisMessage::Ok.into(),
                        Err(e) => err_str!(format!("error {:?}", e)),
                    }
                }
                RedisMessage::ReplConfCapa { .. } => RedisMessage::Ok.into(),
                RedisMessage::ReplConfPort { .. } => RedisMessage::Ok.into(),
                RedisMessage::Get(key) => {
                    let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                    store_access_tx
                        .send(StoreCommand::Get(key, reply_channel_tx))
                        .await
                        .context("sending set store command")?;

                    match reply_channel_rx.await.context("waiting for reply for get") {
                        Ok(result) => match result {
                            Some(value) => bulk!(value.into_bytes()),
                            None => bulk_null!(),
                        },
                        Err(e) => err_str!(format!("error {:?}", e)),
                    }
                }
                RedisMessage::Info(info_data) => match info_data {
                    InfoCommand::Replication => self.server_mode.to_resp(),
                },
                s => bail!("unexpected command {:?}", s),
            };

            self.send_response(response_message)
                .await
                .context("sending response")?;
        }
    }

    // TODO: cover connection shutdown
    #[tracing::instrument(skip(self))]
    async fn next_command(&mut self) -> anyhow::Result<RedisMessage> {
        self.tcp_stream
            .next()
            .await
            .map(|m| m.context("tcp stream closed"))
            .context("next command message")?
    }

    #[tracing::instrument(skip(self))]
    async fn send_response(&mut self, message: RESP) -> anyhow::Result<()> {
        self.tcp_stream
            .send(message)
            .await
            .context("sending message")
    }
}

impl ServerMode {
    fn to_resp(&self) -> RESP {
        match self {
            ServerMode::Master {
                master_replid,
                master_repl_offset,
            } => {
                let role = "role:master".to_string();
                let master_replid = format!("master_replid:{master_replid}");
                let master_repl_offset = format!("master_repl_offset:{master_repl_offset}");
                let string = [role, master_replid, master_repl_offset].join("\n");
                bulk!(string.into_bytes())
            }
            ServerMode::Slave(_) => bulk!(b"role:slave".to_vec()),
        }
    }
}
