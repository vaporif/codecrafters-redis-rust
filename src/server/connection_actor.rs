use async_channel::Sender;
use futures::{sink::SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;

use super::{codec::RespCodec, commands::*, core_listener::ServerMode, error::TransportError};
use crate::prelude::*;

#[derive(DebugExtras)]
#[allow(unused)]
pub struct ConnectionActor {
    pub socket: SocketAddr,
    #[debug_ignore]
    tcp_stream: Framed<TcpStream, RespCodec>,
    #[debug_ignore]
    server_mode: ServerMode,
    #[debug_ignore]
    store_access_tx: Sender<StoreCommand>,
}

impl ConnectionActor {
    pub fn new(
        socket: SocketAddr,
        tcp_stream: TcpStream,
        server_mode: ServerMode,
        store_access_tx: Sender<StoreCommand>,
    ) -> Self {
        let tcp_stream = Framed::new(tcp_stream, RespCodec);
        Self {
            socket,
            tcp_stream,
            server_mode,
            store_access_tx,
        }
    }

    pub async fn run_actor(&mut self) -> anyhow::Result<()> {
        loop {
            match self.hanlde_connection().await {
                Ok(_) => trace!("connection request handled"),
                Err(err) => match err {
                    TransportError::EmptyResponse() => {
                        self.tcp_stream.close().await.context("closing stream")?;
                        trace!("connection closed");
                        return Ok(());
                    }
                    s => Err(anyhow!("Transport error {:?}", s))?,
                },
            }
        }
    }

    async fn hanlde_connection(&mut self) -> Result<(), TransportError> {
        let command = self.next_command().await?;

        let response_message = match command {
            RedisMessage::Ping(_) => RedisMessage::Pong,
            RedisMessage::Echo(echo_string) => RedisMessage::EchoResponse(echo_string),
            RedisMessage::Set(set_data) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                self.store_access_tx
                    .send(StoreCommand::Set(set_data, reply_channel_tx))
                    .await
                    .context("sending set store command")?;

                match reply_channel_rx.await.context("waiting for reply for set") {
                    Ok(_) => RedisMessage::Ok,
                    Err(e) => RedisMessage::Err(format!("error {:?}", e)),
                }
            }
            RedisMessage::ReplConfCapa { .. } => RedisMessage::Ok,
            RedisMessage::ReplConfPort { .. } => RedisMessage::Ok,
            RedisMessage::Psync {
                replication_id,
                offset,
            } => match (replication_id.as_str(), offset) {
                ("?", -1) => {
                    let ServerMode::Master {
                        ref master_replid, ..
                    } = self.server_mode
                    else {
                        return Err(TransportError::Other(anyhow!(
                            "server in slave mode".to_string()
                        )));
                    };

                    RedisMessage::FullResync {
                        replication_id: master_replid.clone(),
                        offset: 0,
                    }
                }
                _ => todo!(),
            },
            RedisMessage::Get(key) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                self.store_access_tx
                    .send(StoreCommand::Get(key, reply_channel_tx))
                    .await
                    .context("sending set store command")?;

                match reply_channel_rx.await.context("waiting for reply for get") {
                    Ok(result) => match result {
                        Some(value) => RedisMessage::CacheFound(value.into_bytes()),
                        None => RedisMessage::CacheNotFound,
                    },
                    Err(e) => RedisMessage::Err(format!("error {:?}", e)),
                }
            }
            RedisMessage::Info(info_data) => match info_data {
                InfoCommand::Replication => RedisMessage::InfoResponse(self.server_mode.clone()),
            },
            e => RedisMessage::Err(format!("unknown command {:?}", e).to_string()),
        };

        self.send_response(response_message).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn next_command(&mut self) -> Result<RedisMessage, TransportError> {
        let message = self.tcp_stream.next().await;
        match message {
            Some(r) => r,
            None => Err(TransportError::EmptyResponse()),
        }
    }

    #[tracing::instrument(skip(self))]
    async fn send_response(&mut self, message: RedisMessage) -> Result<(), TransportError> {
        Ok(self
            .tcp_stream
            .send(message)
            .await
            .context("sending message")?)
    }
}
