use std::net::SocketAddr;

use futures::{sink::SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;

use super::{
    cluster,
    codec::{RespCodec, RespStream},
    commands::*,
    error::TransportError,
    executor::{MasterInfo, ServerMode},
    storage,
};
use crate::{prelude::*, server::rdb::Rdb, ExecutorMessenger};

#[allow(unused)]
#[derive(DebugExtras)]
pub struct Actor {
    #[debug_ignore]
    executor_messenger: ExecutorMessenger,
    #[debug_ignore]
    storage_hnd: storage::ActorHandle,
    #[debug_ignore]
    cluster_hnd: cluster::ActorHandle,
    #[debug_ignore]
    server_mode: ServerMode,
    socket: SocketAddr,
    #[debug_ignore]
    stream: RespStream,
}

enum ConnectionResult {
    Handled,
    SwitchToSlaveMode,
}

impl Actor {
    pub fn new(
        executor_messenger: ExecutorMessenger,
        storage_hnd: storage::ActorHandle,
        cluster_hnd: cluster::ActorHandle,
        server_mode: ServerMode,
        stream_socket: SocketAddr,
        stream: TcpStream,
    ) -> Self {
        let stream = Framed::new(stream, RespCodec);
        Self {
            storage_hnd,
            cluster_hnd,
            server_mode,
            executor_messenger,
            socket: stream_socket,
            stream,
        }
    }

    #[instrument()]
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            trace!("retrieving new message from client");
            match self.handle_connection().await {
                Ok(connection_result) => match connection_result {
                    ConnectionResult::Handled => trace!("connection request handled"),
                    ConnectionResult::SwitchToSlaveMode => {
                        self.cluster_hnd
                            .send(cluster::Message::AddNewSlave((self.socket, self.stream)))
                            .await
                            .context("sending add new slave")?;
                        return Ok(());
                    }
                },
                Err(err) => {
                    self.stream.close().await.context("closing stream")?;
                    match err {
                        TransportError::EmptyResponse() => {
                            trace!("connection closed");
                            return Ok(());
                        }
                        s => {
                            trace!("connection error {:?}", s)
                        }
                    }
                }
            };
        }
    }

    async fn handle_connection(&mut self) -> Result<ConnectionResult, TransportError> {
        let command = self.next_command().await?;
        trace!("command received {:?}", command);

        match command {
            RedisMessage::Ping(_) => self.stream.send(RedisMessage::Pong).await?,
            RedisMessage::Echo(echo_string) => {
                self.stream
                    .send(RedisMessage::EchoResponse(echo_string))
                    .await?
            }
            RedisMessage::Set(set_data) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                self.storage_hnd
                    .send(storage::Message::Set(set_data, reply_channel_tx))
                    .await
                    .context("sending set store command")?;

                let message = match reply_channel_rx.await.context("waiting for reply for set") {
                    Ok(_) => RedisMessage::Ok,
                    Err(e) => RedisMessage::Err(format!("error {:?}", e)),
                };

                self.stream.send(message).await?
            }
            RedisMessage::ReplConfCapa { .. } => self.stream.send(RedisMessage::Ok).await?,
            RedisMessage::ReplConfPort { .. } => self.stream.send(RedisMessage::Ok).await?,
            RedisMessage::Psync {
                replication_id,
                offset,
            } => match (replication_id.as_str(), offset) {
                ("?", -1) => {
                    let ServerMode::Master(MasterInfo {
                        ref master_replid, ..
                    }) = self.server_mode
                    else {
                        return Err(TransportError::Other(anyhow!(
                            "server in slave mode".to_string()
                        )));
                    };

                    let resync_msq = RedisMessage::FullResync {
                        replication_id: master_replid.clone(),
                        offset: 0,
                    };

                    self.stream.send(resync_msq).await?;

                    let db = Rdb::empty();
                    let db = RedisMessage::DbTransfer(db.to_vec());

                    self.stream.send(db).await?;

                    return Ok(ConnectionResult::SwitchToSlaveMode);
                }
                _ => todo!(),
            },
            RedisMessage::Get(key) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                self.storage_hnd
                    .send(storage::Message::Get(key, reply_channel_tx))
                    .await
                    .context("sending set store command")?;

                let message = match reply_channel_rx.await.context("waiting for reply for get") {
                    Ok(result) => match result {
                        Some(value) => RedisMessage::CacheFound(value.into_bytes()),
                        None => RedisMessage::CacheNotFound,
                    },
                    Err(e) => RedisMessage::Err(format!("error {:?}", e)),
                };

                self.stream.send(message).await?
            }
            RedisMessage::Info(info_data) => match info_data {
                InfoCommand::Replication => {
                    self.stream
                        .send(RedisMessage::InfoResponse(self.server_mode.clone()))
                        .await?
                }
            },
            e => {
                self.stream
                    .send(RedisMessage::Err(
                        format!("unknown command {:?}", e).to_string(),
                    ))
                    .await?
            }
        };

        Ok(ConnectionResult::Handled)
    }

    #[tracing::instrument(skip(self))]
    async fn next_command(&mut self) -> Result<RedisMessage, TransportError> {
        let message = self.stream.next().await;
        match message {
            Some(r) => r,
            None => Err(TransportError::EmptyResponse()),
        }
    }

    #[tracing::instrument(skip(self))]
    async fn send_response(&mut self, message: RedisMessage) -> Result<(), TransportError> {
        Ok(self.stream.send(message).await.context("sending message")?)
    }
}

// impl Drop for Actor {
//     fn drop(&mut self) {
//         self.stream.close();
//     }
// }

pub fn spawn_actor(
    socket: SocketAddr,
    stream: TcpStream,
    executor_messenger: ExecutorMessenger,
    storage_hnd: storage::ActorHandle,
    cluster_hnd: cluster::ActorHandle,
    server_mode: ServerMode,
) {
    tokio::spawn(async move {
        let actor = super::connection::Actor::new(
            executor_messenger,
            storage_hnd,
            cluster_hnd,
            server_mode,
            socket,
            stream,
        );
        if let Err(err) = actor.run().await {
            error!("connection failure {:?}", err);
        }

        trace!("actor exited");
    });
}
