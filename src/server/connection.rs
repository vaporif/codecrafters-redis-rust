use std::net::SocketAddr;

use futures::{sink::SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;

use super::{
    cluster,
    codec::{RespCodec, RespTcpStream},
    commands::*,
    error::TransportError,
    executor::{MasterInfo, ServerMode},
    storage,
};
use crate::{prelude::*, server::rdb::Rdb, ExecutorMessenger};

#[allow(unused)]
#[derive(DebugExtras)]
pub struct ConnectionActor {
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
    stream: RespTcpStream,
}

#[allow(unused)]
enum ConnectionResult {
    Dropped,
    SwitchToSlaveMode,
}

impl ConnectionActor {
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

    #[instrument]
    pub async fn run(mut self) -> anyhow::Result<()> {
        trace!("retrieving new message from client");
        match self.handle_connection().await? {
            ConnectionResult::SwitchToSlaveMode => {
                self.cluster_hnd
                    .send(cluster::Message::AddNewSlave((self.socket, self.stream)))
                    .await
                    .context("sending add new slave")?;
                Ok(())
            }
            ConnectionResult::Dropped => self.stream.close().await.context("closing stream"),
        }
    }

    async fn get_replica_count(&mut self) -> anyhow::Result<u64> {
        let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
        self.cluster_hnd
            .send(cluster::Message::GetSlaveCount {
                channel: reply_channel_tx,
            })
            .await
            .context("get slave count")?;

        let count = reply_channel_rx
            .await
            .context("waiting for reply for wait")?;

        Ok(count)
    }

    async fn handle_connection(&mut self) -> Result<ConnectionResult, TransportError> {
        loop {
            match self.stream.next().await {
                Some(command) => {
                    let command = match command {
                        Ok(command) => command,
                        Err(TransportError::UnknownCommand) => {
                            self.stream
                                .send(RedisMessage::Err("unknown command".to_string()))
                                .await?;
                            continue;
                        }
                        Err(e) => Err(e)?,
                    };

                    trace!("command received {:?}", command);

                    match command {
                        RedisMessage::Ping(_) => self.stream.send(RedisMessage::Pong).await?,
                        RedisMessage::Echo(echo_string) => {
                            self.stream
                                .send(RedisMessage::EchoResponse(echo_string))
                                .await?
                        }
                        // TODO: maybe observable?
                        RedisMessage::Wait {
                            replica_count: requested_replicas,
                            timeout,
                        } => {
                            let mut interval =
                                tokio::time::interval(tokio::time::Duration::from_millis(50));
                            loop {
                                tokio::select! {
                                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(timeout)) => {
                                        let replica_count =  self.get_replica_count().await?;
                                        self.stream.send(RedisMessage::WaitReply { replica_count}).await?;
                                        break;
                                    }
                                    _ = interval.tick() => {
                                        let replica_count = self.get_replica_count().await?;
                                        if replica_count >= requested_replicas {
                                            self.stream.send(RedisMessage::WaitReply { replica_count }).await?;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        RedisMessage::Set(data) => {
                            let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                            self.storage_hnd
                                .send(storage::Message::Set {
                                    data,
                                    channel: Some(reply_channel_tx),
                                })
                                .await
                                .context("sending set store command")?;

                            let message =
                                match reply_channel_rx.await.context("waiting for reply for set") {
                                    Ok(_) => RedisMessage::Ok,
                                    Err(e) => RedisMessage::Err(format!("error {:?}", e)),
                                };

                            self.stream.send(message).await?
                        }
                        RedisMessage::ReplConfCapa { .. } => {
                            self.stream.send(RedisMessage::Ok).await?
                        }
                        RedisMessage::ReplConfPort { .. } => {
                            self.stream.send(RedisMessage::Ok).await?
                        }
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
                                let db_message = RedisMessage::DbTransfer(db.to_vec());

                                self.stream.send(db_message).await?;

                                return Ok(ConnectionResult::SwitchToSlaveMode);
                            }
                            _ => todo!(),
                        },
                        RedisMessage::Get(key) => {
                            let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                            self.storage_hnd
                                .send(storage::Message::Get {
                                    key,
                                    channel: reply_channel_tx,
                                })
                                .await
                                .context("sending set store command")?;

                            let message =
                                match reply_channel_rx.await.context("waiting for reply for get") {
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
                        _ => {
                            self.stream
                                .send(RedisMessage::Err(
                                    "command could not be processed".to_string(),
                                ))
                                .await?
                        }
                    }
                }
                None => return Ok(ConnectionResult::Dropped),
            }
        }
    }
}

pub fn spawn_actor(
    socket: SocketAddr,
    stream: TcpStream,
    executor_messenger: ExecutorMessenger,
    storage_hnd: storage::ActorHandle,
    cluster_hnd: cluster::ActorHandle,
    server_mode: ServerMode,
) {
    tokio::spawn(async move {
        let actor = super::connection::ConnectionActor::new(
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

        trace!("generic redis connection stopped");
    });
}
