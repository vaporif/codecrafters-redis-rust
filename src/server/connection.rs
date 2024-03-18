use std::net::SocketAddr;

use futures::{sink::SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;

use super::{
    cluster,
    codec::RespCodec,
    commands::*,
    error::TransportError,
    executor::{MasterInfo, ServerMode},
    storage,
};
use crate::{prelude::*, server::rdb::Rdb, ExecutorMessenger};

#[derive(Clone)]
pub struct NewConnection {
    pub storage_hnd: storage::ActorHandle,
    pub cluster_hnd: cluster::ActorHandle,
    pub server_mode: ServerMode,
}

#[allow(unused)]
pub struct Actor {
    slave_listening_port: Option<u16>,
    executor_messenger: ExecutorMessenger,
    stream_socket: SocketAddr,
    stream: Framed<TcpStream, RespCodec>,
}

impl Actor {
    pub fn new(
        executor_messenger: ExecutorMessenger,
        stream_socket: SocketAddr,
        stream: TcpStream,
    ) -> Self {
        let stream = Framed::new(stream, RespCodec);
        Self {
            slave_listening_port: None,
            executor_messenger,
            stream_socket,
            stream,
        }
    }

    pub async fn run(&mut self, new_connection: NewConnection) -> anyhow::Result<()> {
        loop {
            let new_connection = new_connection.clone();
            match self.handle_connection(new_connection).await {
                Ok(_) => trace!("connection request handled"),
                Err(err) => match err {
                    TransportError::EmptyResponse() => {
                        self.stream.close().await.context("closing stream")?;
                        trace!("connection closed");
                        return Ok(());
                    }
                    s => Err(anyhow!("Transport error {:?}", s))?,
                },
            }
        }
    }

    async fn handle_connection(
        &mut self,
        new_connection: NewConnection,
    ) -> Result<(), TransportError> {
        let NewConnection {
            storage_hnd,
            server_mode,
            cluster_hnd,
        } = new_connection;

        let command = self.next_command().await?;

        match command {
            RedisMessage::Ping(_) => self.stream.send(RedisMessage::Pong).await?,
            RedisMessage::Echo(echo_string) => {
                self.stream
                    .send(RedisMessage::EchoResponse(echo_string))
                    .await?
            }
            RedisMessage::Set(set_data) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                storage_hnd
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
            RedisMessage::ReplConfPort { port } => {
                self.slave_listening_port = Some(port);
                self.stream.send(RedisMessage::Ok).await?
            }
            RedisMessage::Psync {
                replication_id,
                offset,
            } => match (replication_id.as_str(), offset) {
                ("?", -1) => match self.slave_listening_port {
                    Some(port) => {
                        let ServerMode::Master(MasterInfo {
                            ref master_replid, ..
                        }) = server_mode
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

                        let slave_socket = SocketAddr::new(self.stream_socket.ip(), port);

                        cluster_hnd
                            .send(cluster::Message::AddNewSlave(slave_socket))
                            .await?
                    }
                    None => {
                        self.stream
                            .send(RedisMessage::Err("port unknown".to_string()))
                            .await?
                    }
                },
                _ => todo!(),
            },
            RedisMessage::Get(key) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                storage_hnd
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
                        .send(RedisMessage::InfoResponse(server_mode.clone()))
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

        Ok(())
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

pub fn spawn_actor(
    socket: SocketAddr,
    stream: TcpStream,
    executor_messenger: ExecutorMessenger,
    storage_hnd: storage::ActorHandle,
    cluster_hnd: cluster::ActorHandle,
    server_mode: ServerMode,
) {
    tokio::spawn(async move {
        let new_connection = NewConnection {
            storage_hnd,
            server_mode,
            cluster_hnd,
        };
        let mut actor = super::connection::Actor::new(executor_messenger, socket, stream);
        if let Err(err) = actor.run(new_connection).await {
            error!("connection failure {:?}", err);
        }
    });
}
