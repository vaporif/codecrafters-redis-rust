use futures::{sink::SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;

use super::{
    codec::RespCodec,
    commands::*,
    error::TransportError,
    main_loop::{MasterInfo, ServerMode},
    storage_actor::{self, StorageActorHandle},
};
use crate::{prelude::*, server::rdb::Rdb};

#[derive(DebugExtras)]
#[allow(unused)]
pub struct ConnectionActor {
    tcp_stream: Framed<TcpStream, RespCodec>,
    #[debug_ignore]
    server_mode: ServerMode,
    #[debug_ignore]
    storage_handle: StorageActorHandle,
}

impl ConnectionActor {
    pub fn new(
        tcp_stream: TcpStream,
        server_mode: ServerMode,
        storage_handle: StorageActorHandle,
    ) -> Self {
        let tcp_stream = Framed::new(tcp_stream, RespCodec);
        Self {
            tcp_stream,
            server_mode,
            storage_handle,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            match self.handle_connection().await {
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

    async fn handle_connection(&mut self) -> Result<(), TransportError> {
        let command = self.next_command().await?;

        let message = match command {
            RedisMessage::Ping(_) => RedisMessage::Pong,
            RedisMessage::Echo(echo_string) => RedisMessage::EchoResponse(echo_string),
            RedisMessage::Set(set_data) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                self.storage_handle
                    .send(storage_actor::Message::Set(set_data, reply_channel_tx))
                    .await
                    .context("sending set store command")?;

                match reply_channel_rx.await.context("waiting for reply for set") {
                    Ok(_) => RedisMessage::Ok,
                    Err(e) => RedisMessage::Err(format!("error {:?}", e)),
                }
            }
            // TODO: protocol is not stateless, need to rework
            RedisMessage::ReplConfCapa { .. } => RedisMessage::Ok,
            RedisMessage::ReplConfPort { .. } => RedisMessage::Ok,
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

                    self.tcp_stream.send(resync_msq).await?;

                    let db = Rdb::empty();
                    RedisMessage::DbTransfer(db.to_vec())
                }
                _ => todo!(),
            },
            RedisMessage::Get(key) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                self.storage_handle
                    .send(storage_actor::Message::Get(key, reply_channel_tx))
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

        self.tcp_stream.send(message).await?;

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

// WIP
// struct ConnectionActorHandle {
//     server_mode: ServerMode,
//     sender: async_channel::Sender<NewConnectionMessage>,
// }

// impl ConnectionActorHandle {
//     pub fn new(server_mode: ServerMode, max_connections: usize) -> Self {
//         let (sender, receiver) = async_channel::bounded(max_connections);

//         let mut actor = ConnectionActor::new(receive);
//         tokio::spawn(async move {
//             trace!("storage actor started");
//             loop {
//                 actor.run().await;
//             }
//         });

//         Self { sender }
//     }
// }

// enum Message {
//     NewConnection(tokio::net::TcpStream),
// }

// struct NewConnectionMessage {
//     server_mode: ServerMode,
//     stream: tokio::net::TcpStream,
// }
