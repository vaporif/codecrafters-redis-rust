use futures::{sink::SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;

use super::{
    codec::RespCodec,
    commands::*,
    error::TransportError,
    executor::{MasterInfo, ServerMode},
    storage,
};
use crate::{prelude::*, server::rdb::Rdb};

#[derive(Clone)]
pub struct NewConnection {
    pub storage_hnd: storage::ActorHandle,
    pub server_mode: ServerMode,
}

#[derive(DebugExtras)]
#[allow(unused)]
pub struct Actor {
    stream: Framed<TcpStream, RespCodec>,
}

impl Actor {
    pub fn new(stream: TcpStream) -> Self {
        let stream = Framed::new(stream, RespCodec);
        Self { stream }
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
        } = new_connection;

        let command = self.next_command().await?;

        let message = match command {
            RedisMessage::Ping(_) => RedisMessage::Pong,
            RedisMessage::Echo(echo_string) => RedisMessage::EchoResponse(echo_string),
            RedisMessage::Set(set_data) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                storage_hnd
                    .send(storage::Message::Set(set_data, reply_channel_tx))
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
                    RedisMessage::DbTransfer(db.to_vec())
                }
                _ => todo!(),
            },
            RedisMessage::Get(key) => {
                let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                storage_hnd
                    .send(storage::Message::Get(key, reply_channel_tx))
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
                InfoCommand::Replication => RedisMessage::InfoResponse(server_mode.clone()),
            },
            e => RedisMessage::Err(format!("unknown command {:?}", e).to_string()),
        };

        self.stream.send(message).await?;

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
