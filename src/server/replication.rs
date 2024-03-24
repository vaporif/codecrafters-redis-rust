use crate::{prelude::*, ExecutorMessenger, MasterAddr};
use anyhow::Context;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::{
    codec::{RespCodec, RespTcpStream},
    commands::RedisMessage,
    error::TransportError,
    storage,
};

pub struct ReplicationActor {
    master_stream: RespTcpStream,
    storage_hnd: storage::ActorHandle,
}

impl ReplicationActor {
    pub async fn new(master_stream: TcpStream, storage_hnd: storage::ActorHandle) -> Self {
        let stream = Framed::new(master_stream, RespCodec);
        Self {
            storage_hnd,
            master_stream: stream,
        }
    }

    #[instrument(skip(self))]
    pub async fn run(mut self, port: u16) -> anyhow::Result<()> {
        self.master_stream.send(RedisMessage::Ping(None)).await?;

        let RedisMessage::Pong = self
            .master_stream
            .next()
            .await
            .context("expecting repl pong response")?
            .context("expecting pong response")?
        else {
            bail!("expecting pong reply")
        };

        self.master_stream
            .send(RedisMessage::ReplConfPort { port })
            .await?;

        let RedisMessage::Ok = self
            .master_stream
            .next()
            .await
            .context("expecting repl ok response")?
            .context("expecting ok response")?
        else {
            bail!("expecting ok reply")
        };

        self.master_stream
            .send(RedisMessage::ReplConfCapa {
                capa: "psync2".to_string(),
            })
            .await
            .context("expecting send replconf")?;

        let RedisMessage::Ok = self
            .master_stream
            .next()
            .await
            .context("expecting repl response")?
            .context("expecting redis messsage")?
        else {
            bail!("expecting ok reply")
        };

        self.master_stream
            .send(RedisMessage::Psync {
                replication_id: "?".to_string(),
                offset: -1,
            })
            .await?;

        let RedisMessage::FullResync {
            replication_id: _,
            offset: _,
        } = self
            .master_stream
            .next()
            .await
            .context("expecting repl fullserync response")?
            .context("expecting fullresync resp")?
        else {
            bail!("expecting fullresync reply")
        };

        let expected_db = self.master_stream.next().await;

        match expected_db {
            Some(Ok(RedisMessage::DbTransfer(_))) => {
                info!("db received");
            }
            _ => {
                info!("db not sent back");
            }
        }

        trace!("connected to master");

        if let Err(error) = self.handle_master_connection().await {
            self.master_stream
                .send(RedisMessage::Err(format!("{:?}", error)))
                .await?;
            Err(error)?;
        }

        Ok(())
    }

    async fn handle_master_connection(&mut self) -> anyhow::Result<(), TransportError> {
        while let Some(command) = self.master_stream.next().await {
            let command = command.context("reading master connection")?;
            match command {
                RedisMessage::Set(data) => {
                    self.storage_hnd
                        .send(storage::Message::Set {
                            data,
                            channel: None,
                        })
                        .await
                        .context("sending set command")?;
                }
                RedisMessage::ReplConfGetAck => {
                    self.master_stream
                        .send(RedisMessage::ReplConfAck { offset: 0 })
                        .await
                        .context("sending replconf")?;
                }
                s => Err(anyhow!("unsupported command {:?}", s))?,
            }
        }

        Ok(())
    }
}

#[allow(unused)]
pub fn spawn_actor(
    master_addr: MasterAddr,
    storage_hnd: storage::ActorHandle,
    executor_messenger: ExecutorMessenger,
    port: u16,
    on_complete_tx: tokio::sync::oneshot::Sender<TcpStream>,
) {
    tokio::spawn(async move {
        let master_stream = tokio::net::TcpStream::connect(master_addr)
            .await
            .expect("failed to connect to master");
        let actor = super::replication::ReplicationActor::new(master_stream, storage_hnd).await;

        executor_messenger
            .propagate_fatal_errors(actor.run(port).await.context("failed to replicate"))
            .await;
    });
}
