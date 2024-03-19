use crate::{prelude::*, MasterAddr};
use anyhow::Context;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::{
    codec::{RespCodec, RespStream},
    commands::RedisMessage,
    error::TransportError,
    storage,
};

pub struct Actor {
    master_stream: RespStream,
    storage_hnd: storage::ActorHandle,
}

impl Actor {
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
            .context("expecting pong response")??
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
            .context("expecting repl response")??
        else {
            bail!("expecting ok reply")
        };

        self.master_stream
            .send(RedisMessage::ReplConfCapa {
                capa: "psync2".to_string(),
            })
            .await?;

        let RedisMessage::Ok = self
            .master_stream
            .next()
            .await
            .context("expecting repl response")??
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
            .context("expecting fullserync response")??
        else {
            bail!("expecting fullresync reply")
        };

        self.handle_master_connection().await?;

        Ok(())
    }

    async fn handle_master_connection(&mut self) -> anyhow::Result<(), TransportError> {
        while let Some(command) = self.master_stream.next().await {
            let command = command?;
            match command {
                RedisMessage::Set(set_data) => {
                    self.storage_hnd
                        .send(storage::Message::Set(set_data, None))
                        .await?;
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
    port: u16,
    on_complete_tx: tokio::sync::oneshot::Sender<TcpStream>,
) {
    tokio::spawn(async move {
        let master_stream = tokio::net::TcpStream::connect(master_addr)
            .await
            .expect("failed to connect to master");
        let actor = super::replication::Actor::new(master_stream, storage_hnd).await;
        actor.run(port).await.expect("failed to replicate");
    });
}
