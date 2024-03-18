use crate::prelude::*;
use anyhow::Context;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::{codec::RespCodec, commands::RedisMessage};

pub struct Actor {
    master_stream: Framed<TcpStream, RespCodec>,
}

impl Actor {
    #[instrument]
    pub async fn new(master_stream: TcpStream) -> Self {
        let stream = Framed::new(master_stream, RespCodec);
        Self {
            master_stream: stream,
        }
    }

    #[instrument(skip(self))]
    pub async fn replicate_from_scratch(mut self, port: u16) -> anyhow::Result<TcpStream> {
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

        Ok(self.master_stream.into_inner())
    }
}
