use std::usize;

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::prelude::*;

use super::{
    codec::RespCodec,
    commands::{RedisMessage, SetData},
};

#[allow(unused)]
#[derive(Clone, Debug)]
pub enum Message {
    Set(SetData),
}

struct Actor {
    slave_stream: Framed<TcpStream, RespCodec>,
    receive: tokio::sync::broadcast::Receiver<Message>,
}

impl Actor {
    async fn new(
        slave_stream: Framed<TcpStream, RespCodec>,
        receive: tokio::sync::broadcast::Receiver<Message>,
    ) -> anyhow::Result<Self> {
        trace!("connecting to new slave");
        Ok(Self {
            slave_stream,
            receive,
        })
    }

    #[instrument(skip(self))]
    async fn run(&mut self) {
        while let Ok(message) = self.receive.recv().await {
            match message {
                Message::Set(set_data) => {
                    trace!("forwarding set to replica");
                    self.slave_stream
                        .send(RedisMessage::Set(set_data))
                        .await
                        .expect("sent set to slave");

                    trace!("replica processed");
                }
            }
        }
    }
}

pub struct ActorHandle {
    broadcast: tokio::sync::broadcast::Sender<Message>,
}

#[allow(unused)]
// TODO: limit replicas
impl ActorHandle {
    pub fn new(broadcast: tokio::sync::broadcast::Sender<Message>) -> Self {
        Self { broadcast }
    }

    pub async fn start_slave(
        &self,
        slave_stream: Framed<TcpStream, RespCodec>,
    ) -> anyhow::Result<()> {
        let mut actor = Actor::new(slave_stream, self.broadcast.subscribe()).await?;
        tokio::spawn(async move {
            tracing::trace!("slave actor started");
            actor.run().await;
        });

        Ok(())
    }

    pub async fn send(
        &self,
        message: Message,
    ) -> Result<usize, tokio::sync::broadcast::error::SendError<Message>> {
        self.broadcast.send(message)
    }
}
