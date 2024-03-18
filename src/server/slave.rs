use std::net::SocketAddr;

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
        slave_addr: SocketAddr,
        receive: tokio::sync::broadcast::Receiver<Message>,
    ) -> anyhow::Result<Self> {
        trace!("connecting to new slave {:?}", &slave_addr);
        let slave_stream = TcpStream::connect(slave_addr)
            .await
            .context("failed to connect")?;
        trace!("connected to {:?}", &slave_addr);
        let slave_stream = Framed::new(slave_stream, RespCodec);
        Ok(Self {
            slave_stream,
            receive,
        })
    }

    async fn run(&mut self) {
        while let Ok(message) = self.receive.recv().await {
            match message {
                Message::Set(set_data) => {
                    self.slave_stream
                        .send(RedisMessage::Set(set_data))
                        .await
                        .expect("sent set to slave");
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

    pub async fn start_slave(&self, slave_addr: SocketAddr) -> anyhow::Result<()> {
        let mut actor = Actor::new(slave_addr, self.broadcast.subscribe()).await?;
        tokio::spawn(async move {
            tracing::trace!("slave actor started");
            loop {
                actor.run().await;
            }
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
