use std::net::SocketAddr;

use futures::SinkExt;

use crate::prelude::*;

use super::{
    codec::RespStream,
    commands::{RedisMessage, SetData},
};

#[allow(unused)]
#[derive(Clone, Debug)]
pub enum Message {
    Set(SetData),
}

#[allow(unused)]
#[derive(DebugExtras)]
struct Actor {
    socket: SocketAddr,
    #[debug_ignore]
    slave_stream: RespStream,
    #[debug_ignore]
    receive: tokio::sync::broadcast::Receiver<Message>,
}

impl Actor {
    async fn new(
        socket: SocketAddr,
        slave_stream: RespStream,
        receive: tokio::sync::broadcast::Receiver<Message>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            socket,
            slave_stream,
            receive,
        })
    }

    #[instrument]
    async fn run(&mut self) {
        trace!("switched to slave mode");
        while let Ok(message) = self.receive.recv().await {
            trace!("received {:?}", message);
            match message {
                Message::Set(set_data) => {
                    self.slave_stream
                        .send(RedisMessage::Set(set_data))
                        .await
                        .expect("sent set to slave");
                }
            }

            trace!("slave processed messages");
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
        socket: SocketAddr,
        slave_stream: RespStream,
    ) -> anyhow::Result<()> {
        let mut actor = Actor::new(socket, slave_stream, self.broadcast.subscribe()).await?;
        tokio::spawn(async move {
            actor.run().await;
        });

        Ok(())
    }

    pub async fn send(
        &self,
        message: Message,
    ) -> Result<usize, tokio::sync::broadcast::error::SendError<Message>> {
        let res = self.broadcast.send(message);

        if let Ok(count) = res {
            trace!("broadcasted to {count} of slaves");
        }

        res
    }
}
