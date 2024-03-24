use std::net::SocketAddr;

use futures::SinkExt;

use crate::prelude::*;

use super::{
    codec::RespTcpStream,
    commands::{RedisMessage, SetData},
};

#[allow(unused)]
#[derive(Clone, Debug)]
pub enum Message {
    Set(SetData),
}

#[allow(unused)]
#[derive(DebugExtras)]
struct SlaveConnectionActor {
    socket: SocketAddr,
    #[debug_ignore]
    stream: RespTcpStream,
    #[debug_ignore]
    receive: tokio::sync::broadcast::Receiver<Message>,
}

impl SlaveConnectionActor {
    async fn new(
        socket: SocketAddr,
        stream: RespTcpStream,
        receive: tokio::sync::broadcast::Receiver<Message>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            socket,
            stream,
            receive,
        })
    }

    #[instrument]
    async fn run(&mut self) -> anyhow::Result<()> {
        trace!("switched to slave mode");
        while let Ok(message) = self.receive.recv().await {
            trace!("received {:?}", message);
            match message {
                Message::Set(set_data) => {
                    self.stream
                        .send(RedisMessage::Set(set_data))
                        .await
                        .context("sent set to slave")?;
                }
            }

            trace!("slave processed messages");
        }

        Ok(())
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
        slave_stream: RespTcpStream,
    ) -> anyhow::Result<()> {
        let mut actor =
            SlaveConnectionActor::new(socket, slave_stream, self.broadcast.subscribe()).await?;
        tokio::spawn(async move { actor.run().await });

        Ok(())
    }

    pub async fn send(&self, message: Message) {
        match self.broadcast.send(message) {
            Ok(count) => trace!("broadcasted to {count} of slaves"),
            Err(_) => info!("no slaves connected"),
        }
    }
}
