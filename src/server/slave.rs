use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::prelude::*;

use super::{
    codec::RespCodec,
    commands::{RedisMessage, SetData},
};

#[allow(unused)]
#[derive(Clone)]
pub enum Message {
    Set(SetData),
}

struct Actor {
    slave_stream: Framed<TcpStream, RespCodec>,
    receive: tokio::sync::broadcast::Receiver<Message>,
}

impl Actor {
    fn new(slave_stream: TcpStream, receive: tokio::sync::broadcast::Receiver<Message>) -> Self {
        let slave_stream = Framed::new(slave_stream, RespCodec);
        Self {
            slave_stream,
            receive,
        }
    }

    async fn run(&mut self) {
        while let Ok(message) = self.receive.recv().await {
            match message {
                Message::Set(set_data) => {
                    self.slave_stream
                        .send(RedisMessage::Set(set_data))
                        .await
                        .expect("todo");
                }
            }
        }
    }
}

pub struct ActorHandle {
    sender: tokio::sync::broadcast::Sender<Message>,
}

#[allow(unused)]
impl ActorHandle {
    pub fn new(slave_stream: TcpStream) -> Self {
        // TODO: limit replicas
        let (sender, receive) = tokio::sync::broadcast::channel(40);

        let mut actor = Actor::new(slave_stream, receive);
        tokio::spawn(async move {
            tracing::trace!("slave actor started");
            loop {
                actor.run().await;
            }
        });

        Self { sender }
    }

    pub async fn send(
        &self,
        message: Message,
    ) -> Result<usize, tokio::sync::broadcast::error::SendError<Message>> {
        self.sender.send(message)
    }
}
