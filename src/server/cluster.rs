use std::net::SocketAddr;

pub use crate::prelude::*;

use super::{codec::RespStream, commands::SetData};

pub type MasterAddr = (String, u16);
#[derive(Debug)]
#[allow(unused)]
pub enum Message {
    AddNewSlave((SocketAddr, RespStream)),
    Set(SetData),
}

pub struct Actor {
    slave_handler: super::slave::ActorHandle,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Message>,
}

#[allow(unused)]
impl Actor {
    fn new(
        slave_handler: super::slave::ActorHandle,
        receiver: tokio::sync::mpsc::UnboundedReceiver<Message>,
    ) -> Self {
        Actor {
            slave_handler,
            receiver,
        }
    }

    #[instrument(skip(self))]
    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            trace!("new message {:?}", &message);
            match message {
                Message::AddNewSlave((socket, stream)) => {
                    if let Err(error) = self.slave_handler.start_slave(socket, stream).await {
                        error!("failed to connect {:?}", error);
                    }
                }
                Message::Set(set_data) => {
                    _ = self
                        .slave_handler
                        .send(super::slave::Message::Set(set_data))
                        .await
                }
            }

            trace!("message processed");
        }
    }
}

#[allow(unused)]
#[derive(Clone)]
pub struct ActorHandle {
    sender: tokio::sync::mpsc::UnboundedSender<Message>,
}

#[allow(unused)]
impl ActorHandle {
    pub fn new() -> Self {
        let (broadcast, _) = tokio::sync::broadcast::channel(40);
        let (sender, receive) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut actor = Actor::new(super::slave::ActorHandle::new(broadcast), receive);
            trace!("cluster actor started");
            loop {
                actor.run().await;
            }
        });

        Self { sender }
    }

    pub async fn send(&self, message: Message) -> Result<()> {
        self.sender.send(message).context("sending message")
    }
}
