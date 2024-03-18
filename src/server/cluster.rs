use std::collections::HashMap;

use tokio::net::TcpStream;
use tokio::task::JoinHandle;

pub use crate::prelude::*;

use super::commands::SetData;

pub type MasterAddr = (String, u16);
#[allow(unused)]
pub enum Message {
    AddNewSlave(TcpStream),
    Set(SetData),
}

pub struct Actor {
    slaves: Option<HashMap<usize, JoinHandle<()>>>,
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
            slaves: None,
        }
    }

    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                Message::AddNewSlave(slave_stream) => {
                    let slaves = self.slaves.get_or_insert(HashMap::new());
                    let join_handle = self.slave_handler.start_slave(slave_stream).await;
                    // TODO: cover remove & drop & close of handle
                    // slaves.insert(slave_stream, v)
                }
                Message::Set(set_data) => {
                    _ = self
                        .slave_handler
                        .send(super::slave::Message::Set(set_data))
                        .await
                        .expect("todo")
                }
            }
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
