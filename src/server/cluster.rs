use std::{collections::HashMap, net::TcpStream};

pub use crate::prelude::*;
use std::net::SocketAddr;

use super::commands::SetData;

pub type MasterAddr = (String, u16);
#[allow(unused)]
pub enum Message {
    AddNewSlave(TcpStream),
    Set(SetData),
}

pub struct Actor {
    slaves: Option<HashMap<SocketAddr, super::slave::ActorHandle>>,
    // broadcast: super::
    receiver: tokio::sync::mpsc::UnboundedReceiver<Message>,
}

#[allow(unused)]
impl Actor {
    fn new(receiver: tokio::sync::mpsc::UnboundedReceiver<Message>) -> Self {
        Actor {
            receiver,
            slaves: None,
        }
    }

    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                Message::AddNewSlave(slave_stream) => {
                    let slaves = self.slaves.get_or_insert(HashMap::new());
                }
                Message::Set(set_data) => {}
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
        let (sender, receive) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut actor = Actor::new(receive);
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
