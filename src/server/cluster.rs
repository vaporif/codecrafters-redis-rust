use std::{collections::HashSet, net::IpAddr};

use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub use crate::prelude::*;

use super::{codec::RespCodec, commands::SetData};

pub type MasterAddr = (String, u16);
#[derive(Debug)]
#[allow(unused)]
pub enum Message {
    AddNewSlave(Framed<TcpStream, RespCodec>),
    Set(SetData),
}

pub struct Actor {
    slaves: Option<HashSet<IpAddr>>,
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

    #[instrument(skip(self))]
    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            trace!("new message {:?}", &message);
            match message {
                Message::AddNewSlave(stream) => {
                    let slaves = self.slaves.get_or_insert(HashSet::new());
                    if let Err(error) = self.slave_handler.start_slave(stream).await {
                        error!("failed to connect {:?}", error);
                    }

                    trace!("slave running");

                    // TODO: cover remove & drop & close of handle
                    // _ = slaves.insert(stream.sock);
                    // trace!("slave added, slaves in collection {:?}", slaves);
                }
                Message::Set(set_data) => {
                    _ = self
                        .slave_handler
                        .send(super::slave::Message::Set(set_data))
                        .await
                        .expect("set completed")
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
