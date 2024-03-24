use std::net::SocketAddr;

pub use crate::prelude::*;

use super::{codec::RespTcpStream, commands::SetData};

pub type MasterAddr = (String, u16);
#[derive(DebugExtras)]
#[allow(unused)]
pub enum Message {
    AddNewSlave((SocketAddr, RespTcpStream)),
    SlaveDisconnected,
    Set(SetData),
    GetSlaveCount {
        #[debug_ignore]
        channel: tokio::sync::oneshot::Sender<u64>,
    },
}

pub struct Actor {
    replica_rount: u64,
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
            replica_rount: 0,
            slave_handler,
            receiver,
        }
    }

    #[instrument(skip(self))]
    async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            trace!("new message {:?}", &message);
            match message {
                Message::AddNewSlave((socket, mut stream)) => {
                    if let Err(error) = self.slave_handler.run(socket, stream).await {
                        error!("failed to connect {:?}", error);
                    }

                    self.replica_rount += 1;
                }
                Message::SlaveDisconnected => self.replica_rount -= 1,
                Message::GetSlaveCount { channel } => {
                    channel.send(self.replica_rount);
                }
                Message::Set(set_data) => {
                    trace!("sending to slaves");
                    _ = self
                        .slave_handler
                        .send(super::slave::Message::Set(set_data))
                        .await;
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
        let hnd = Self { sender };

        let handle = hnd.clone();

        tokio::spawn(async move {
            let mut actor = Actor::new(super::slave::ActorHandle::new(handle, broadcast), receive);
            trace!("cluster actor started");
            loop {
                actor.run().await;
            }
        });

        hnd
    }

    #[instrument(skip(self))]
    pub async fn send(&self, message: Message) -> Result<()> {
        self.sender.send(message).context("sending message")
    }
}
