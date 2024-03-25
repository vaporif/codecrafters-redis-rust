use std::{net::SocketAddr, usize};

use rand::{distributions::Alphanumeric, Rng};

pub use crate::prelude::*;

use super::{codec::RespTcpStream, commands::SetData};

#[derive(Debug, Clone)]
pub enum ServerMode {
    Master(MasterInfo),
    Slave(MasterAddr),
}

#[derive(Debug, Clone)]
pub struct MasterInfo {
    pub master_replid: String,
    pub master_repl_offset: usize,
}

impl ServerMode {
    fn new(replication_ip: Option<MasterAddr>) -> Self {
        match replication_ip {
            Some(master_addr) => ServerMode::Slave(master_addr),
            None => {
                let random_string: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(40)
                    .map(char::from)
                    .collect();

                ServerMode::Master(MasterInfo {
                    master_replid: random_string,
                    master_repl_offset: 0,
                })
            }
        }
    }
}

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
    GetServerMode {
        #[debug_ignore]
        channel: tokio::sync::oneshot::Sender<ServerMode>,
    },
    AddMasterOffset {
        offset: usize,
    },
}

pub struct Actor {
    server_mode: ServerMode,
    replica_count: u64,
    replica_count_up_to_date: u64,
    slave_handler: super::slave::ActorHandle,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Message>,
}

#[allow(unused)]
impl Actor {
    fn new(
        master_addr: Option<MasterAddr>,
        slave_handler: super::slave::ActorHandle,
        receiver: tokio::sync::mpsc::UnboundedReceiver<Message>,
    ) -> Self {
        let random_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();

        Actor {
            server_mode: ServerMode::new(master_addr),
            replica_count: 0,
            replica_count_up_to_date: 0,
            slave_handler,
            receiver,
        }
    }

    #[instrument(skip(self))]
    async fn run(&mut self) {
        _ = self.replica_count_up_to_date;

        while let Some(message) = self.receiver.recv().await {
            trace!("new message {:?}", &message);
            match message {
                Message::AddNewSlave((socket, mut stream)) => {
                    if let Err(error) = self.slave_handler.run(socket, stream).await {
                        error!("failed to connect {:?}", error);
                    }

                    self.replica_count += 1;
                }
                Message::SlaveDisconnected => self.replica_count -= 1,
                Message::GetSlaveCount { channel } => {
                    channel.send(self.replica_count);
                }
                Message::Set(set_data) => {
                    trace!("sending to slaves");
                    _ = self
                        .slave_handler
                        .send(super::slave::Message::Set(set_data))
                        .await;
                }
                Message::GetServerMode { channel } => {
                    channel.send(self.server_mode.clone());
                }
                Message::AddMasterOffset { offset } => {
                    if let ServerMode::Master(ref mut master_info) = self.server_mode {
                        master_info.master_repl_offset += offset;
                    }
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
    pub fn new(replication_ip: Option<MasterAddr>) -> Self {
        let (broadcast, _) = tokio::sync::broadcast::channel(40);
        let (sender, receive) = tokio::sync::mpsc::unbounded_channel();
        let hnd = Self { sender };

        let handle = hnd.clone();

        tokio::spawn(async move {
            let mut actor = Actor::new(
                replication_ip,
                super::slave::ActorHandle::new(handle, broadcast),
                receive,
            );
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
