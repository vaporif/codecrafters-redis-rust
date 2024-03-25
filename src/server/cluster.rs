use core::panic;
use std::{collections::HashMap, default, net::SocketAddr, usize};

use rand::{distributions::Alphanumeric, Rng};

pub use crate::prelude::*;
use crate::server::slave;

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
    SlaveDisconnected {
        socket: SocketAddr,
    },
    Set(SetData),
    GetUpToDateSlaveCount {
        expected_replicas: u64,
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
    NotifyReplicaOffset {
        socket: SocketAddr,
        offset: usize,
    },
}

pub struct Actor {
    server_mode: ServerMode,
    replicas_offsets: HashMap<SocketAddr, usize>,
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
            replicas_offsets: default::Default::default(),
            slave_handler,
            receiver,
        }
    }

    #[instrument(skip(self))]
    async fn run(&mut self) {
        trace!("cluster started {:?}", self.server_mode);
        while let Some(message) = self.receiver.recv().await {
            trace!("new message {:?}", &message);
            match message {
                Message::AddNewSlave((socket, mut stream)) => {
                    if let Err(error) = self.slave_handler.run(socket, stream).await {
                        error!("failed to connect {:?}", error);
                    }
                    self.replicas_offsets.entry(socket).or_insert(0);
                }
                Message::SlaveDisconnected { socket } => {
                    self.replicas_offsets.remove(&socket);
                }
                Message::GetUpToDateSlaveCount {
                    expected_replicas,
                    channel,
                } => {
                    if let ServerMode::Master(ref master_info) = self.server_mode {
                        let count_up_to_date: usize = self
                            .replicas_offsets
                            .iter()
                            .filter(|f| f.1 >= &master_info.master_repl_offset)
                            .map(|f| *f.1)
                            .sum();

                        trace!(
                            "request to calculate synced replicas, current value: {}, master offset: {}",
                            count_up_to_date,
                            &master_info.master_repl_offset
                        );

                        if count_up_to_date < expected_replicas as usize {
                            self.slave_handler.send(slave::Message::RefreshOffset).await;
                        }

                        channel.send(count_up_to_date as u64).expect("sent");
                    } else {
                        panic!("this is for master only");
                    }
                }
                Message::Set(set_data) => {
                    trace!("sending to slaves");
                    _ = self.slave_handler.send(slave::Message::Set(set_data)).await;
                }
                Message::GetServerMode { channel } => {
                    channel.send(self.server_mode.clone());
                }
                Message::AddMasterOffset { offset } => {
                    if let ServerMode::Master(ref mut master_info) = self.server_mode {
                        master_info.master_repl_offset += offset;
                    }
                }
                Message::NotifyReplicaOffset { socket, offset } => {
                    self.replicas_offsets
                        .entry(socket)
                        .and_modify(|f| *f = offset)
                        .or_insert(offset);
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
            let default_panic = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                error!("cluster crashed {info:?}");
                std::process::exit(1);
            }));
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
        self.sender
            .send(message)
            .context("[cluster] sending message")
    }
}
