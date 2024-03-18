use crate::{prelude::*, MasterAddr};
use rand::{distributions::Alphanumeric, Rng};
use tokio::{net::TcpStream, task::JoinHandle};

use super::{commands::SetData, connection::NewConnection};

#[derive(Debug, Clone)]
pub enum ServerMode {
    Master(MasterInfo),
    Slave(MasterAddr),
}

#[derive(Debug, Clone)]
pub struct MasterInfo {
    pub master_replid: String,
    pub master_repl_offset: i32,
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

pub async fn spawn_actor_executor(
    replication_ip: Option<MasterAddr>,
    port: u16,
    max_connections: usize,
) -> (ExecutorMessenger, JoinHandle<()>) {
    let (connection_sender, connection_receiver) = tokio::sync::mpsc::channel(max_connections);
    let (internal_sender, internal_receiver) = tokio::sync::mpsc::channel(max_connections);

    let executor_messenger = ExecutorMessenger {
        connection_sender,
        internal_sender,
    };

    let resp_executor_messenger = executor_messenger.clone();

    let join_handle = tokio::spawn(async move {
        let server_mode = ServerMode::new(replication_ip);
        let executor = Executor::new(
            server_mode,
            port,
            executor_messenger,
            connection_receiver,
            internal_receiver,
        )
        .await;

        match executor {
            Ok(executor) => {
                if let Err(err) = executor.run().await {
                    error!("executor crash {:?}", err)
                }
            }
            Err(err) => error!("executor crash {:?}", err),
        }
    });

    (resp_executor_messenger, join_handle)
}

#[derive(Clone)]
pub struct ExecutorMessenger {
    pub connection_sender: tokio::sync::mpsc::Sender<ConnectionMessage>,
    pub internal_sender: tokio::sync::mpsc::Sender<Message>,
}

#[allow(unused)]
pub struct Executor {
    server_mode: ServerMode,
    port: u16,
    storage_hnd: super::storage::ActorHandle,
    cluster_hnd: super::cluster::ActorHandle,
    executor_messenger: ExecutorMessenger,
    connection_receiver: tokio::sync::mpsc::Receiver<ConnectionMessage>,
    internal_receiver: tokio::sync::mpsc::Receiver<Message>,
}

impl Executor {
    async fn new(
        server_mode: ServerMode,
        port: u16,
        executor_messenger: ExecutorMessenger,
        connection_receiver: tokio::sync::mpsc::Receiver<ConnectionMessage>,
        internal_receiver: tokio::sync::mpsc::Receiver<Message>,
    ) -> anyhow::Result<Self> {
        let storage_hnd = super::storage::ActorHandle::new(executor_messenger.clone());
        let cluster_hnd = super::cluster::ActorHandle::new();

        if let ServerMode::Slave(ref master_addr) = server_mode {
            trace!("replicating");
            let (db_done_tx, _) = tokio::sync::oneshot::channel();

            executor_messenger
                .internal_sender
                .send(Message::ReplicateMaster((master_addr.clone(), db_done_tx)))
                .await
                .context("replicate from master")?;

            // TODO: exercise doesn't reply :(
            // db_done_rx
            //     .await
            //     .context("waiting till master replica is completed")?;
        }

        Ok(Self {
            server_mode,
            port,
            storage_hnd,
            cluster_hnd,
            executor_messenger,
            connection_receiver,
            internal_receiver,
        })
    }

    async fn run(mut self) -> anyhow::Result<()> {
        trace!("waiting on channels");
        loop {
            tokio::select! {
             Some(message) = self.connection_receiver.recv() => {
                    match message {
                        ConnectionMessage::NewConnection(stream) => {
                            let storage_hnd = self.storage_hnd.clone();
                            let server_mode = self.server_mode.clone();

                            tokio::spawn(async move {
                                let new_connection = NewConnection {
                                    storage_hnd,
                                    server_mode,
                                };
                                let mut actor = super::connection::Actor::new(stream);
                                if let Err(err) = actor.run(new_connection).await {
                                    error!("connection failure {:?}", err);
                                }
                            });
                        }
                        ConnectionMessage::FatalError(error) => bail!(format!("fatal error {:?}", error)),
                    }
                }
             Some(message) = self.internal_receiver.recv() => {
                    match message {
                        Message::AddNewReplica() => todo!(),
                        Message::ReplicateMaster((ref master_addr, sender)) => {
                            let master_addr = master_addr.clone();
                            tokio::spawn(async move {
                                let master_stream = tokio::net::TcpStream::connect(master_addr)
                                    .await
                                    .expect("failed to connect to master");
                                let actor = super::replication::Actor::new(master_stream).await;
                                let master_stream = actor.replicate_from_scratch(self.port).await.expect("todo");
                                sender.send(master_stream).expect("sent");
                            });

                        },
                        // TODO: Buffer
                        Message::ForwardSetToReplica(set_data) => {
                            if self.cluster_hnd.send(super::cluster::Message::Set(set_data)).await.is_err() {
                                error!("No replicas")
                            };
                        },
                    }
                }
            }
        }
    }
}

pub enum ConnectionMessage {
    NewConnection(tokio::net::TcpStream),
    FatalError(std::io::Error),
}

pub enum Message {
    AddNewReplica(),
    ReplicateMaster((MasterAddr, tokio::sync::oneshot::Sender<TcpStream>)),
    ForwardSetToReplica(SetData),
}
