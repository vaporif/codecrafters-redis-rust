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

// #[instrument]
// pub async fn run(&self) -> anyhow::Result<()> {
//     // TODO: extract to actor
//     self.handle_replication().await.context("replication")?;

//     let (connection_processor_tx, connection_processor_rx) = bounded(self.max_connections);

//     spawn_connections_processing(connection_processor_rx);

//     let storage_actor_hnd = StorageActorHandle::new();

//     loop {
//         match self.listener.accept().await {
//             Ok((stream, socket)) => {
//                 trace!("new connection from {:?}", socket);
//                 let storage_actor_hnd = storage_actor_hnd.clone();
//                 let new_connection = NewConnection {
//                     server_mode: self.server_mode.clone(),
//                     stream,
//                     storage_handle: storage_actor_hnd.clone(),
//                 };

//                 if let Err(e) = connection_processor_tx.send(new_connection).await {
//                     bail!("processing channel error {e}");
//                 }
//             }
//             Err(e) => error!("Failed to accept connection {:?}", e),
//         }
//     }
// }

// TODO: Extract tcp resp framed, refactor
// #[instrument(skip(self))]
// async fn handle_replication(&self) -> anyhow::Result<()> {
//     let ServerMode::Slave(ref master_addr) = self.server_mode else {
//         return Ok(());
//     };

//     trace!("connecting to {:?}", master_addr);

//     let master_connection = TcpStream::connect(master_addr)
//         .await
//         .context("failed to connect to master")?;
//     let mut master_connection = Framed::new(master_connection, RespCodec);

//     master_connection.send(RedisMessage::Ping(None)).await?;

//     let RedisMessage::Pong = master_connection
//         .next()
//         .await
//         .context("expecting pong response")??
//     else {
//         bail!("expecting pong reply")
//     };

//     master_connection
//         .send(RedisMessage::ReplConfPort {
//             port: self
//                 .listener
//                 .local_addr()
//                 .map(|f| f.port())
//                 .context("getting port")?,
//         })
//         .await?;

//     let RedisMessage::Ok = master_connection
//         .next()
//         .await
//         .context("expecting repl response")??
//     else {
//         bail!("expecting ok reply")
//     };

//     master_connection
//         .send(RedisMessage::ReplConfCapa {
//             capa: "psync2".to_string(),
//         })
//         .await?;

//     let RedisMessage::Ok = master_connection
//         .next()
//         .await
//         .context("expecting repl response")??
//     else {
//         bail!("expecting ok reply")
//     };

//     master_connection
//         .send(RedisMessage::Psync {
//             replication_id: "?".to_string(),
//             offset: -1,
//         })
//         .await?;

//     let RedisMessage::FullResync {
//         replication_id: _,
//         offset: _,
//     } = master_connection
//         .next()
//         .await
//         .context("expecting fullserync response")??
//     else {
//         bail!("expecting fullresync reply")
//     };

//     Ok(())
// }
// }

// #[derive(Debug)]
// pub struct ServerHandle {
//     listener: tokio::net::TcpListener,
//     max_connections: usize,
//     pub server_mode: ServerMode,
// }

// pub struct HandleConnection {
//     pub server_mode: ServerMode,
//     pub storage_handle: StorageActorHandle,
// }

// pub fn spawn_connections_processing(receiver: async_channel::Receiver<HandleConnection>) {
//     tokio::spawn(async move {
//         trace!("connections loop started");
//         loop {
//             while let Ok(connection) = receiver.recv().await {
//                 tokio::spawn(async move {
//                     trace!("processing connection new connection");
//                     if let Err(e) =
//                         run_connection_actor(connection.server_mode, connection.storage_handle)
//                             .await
//                     {
//                         error!("Failed to process connection, error {:?}", e)
//                     }
//                 });
//             }
//         }
//     });
// }

// TODO: Refactor
// impl ServerHandle {
//     #[instrument]
//     pub fn new(
//         listener: tokio::net::TcpListener,
//         max_connections: usize,
//         master_addr: Option<MasterAddr>,
//     ) -> Self {
//         Self {
//             listener,
//             max_connections,
//             server_mode: ServerMode::new(master_addr),
//         }
//     }

//     #[instrument]
//     pub async fn run(&self) -> anyhow::Result<()> {
//         // TODO: extract to actor
//         self.handle_replication().await.context("replication")?;

//         let (connection_processor_tx, connection_processor_rx) = bounded(self.max_connections);

//         spawn_connections_processing(connection_processor_rx);

//         let storage_actor_hnd = StorageActorHandle::new();

//         loop {
//             match self.listener.accept().await {
//                 Ok((stream, socket)) => {
//                     trace!("new connection from {:?}", socket);
//                     let storage_actor_hnd = storage_actor_hnd.clone();
//                     let new_connection = NewConnection {
//                         server_mode: self.server_mode.clone(),
//                         stream,
//                         storage_handle: storage_actor_hnd.clone(),
//                     };

//                     if let Err(e) = connection_processor_tx.send(new_connection).await {
//                         bail!("processing channel error {e}");
//                     }
//                 }
//                 Err(e) => error!("Failed to accept connection {:?}", e),
//             }
//         }
//     }

//     // TODO: Extract tcp resp framed, refactor
//     #[instrument(skip(self))]
//     async fn handle_replication(&self) -> anyhow::Result<()> {
//         let ServerMode::Slave(ref master_addr) = self.server_mode else {
//             return Ok(());
//         };

//         trace!("connecting to {:?}", master_addr);

//         let master_connection = TcpStream::connect(master_addr)
//             .await
//             .context("failed to connect to master")?;
//         let mut master_connection = Framed::new(master_connection, RespCodec);

//         master_connection.send(RedisMessage::Ping(None)).await?;

//         let RedisMessage::Pong = master_connection
//             .next()
//             .await
//             .context("expecting pong response")??
//         else {
//             bail!("expecting pong reply")
//         };

//         master_connection
//             .send(RedisMessage::ReplConfPort {
//                 port: self
//                     .listener
//                     .local_addr()
//                     .map(|f| f.port())
//                     .context("getting port")?,
//             })
//             .await?;

//         let RedisMessage::Ok = master_connection
//             .next()
//             .await
//             .context("expecting repl response")??
//         else {
//             bail!("expecting ok reply")
//         };

//         master_connection
//             .send(RedisMessage::ReplConfCapa {
//                 capa: "psync2".to_string(),
//             })
//             .await?;

//         let RedisMessage::Ok = master_connection
//             .next()
//             .await
//             .context("expecting repl response")??
//         else {
//             bail!("expecting ok reply")
//         };

//         master_connection
//             .send(RedisMessage::Psync {
//                 replication_id: "?".to_string(),
//                 offset: -1,
//             })
//             .await?;

//         let RedisMessage::FullResync {
//             replication_id: _,
//             offset: _,
//         } = master_connection
//             .next()
//             .await
//             .context("expecting fullserync response")??
//         else {
//             bail!("expecting fullresync reply")
//         };

//         Ok(())
//     }
// }

// #[instrument(skip_all)]
// fn run_connections_processor(connection_processor_rx: Receiver<ConnectionActor>) {
//     tokio::spawn(async move {
//         trace!("connections processor started");
//         loop {
//             while let Ok(mut connection_actor) = connection_processor_rx.recv().await {
//                 trace!("accepted new connection {:?}", &connection_actor);
//                 tokio::spawn(async move {
//                     trace!("processing connection {:?}", &connection_actor);
//                     if let Err(e) = connection_actor.run().await {
//                         error!("Failed to process connection, error {:?}", e)
//                     }
//                 });
//             }
//         }
//     });
// }
