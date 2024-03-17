use crate::{
    cli::MasterAddr,
    prelude::*,
    server::{codec::RespCodec, commands::RedisMessage},
};
use async_channel::{bounded, Receiver};
use futures::{SinkExt, StreamExt};
use rand::{distributions::Alphanumeric, Rng};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use super::{connection_actor::ConnectionActor, storage_actor::StorageActorHandle};

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

#[derive(Debug)]
pub struct Server {
    socket: SocketAddr,
    max_connections: usize,
    pub server_mode: ServerMode,
}

// TODO: Refactor
impl Server {
    #[instrument]
    pub fn new(
        socket: SocketAddr,
        max_connections: usize,
        master_addr: Option<MasterAddr>,
    ) -> Self {
        Self {
            socket,
            max_connections,
            server_mode: ServerMode::new(master_addr),
        }
    }

    #[instrument]
    pub async fn run(&self) -> anyhow::Result<()> {
        self.handle_replication().await.context("replication")?;

        let listener = TcpListener::bind(&self.socket)
            .await
            .context("listening on port")?;

        let (connection_processor_tx, connection_processor_rx) =
            bounded::<ConnectionActor>(self.max_connections);

        Self::run_connections_processor(connection_processor_rx);

        let storage_actor_hnd = StorageActorHandle::new();

        loop {
            match listener.accept().await {
                Ok((tcp_stream, socket)) => {
                    trace!("new connection from {:?}", socket);
                    let storage_actor_hnd = storage_actor_hnd.clone();
                    let connection = ConnectionActor::new(
                        tcp_stream,
                        self.server_mode.clone(),
                        storage_actor_hnd,
                    );

                    if let Err(e) = connection_processor_tx.send(connection).await {
                        bail!("processing channel error {e}");
                    }
                }
                Err(e) => error!("Failed to accept connection {:?}", e),
            }
        }
    }

    // TODO: Extract tcp resp framed, refactor
    #[instrument(skip(self))]
    async fn handle_replication(&self) -> anyhow::Result<()> {
        let ServerMode::Slave(ref master_addr) = self.server_mode else {
            return Ok(());
        };

        trace!("connecting to {:?}", master_addr);

        let master_connection = TcpStream::connect(master_addr)
            .await
            .context("failed to connect to master")?;
        let mut master_connection = Framed::new(master_connection, RespCodec);

        master_connection.send(RedisMessage::Ping(None)).await?;

        let RedisMessage::Pong = master_connection
            .next()
            .await
            .context("expecting pong response")??
        else {
            bail!("expecting pong reply")
        };

        master_connection
            .send(RedisMessage::ReplConfPort {
                port: self.socket.port(),
            })
            .await?;

        let RedisMessage::Ok = master_connection
            .next()
            .await
            .context("expecting repl response")??
        else {
            bail!("expecting ok reply")
        };

        master_connection
            .send(RedisMessage::ReplConfCapa {
                capa: "psync2".to_string(),
            })
            .await?;

        let RedisMessage::Ok = master_connection
            .next()
            .await
            .context("expecting repl response")??
        else {
            bail!("expecting ok reply")
        };

        master_connection
            .send(RedisMessage::Psync {
                replication_id: "?".to_string(),
                offset: -1,
            })
            .await?;

        let RedisMessage::FullResync {
            replication_id: _,
            offset: _,
        } = master_connection
            .next()
            .await
            .context("expecting fullserync response")??
        else {
            bail!("expecting fullresync reply")
        };

        Ok(())
    }

    #[instrument(skip_all)]
    fn run_connections_processor(connection_processor_rx: Receiver<ConnectionActor>) {
        tokio::spawn(async move {
            trace!("connections processor started");
            loop {
                while let Ok(mut connection_actor) = connection_processor_rx.recv().await {
                    trace!("accepted new connection {:?}", &connection_actor);
                    tokio::spawn(async move {
                        trace!("processing connection {:?}", &connection_actor);
                        if let Err(e) = connection_actor.run().await {
                            error!("Failed to process connection, error {:?}", e)
                        }
                    });
                }
            }
        });
    }
}
