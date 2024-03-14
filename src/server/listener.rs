use crate::prelude::*;
use async_channel::{bounded, unbounded, Receiver, Sender};
use rand::{distributions::Alphanumeric, Rng};
use std::net::SocketAddr;
use tokio::net::TcpListener;

use super::{
    commands::StoreCommand, connection_actor::ConnectionActor, storage_actor::StorageActor,
};
#[derive(Debug, Clone)]
pub enum ServerMode {
    Master {
        master_replid: String,
        master_repl_offset: u64,
    },
    Slave(SocketAddr),
}

impl ServerMode {
    fn new(replication_ip: Option<SocketAddr>) -> Self {
        match replication_ip {
            Some(socket_addr) => ServerMode::Slave(socket_addr),
            None => {
                let random_string: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(40)
                    .map(char::from)
                    .collect();
                ServerMode::Master {
                    master_replid: random_string,
                    master_repl_offset: 0,
                }
            }
        }
    }
}

pub struct Server {
    socket: SocketAddr,
    max_connections: usize,
    pub server_mode: ServerMode,
}

// TODO: Refactor
impl Server {
    pub fn new(
        socket: SocketAddr,
        max_connections: usize,
        replication_ip: Option<SocketAddr>,
    ) -> Self {
        Self {
            socket,
            max_connections,
            server_mode: ServerMode::new(replication_ip),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.socket)
            .await
            .context("listening on port")?;

        let (connection_processor_tx, connection_processor_rx) =
            bounded::<ConnectionActor>(self.max_connections);
        let (store_access_tx, store_access_rx) = unbounded::<StoreCommand>();

        Self::run_connections_processor(connection_processor_rx, store_access_tx);

        let storage_actor = StorageActor::new(store_access_rx);
        storage_actor.run_actor();

        loop {
            match listener.accept().await {
                Ok((tcp_stream, socket)) => {
                    let connection =
                        ConnectionActor::new(socket, tcp_stream, self.server_mode.clone());

                    if let Err(e) = connection_processor_tx.send(connection).await {
                        bail!("processing channel error {e}");
                    }
                }
                Err(e) => error!("Failed to accept connection {:?}", e),
            }
        }
    }

    #[instrument(skip_all)]
    fn run_connections_processor(
        connection_processor_rx: Receiver<ConnectionActor>,
        store_access_tx: Sender<StoreCommand>,
    ) {
        tokio::spawn(async move {
            trace!("connections processor started");
            loop {
                while let Ok(mut connection) = connection_processor_rx.recv().await {
                    trace!("accepted new connection {:?}", &connection);
                    let store_access_tx = store_access_tx.clone();
                    tokio::spawn(async move {
                        trace!("processing connection {:?}", &connection);
                        if let Err(e) = connection.run_actor(store_access_tx).await {
                            error!("Failed to process connection, error {:?}", e)
                        }
                    });
                }
            }
        });
    }
}
