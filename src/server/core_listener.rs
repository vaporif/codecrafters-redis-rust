use crate::{prelude::*, server::codec::RespCodec};
use async_channel::{bounded, unbounded, Receiver, Sender};
use futures::SinkExt;
use rand::{distributions::Alphanumeric, Rng};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use resp::Value as RespMessage;

use super::{
    commands::StoreCommand, connection_actor::ConnectionActor, storage_actor::StorageActor,
};
#[derive(Debug, Clone)]
pub enum ServerMode {
    Master {
        master_replid: String,
        master_repl_offset: u64,
    },
    Slave(MasterAddr),
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
                ServerMode::Master {
                    master_replid: random_string,
                    master_repl_offset: 0,
                }
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

    // TODO: Extract tcp resp framed
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
        master_connection
            .send(RespMessage::Array(vec![RespMessage::Bulk(
                "ping".to_string(),
            )]))
            .await?;

        Ok(())
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