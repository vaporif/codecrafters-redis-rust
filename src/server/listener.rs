use async_channel::{bounded, unbounded, Receiver, Sender};
use std::net::SocketAddr;
use tokio::net::TcpListener;

use crate::prelude::*;

use super::{
    commands::StoreCommand, connection_actor::ConnectionActor, storage_actor::StorageActor,
};

pub async fn run_listener(socket: SocketAddr, max_connections: usize) -> anyhow::Result<()> {
    let listener = TcpListener::bind(socket)
        .await
        .context("failed to listen")?;

    let (connection_processor_tx, connection_processor_rx) =
        bounded::<ConnectionActor>(max_connections);
    let (store_access_tx, store_access_rx) = unbounded::<StoreCommand>();

    run_connections_processor(connection_processor_rx, store_access_tx);

    let storage_actor = StorageActor::new(store_access_rx);
    storage_actor.run_actor();

    loop {
        match listener.accept().await {
            Ok((tcp_stream, socket)) => {
                let connection = ConnectionActor::new(socket, tcp_stream);

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
