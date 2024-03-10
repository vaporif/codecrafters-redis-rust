use async_channel::{bounded, Receiver};
use std::net::SocketAddr;
use tokio::net::TcpListener;

use crate::prelude::*;
use connection::Connection;
mod connection;

pub async fn connections_listen(socket: SocketAddr, max_connections: usize) -> anyhow::Result<()> {
    let listener = TcpListener::bind(socket)
        .await
        .context("failed to listen")?;

    let (connection_snd, connections_processor) = bounded::<Connection>(max_connections);
    processing_loop(connections_processor);

    loop {
        match listener.accept().await {
            Ok((tcp_stream, socket)) => {
                let connection = Connection::new(socket, tcp_stream);

                if let Err(e) = connection_snd.send(connection).await {
                    bail!("processing channel error {e}");
                }
            }
            Err(e) => eprintln!("Failed to accept connection {e}"),
        }
    }
}

fn processing_loop(receiver_tx: Receiver<Connection>) {
    tokio::spawn(async move {
        loop {
            while let Ok(mut connection) = receiver_tx.recv().await {
                trace!("accepted new connection");
                tokio::spawn(async move {
                    if let Err(e) = connection.process().await {
                        eprintln!("Failed to process stream, error {e}")
                    }
                });
            }
        }
    });
}
