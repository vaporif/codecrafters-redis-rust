use std::net::SocketAddr;

use crate::prelude::*;

use async_channel::{bounded, Receiver};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

pub async fn server_listen(socket: SocketAddr, max_connections: usize) -> anyhow::Result<()> {
    let listener = TcpListener::bind(socket)
        .await
        .context("failed to listen")?;

    let (connection_snd, connections_processor) = bounded::<Connection>(max_connections);
    processing_loop(connections_processor);

    loop {
        match listener.accept().await {
            Ok((tcp_stream, socket)) => {
                let connection = Connection { socket, tcp_stream };

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

pub struct Connection {
    socket: SocketAddr,
    tcp_stream: TcpStream,
}

impl Connection {
    #[tracing::instrument]
    async fn process(&mut self) -> anyhow::Result<()> {
        self.tcp_stream
            .write_all("+PONG\r\n".as_bytes())
            .await
            .context("Failed to process ")
    }
}

enum Message {
    Ping,
    Pong,
}

impl Message {
    fn to_resp(&self) -> anyhow::Result<Message> {
        match self {
            Message::Ping => Ok(Message::Pong),
            _ => bail!("could not be responded"),
        }
    }

    fn to_binary_resp(&self) -> &'static [u8] {
        match self {
            Message::Ping => "Ping".as_bytes(),
            Message::Pong => "Pong".as_bytes(),
        }
    }
}
