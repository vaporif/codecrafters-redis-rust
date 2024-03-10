use std::net::SocketAddr;

use crate::prelude::*;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Debug)]
#[allow(unused)]
pub struct Connection {
    socket: SocketAddr,
    tcp_stream: TcpStream,
}

impl Connection {
    pub fn new(socket: SocketAddr, tcp_stream: TcpStream) -> Self {
        Self { socket, tcp_stream }
    }
    #[tracing::instrument]
    pub async fn process(&mut self) -> anyhow::Result<()> {
        self.tcp_stream
            .write_all("+PONG\r\n".as_bytes())
            .await
            .context("Failed to process ")
    }
}

#[allow(unused)]
enum Message {
    Ping,
    Pong,
}

#[allow(unused)]
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
