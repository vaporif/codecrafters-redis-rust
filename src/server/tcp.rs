use tokio::net::TcpListener;

use crate::prelude::*;

pub struct TcpServer {
    listener: TcpListener,
    executor_messenger: super::ExecutorMessenger,
}

impl TcpServer {
    pub async fn new(listener: TcpListener, executor_messenger: super::ExecutorMessenger) -> Self {
        Self {
            listener,
            executor_messenger,
        }
    }

    pub async fn start(&mut self) {
        if let Err(error) = self.accept_connections_loop().await {
            let _ = self
                .executor_messenger
                .connection_sender
                .send(super::executor::ConnectionMessage::FatalError(error))
                .await;
        }
    }

    async fn accept_connections_loop(&mut self) -> Result<(), std::io::Error> {
        trace!("listener started at {:?}", &self.listener);
        loop {
            let (stream, socket) = self.listener.accept().await?;
            tracing::trace!("new connection incoming {:?}", socket);
            let _ = self
                .executor_messenger
                .connection_sender
                .send(super::executor::ConnectionMessage::NewConnection(stream))
                .await;
        }
    }
}
