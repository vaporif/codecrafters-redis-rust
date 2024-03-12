use async_channel::Sender;
use futures::{sink::SinkExt, StreamExt};
use resp::Value as RespMessage;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tokio::sync::oneshot;

use super::{
    codec::{Command, RespCodec},
    commands::*,
};
use crate::prelude::*;

#[derive(DebugExtras)]
#[allow(unused)]
pub struct ConnectionActor {
    pub socket: SocketAddr,
    #[debug_ignore]
    tcp_stream: Framed<TcpStream, RespCodec>,
}

impl ConnectionActor {
    pub fn new(socket: SocketAddr, tcp_stream: TcpStream) -> Self {
        let tcp_stream = Framed::new(tcp_stream, RespCodec);
        Self { socket, tcp_stream }
    }

    #[instrument(skip(store_access_tx))]
    pub async fn run_actor(&mut self, store_access_tx: Sender<StoreCommand>) -> anyhow::Result<()> {
        loop {
            let command = self
                .next_command()
                .await
                .context("next command from client")?;

            let response_message = match command {
                Command::Ping(_) => RespMessage::Bulk("pong".to_uppercase().to_string()),
                Command::Echo(echo_string) => RespMessage::Bulk(echo_string),
                Command::Set(set_data) => {
                    let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                    store_access_tx
                        .send(StoreCommand::Set(set_data, reply_channel_tx))
                        .await
                        .context("sending set store command")?;

                    match reply_channel_rx.await.context("waiting for reply for set") {
                        Ok(_) => RespMessage::Bulk("ok".to_uppercase().to_string()),
                        Err(e) => RespMessage::Error(format!("error {:?}", e)),
                    }
                }
                Command::Get(key) => {
                    let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                    store_access_tx
                        .send(StoreCommand::Get(key, reply_channel_tx))
                        .await
                        .context("sending set store command")?;

                    match reply_channel_rx.await.context("waiting for reply for get") {
                        Ok(result) => match result {
                            Some(value) => RespMessage::Bulk(value),
                            None => RespMessage::Null,
                        },
                        Err(e) => RespMessage::Error(format!("error {:?}", e)),
                    }
                }
                _ => bail!("unexpected"),
            };

            self.send_response(response_message)
                .await
                .context("sending response")?;
        }
    }

    // TODO: cover connection shutdown
    #[tracing::instrument(skip(self))]
    async fn next_command(&mut self) -> anyhow::Result<Command> {
        self.tcp_stream
            .next()
            .await
            .map(|m| m.context("tcp stream closed"))
            .context("next command message")?
    }

    #[tracing::instrument(skip(self))]
    async fn send_response(&mut self, message: RespMessage) -> anyhow::Result<()> {
        self.tcp_stream
            .send(message)
            .await
            .context("sending message")
    }
}
