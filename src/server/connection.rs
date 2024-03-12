use async_channel::Sender;
use bytes::Buf;
use resp::{Decoder as RespDecoder, Value as RespMessage};
use std::{
    io::{BufReader, Cursor},
    net::SocketAddr,
    time::Duration,
};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use tokio::sync::oneshot;

use crate::{prelude::*, server::SetData};
use futures::{sink::SinkExt, StreamExt};

use super::{SetArguments, StoreCommand};

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
    pub async fn run_connection_actor(
        &mut self,
        store_access_tx: Sender<StoreCommand>,
    ) -> anyhow::Result<()> {
        loop {
            let command = self.next_command().await.context("get command error")?;

            // TODO: refactor get&set
            let command_to_send = match command {
                Command::Ping(_) => RespMessage::Bulk("pong".to_uppercase().to_string()),
                Command::Echo(echo_string) => RespMessage::Bulk(echo_string),
                Command::Set(set_data) => {
                    let (reply_channel_tx, reply_channel_rx) = oneshot::channel();
                    store_access_tx
                        .send(StoreCommand::Set(set_data, reply_channel_tx))
                        .await
                        .context("sending set store command")?;

                    match reply_channel_rx.await.context("waiting for reply") {
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

                    match reply_channel_rx.await.context("waiting for reply") {
                        Ok(result) => match result {
                            Some(value) => RespMessage::Bulk(value),
                            None => RespMessage::Null,
                        },
                        Err(e) => RespMessage::Error(format!("error {:?}", e)),
                    }
                }
                _ => bail!("unexpected"),
            };

            self.send_command(command_to_send)
                .await
                .context("sending command")?;
        }
    }

    #[tracing::instrument(skip(self))]
    async fn next_command(&mut self) -> anyhow::Result<Command> {
        self.tcp_stream
            .next()
            .await
            .map(|m| m.context("stream closed"))
            .context("message expected")?
    }

    #[tracing::instrument(skip(self))]
    async fn send_command(&mut self, message: RespMessage) -> anyhow::Result<()> {
        self.tcp_stream
            .send(message)
            .await
            .context("sending message")
    }
}

#[derive(Debug)]
struct RespCodec;

impl Decoder for RespCodec {
    type Item = Command;

    type Error = anyhow::Error;

    #[instrument]
    fn decode(
        &mut self,
        src: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            trace!("empty message from network");
            return Ok(None);
        }

        let cursor = Cursor::new(src.clone().freeze());

        trace!("bytes buffer {:?}", cursor);
        let buff_reader = BufReader::new(cursor);
        let mut decoder = RespDecoder::new(buff_reader);
        let message = decoder.decode().context("decode resp error")?;

        // NOTE: I expect all packets at once
        src.advance(message.encode().len());
        let command = Command::to_command(message)?;
        Ok(Some(command))
    }
}

impl Encoder<RespMessage> for RespCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: RespMessage,
        dst: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<(), Self::Error> {
        dst.extend_from_slice(&item.encode());
        Ok(())
    }
}

#[allow(unused)]
#[derive(Debug)]
enum Command {
    Ping(Option<String>),
    Pong,
    Echo(String),
    Set(SetData),
    Get(String),
}

impl Command {
    fn to_command(message: RespMessage) -> anyhow::Result<Self> {
        let RespMessage::Array(messages) = message else {
            bail!("expected array for command");
        };

        trace!("messages parsed {:?}", messages);
        if messages.is_empty() {
            bail!("empty messages");
        }

        let RespMessage::Bulk(string) = messages.first().context("command expected")? else {
            bail!("non bulk string for command");
        };

        let string = string.to_lowercase();
        match string.as_ref() {
            "ping" => Ok(Command::Ping(None)),
            "echo" => {
                let RespMessage::Bulk(argument) = messages.get(1).context("argument expected")?
                else {
                    bail!("non bulk string for argument");
                };

                Ok(Command::Echo(argument.to_string()))
            }
            "set" => {
                let RespMessage::Bulk(key) = messages.get(1).context("key expected")? else {
                    bail!("non bulk string for key");
                };

                let RespMessage::Bulk(value) = messages.get(2).context("value expected")? else {
                    bail!("non bulk string for value");
                };

                let mut set_data = SetData {
                    key: key.to_string(),
                    value: value.to_string(),
                    arguments: SetArguments { ttl: None },
                };

                // TODO: there are other args!
                if let Ok(RespMessage::Bulk(ttl_format)) = messages.get(3).context("ttl expected") {
                    let RespMessage::Bulk(ttl) = messages.get(4).context("px expected")? else {
                        bail!("non integer string for px");
                    };

                    let ttl = ttl.parse::<u64>().context("wrong conversion")?;

                    let ttl_format = ttl_format.to_lowercase();
                    match ttl_format.as_ref() {
                        "ex" => {
                            set_data.arguments.ttl = Some(Duration::from_secs(ttl));
                        }
                        "px" => {
                            set_data.arguments.ttl = Some(Duration::from_millis(ttl));
                        }
                        _ => bail!("unknown args"),
                    }
                };

                Ok(Command::Set(set_data))
            }
            "get" => {
                let RespMessage::Bulk(key) = messages.get(1).context("key expected")? else {
                    bail!("non bulk string for key");
                };

                Ok(Command::Get(key.to_string()))
            }
            s => bail!("unknown command {:?}", s),
        }
    }
}
