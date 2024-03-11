use bytes::Buf;
use resp::{Decoder as RespDecoder, Value as RespMessage};
use std::{
    io::{BufReader, Cursor},
    net::SocketAddr,
};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::prelude::*;
use futures::{sink::SinkExt, StreamExt};

#[derive(Debug)]
#[allow(unused)]
pub struct Connection {
    socket: SocketAddr,
    tcp_stream: Framed<TcpStream, RespCodec>,
}

impl Connection {
    pub fn new(socket: SocketAddr, tcp_stream: TcpStream) -> Self {
        let tcp_stream = Framed::new(tcp_stream, RespCodec);
        Self { socket, tcp_stream }
    }

    #[instrument(skip(self), fields(self.socket = %self.socket))]
    pub async fn process(&mut self) -> anyhow::Result<()> {
        loop {
            let command = self.next_command().await.context("get command error")?;

            let command_to_send = match command {
                Command::Ping(_) => Command::Pong,
                Command::Echo(echo_string) => Command::Echo(echo_string),
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
    async fn send_command(&mut self, message: Command) -> anyhow::Result<()> {
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
            "ping" => Ok(Some(Command::Ping(None))),
            "echo" => {
                let RespMessage::Bulk(argument) = messages.get(1).context("argument expected")?
                else {
                    bail!("non builk string for argument");
                };

                Ok(Some(Command::Echo(argument.to_string())))
            }
            s => bail!("unknown command {:?}", s),
        }
    }
}

impl Encoder<Command> for RespCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Command,
        dst: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<(), Self::Error> {
        let resp_message = reply_message(item).context("incorrect command")?;
        dst.extend_from_slice(&resp_message.encode());
        Ok(())
    }
}

fn reply_message(value: Command) -> anyhow::Result<RespMessage> {
    match value {
        // TODO: add arg
        Command::Pong => Ok(RespMessage::Bulk("pong".to_uppercase().to_string())),
        Command::Echo(echo_string) => Ok(RespMessage::Bulk(echo_string)),

        s => bail!("reply not supported for {:?}", s),
    }
}

#[allow(unused)]
#[derive(Debug)]
enum Command {
    Ping(Option<String>),
    Pong,
    Echo(String),
}
