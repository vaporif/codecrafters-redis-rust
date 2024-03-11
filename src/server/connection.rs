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
            let message = self.next_message().await.context("get message error")?;
            let commands = convert_to_command(message).context("converting to command")?;

            for command in commands {
                match command {
                    Command::Ping => self
                        .send_message(Command::Pong)
                        .await
                        .context("sending PONG")?,
                    _ => bail!("unexpected"),
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn next_message(&mut self) -> anyhow::Result<RespMessage> {
        self.tcp_stream
            .next()
            .await
            .map(|m| m.context("stream closed"))
            .context("message expected")?
    }

    #[tracing::instrument(skip(self))]
    async fn send_message(&mut self, message: Command) -> anyhow::Result<()> {
        self.tcp_stream
            .send(message)
            .await
            .context("sending message")
    }
}

#[instrument]
fn convert_to_command(value: RespMessage) -> anyhow::Result<Vec<Command>> {
    let commands = match value {
        RespMessage::String(string) => parse_command(string)?,
        RespMessage::Bulk(string) => parse_command(string)?,
        RespMessage::Array(items) => {
            items
                .into_iter()
                .map(convert_to_command)
                .try_fold(Vec::new(), |mut acc, f| {
                    let value = f.context("error during parse")?;
                    acc.extend(value);

                    Ok::<Vec<_>, anyhow::Error>(acc)
                })?
        }
        _ => bail!("unsupported command"),
    };

    fn parse_command(string: String) -> anyhow::Result<Vec<Command>> {
        let command: Command = string.try_into()?;
        Ok(vec![command])
    }

    Ok(commands)
}

#[derive(Debug)]
struct RespCodec;

impl Decoder for RespCodec {
    type Item = RespMessage;

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

        trace!("message parsed {:?}", message);
        Ok(Some(message))
    }
}

impl Encoder<Command> for RespCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Command,
        dst: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<(), Self::Error> {
        let resp_message: RespMessage = item.into();
        dst.extend_from_slice(&resp_message.encode());
        Ok(())
    }
}

impl From<Command> for RespMessage {
    fn from(value: Command) -> Self {
        match value {
            Command::Ping => RespMessage::String("ping".to_uppercase().to_string()),
            Command::Pong => RespMessage::String("pong".to_uppercase().to_string()),
        }
    }
}

impl TryFrom<String> for Command {
    type Error = anyhow::Error;

    fn try_from(value: String) -> std::prelude::v1::Result<Self, Self::Error> {
        let value = value.to_lowercase();
        let command = match value.as_str() {
            "ping" => Command::Ping,
            "pong" => Command::Pong,
            s => bail!("unknown command {s}"),
        };

        Ok(command)
    }
}

#[allow(unused)]
#[derive(Debug)]
enum Command {
    Ping,
    Pong,
}
