use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::prelude::*;
use futures::{sink::SinkExt, StreamExt};

const EOL: &[u8] = b"\r\n";

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
            match message {
                RespMessage::String(string_message) => {
                    let command: Command =
                        string_message.try_into().context("converting to command")?;

                    match command {
                        Command::Ping => self
                            .send_message(Command::Pong)
                            .await
                            .context("sending PONG")?,
                        _ => bail!("unexpected"),
                    }
                }
                _ => todo!("not implemented"),
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

        if let Some(pos) = src.windows(EOL.len()).position(|ele| ele == EOL) {
            trace!("full message found at {pos}");
            let buf = src.split_to(pos + EOL.len());

            if buf.len() <= 1 + EOL.len() {
                trace!("empty message");
                return Ok(None);
            }

            let message = RespMessage::from_bytes(&buf).context("parsing error")?;
            return Ok(Some(message));
        }

        trace!("\r\n not found");
        Ok(None)
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
        dst.extend_from_slice(&resp_message.to_bytes());
        Ok(())
    }
}

#[allow(unused)]
enum RespMessage {
    Null,
    String(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespMessage>>),
}

impl RespMessage {
    #[instrument()]
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<RespMessage> {
        if bytes.is_empty() {
            return Ok(Self::Null);
        }

        if bytes.len() < 1 + EOL.len() {
            bail!("not enough bytes");
        }

        let first_byte = bytes[0];

        if !bytes.ends_with(EOL) {
            bail!("bytes do not end with EOL");
        }

        let message = match first_byte {
            b'+' => {
                let string = String::from_utf8(bytes[1..bytes.len() - EOL.len()].to_vec())
                    .context("failed to convert to string")?;

                trace!("parsed as string {string}");

                RespMessage::String(string)
            }
            _ => bail!("unsupported type"),
        };

        Ok(message)
    }

    fn to_bytes(&self) -> Vec<u8> {
        match self {
            RespMessage::Null => todo!(),
            RespMessage::String(string) => {
                let mut vec = Vec::with_capacity(1 + string.len() + EOL.len());
                vec.push(b'+');
                vec.extend_from_slice(string.as_bytes());
                vec.extend_from_slice(EOL);
                vec
            }
            RespMessage::Error(_) => todo!(),
            RespMessage::Integer(_) => todo!(),
            RespMessage::BulkString(_) => todo!(),
            RespMessage::Array(_) => todo!(),
        }
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

    #[instrument]
    fn try_from(value: String) -> std::prelude::v1::Result<Self, Self::Error> {
        match value {
            _ if value.starts_with("PING") => Ok(Self::Ping),
            _ if value.starts_with("PONG") => Ok(Self::Pong),
            s => bail!("wrong value {s}"),
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
enum Command {
    Ping,
    Pong,
}
