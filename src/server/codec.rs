use bytes::Buf;
use resp::{Decoder as RespDecoder, Value as RespMessage};
use std::{
    io::{BufReader, Cursor},
    time::Duration,
};
use tokio_util::codec::{Decoder, Encoder};

use super::commands::*;
use crate::prelude::*;

#[derive(Debug)]
pub struct RespCodec;

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
        let message = decoder.decode().context("decode resp message")?;

        // NOTE: I expect all packets at once
        src.advance(message.encode().len());
        let command = Command::try_from(message)?;
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
pub enum Command {
    Ping(Option<String>),
    Pong,
    Echo(String),
    Set(SetData),
    Get(String),
}

impl TryFrom<RespMessage> for Command {
    type Error = anyhow::Error;

    fn try_from(message: RespMessage) -> std::prelude::v1::Result<Self, Self::Error> {
        let RespMessage::Array(messages) = message else {
            bail!("expected array for command");
        };

        trace!("messages parsed {:?}", messages);
        if messages.is_empty() {
            bail!("empty messages");
        }

        let RespMessage::Bulk(string) = messages.first().context("first resp string")? else {
            bail!("non bulk string for command");
        };

        let string = string.to_lowercase();
        match string.as_ref() {
            "ping" => Ok(Command::Ping(None)),
            "echo" => {
                let RespMessage::Bulk(argument) = messages.get(1).context("first argument")? else {
                    bail!("non bulk string for argument");
                };

                Ok(Command::Echo(argument.to_string()))
            }
            "set" => {
                let RespMessage::Bulk(key) = messages.get(1).context("first argument")? else {
                    bail!("non bulk string for key");
                };

                let RespMessage::Bulk(value) = messages.get(2).context("second argument")? else {
                    bail!("non bulk string for value");
                };

                let mut set_data = SetData {
                    key: key.to_string(),
                    value: value.to_string(),
                    arguments: SetArguments { ttl: None },
                };

                // TODO: there are other args!
                if let Ok(RespMessage::Bulk(ttl_format)) = messages.get(3).context("ttl get") {
                    let RespMessage::Bulk(ttl) = messages.get(4).context("ttl value get")? else {
                        bail!("non integer string for px");
                    };

                    let ttl = ttl.parse::<u64>().context("converting ttl to number")?;

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
                let RespMessage::Bulk(key) = messages.get(1).context("get key")? else {
                    bail!("non bulk string for key");
                };

                Ok(Command::Get(key.to_string()))
            }
            s => bail!("unknown command {:?}", s),
        }
    }
}
