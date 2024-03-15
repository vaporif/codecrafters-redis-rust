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
    type Item = Message;

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
        let command = Message::try_from(message)?;
        Ok(Some(command))
    }
}

impl Encoder<RespMessage> for RespCodec {
    type Error = anyhow::Error;

    #[instrument]
    fn encode(
        &mut self,
        item: RespMessage,
        dst: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<(), Self::Error> {
        dst.extend_from_slice(&item.encode());
        Ok(())
    }
}

// TODO: very messy parsing, needs refactoring
impl TryFrom<RespMessage> for Message {
    type Error = anyhow::Error;

    fn try_from(message: RespMessage) -> std::prelude::v1::Result<Self, Self::Error> {
        match message {
            RespMessage::Null => todo!(),
            RespMessage::NullArray => todo!(),
            RespMessage::String(message) => match message.to_lowercase().as_str() {
                "ping" => Ok(Message::Ping(None)),
                "pong" => Ok(Message::Pong),
                "ok" => Ok(Message::Ok),
                s if s.starts_with("fullresync") => {
                    let mut split_message = s.split_whitespace().skip(1);
                    let replication_id = split_message
                        .next()
                        .context("replication_id missed")?
                        .to_string();
                    let offset = split_message
                        .next()
                        .context("offset missed")?
                        .parse::<i32>()
                        .context("wrong data")?;
                    Ok(Message::FullResync {
                        replication_id,
                        offset,
                    })
                }
                s => bail!("unknown message {}", s),
            },
            RespMessage::Error(_) => todo!(),
            RespMessage::Integer(_) => todo!(),
            RespMessage::Bulk(message) => match message.to_lowercase().as_str() {
                "ping" => Ok(Message::Ping(None)),
                "pong" => Ok(Message::Pong),
                "ok" => Ok(Message::Ok),
                s => bail!("unknown message {}", s),
            },
            RespMessage::BufBulk(_) => todo!(),
            RespMessage::Array(messages) => {
                trace!("messages parsed {:?}", messages);
                if messages.is_empty() {
                    bail!("empty messages");
                }

                let RespMessage::Bulk(string) = messages.first().context("first resp string")?
                else {
                    bail!("non bulk string for command");
                };

                let string = string.to_lowercase();
                match string.as_ref() {
                    "ping" => Ok(Message::Ping(None)),
                    "pong" => Ok(Message::Pong),
                    "echo" => {
                        let RespMessage::Bulk(argument) =
                            messages.get(1).context("first argument")?
                        else {
                            bail!("non bulk string for argument");
                        };

                        Ok(Message::Echo(argument.to_string()))
                    }
                    "set" => {
                        let RespMessage::Bulk(key) = messages.get(1).context("first argument")?
                        else {
                            bail!("non bulk string for key");
                        };

                        let RespMessage::Bulk(value) =
                            messages.get(2).context("second argument")?
                        else {
                            bail!("non bulk string for value");
                        };

                        let mut set_data = SetData {
                            key: key.to_string(),
                            value: value.to_string(),
                            arguments: SetArguments { ttl: None },
                        };

                        // TODO: there are other args!
                        if let Ok(RespMessage::Bulk(ttl_format)) =
                            messages.get(3).context("ttl get")
                        {
                            let RespMessage::Bulk(ttl) =
                                messages.get(4).context("ttl value get")?
                            else {
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

                        Ok(Message::Set(set_data))
                    }
                    "get" => {
                        let RespMessage::Bulk(key) = messages.get(1).context("get key")? else {
                            bail!("non bulk string for key");
                        };

                        Ok(Message::Get(key.to_string()))
                    }
                    "info" => {
                        let RespMessage::Bulk(command_type) =
                            messages.get(1).context("get type")?
                        else {
                            bail!("non bulk string for type");
                        };

                        let command_type = command_type.to_lowercase();
                        match command_type.as_str() {
                            "replication" => Ok(Message::Info(InfoCommand::Replication)),
                            _ => bail!("unsupported command"),
                        }
                    }
                    s => bail!("unknown command {:?}", s),
                }
            }
        }
    }
}
