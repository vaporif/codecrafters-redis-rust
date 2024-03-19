use super::{
    error::TransportError,
    executor::{MasterInfo, ServerMode},
};
use bytes::Buf;
use itertools::{Either, Itertools};
use serde_resp::{array, bulk, bulk_null, de, err_str, ser, simple, RESP};
use std::{
    io::{BufReader, Cursor},
    time::Duration,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

use super::commands::*;
use crate::prelude::*;

#[derive(Debug)]
pub struct RespCodec;

pub type RespStream = Framed<tokio::net::TcpStream, RespCodec>;

impl Decoder for RespCodec {
    type Item = RedisMessage;

    type Error = TransportError;

    fn decode(
        &mut self,
        src: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let cursor = Cursor::new(src.clone().freeze());

        let mut buff_reader = BufReader::new(cursor);
        let message: serde_resp::RESP = de::from_buf_reader(&mut buff_reader)?;

        let final_position = buff_reader.into_inner().position();

        // NOTE: I expect all packets at once
        src.advance(final_position as usize);
        let command = RedisMessage::try_from(message)?;

        trace!("received {:?}", &command);
        Ok(Some(command))
    }
}

impl Encoder<RedisMessage> for RespCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: RedisMessage,
        dst: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<(), Self::Error> {
        trace!("writing message {:?}", item);
        let message_bytes = match item {
            RedisMessage::DbTransfer(db) => {
                let mut data = format!("${}\r\n", db.len()).as_bytes().to_vec();
                trace!("serialized db transfer");
                data.extend(db);
                data
            }
            item => {
                let item: RESP = item.into();
                let serialized = ser::to_string(&item).context("serialize resp")?;

                trace!("serialized as {:?}", &serialized);
                serialized.into_bytes()
            }
        };

        dst.extend_from_slice(&message_bytes);

        Ok(())
    }
}

impl TryFrom<RESP> for RedisMessage {
    type Error = super::error::TransportError;

    fn try_from(message: RESP) -> std::prelude::v1::Result<Self, Self::Error> {
        match message {
            serde_resp::RESPType::SimpleString(string) => {
                RedisMessage::parse_string_message(&string)
            }
            serde_resp::RESPType::Error(error) => {
                Err(TransportError::ResponseError(error.to_string()))
            }
            serde_resp::RESPType::Integer(_) => Err(TransportError::Other(anyhow!(
                "integer not expected".to_string()
            ))),
            serde_resp::RESPType::BulkString(bytes) => {
                let bytes = bytes.context("empty bytes")?;
                let string = resp_bulkstring_to_string(bytes)?;
                RedisMessage::parse_string_message(&string)
            }
            serde_resp::RESPType::Array(data) => {
                let Some(data) = data else {
                    return Err(TransportError::Other(anyhow!("array empty".to_string())));
                };

                RedisMessage::parse_array_message(data)
            }
        }
    }
}

fn resp_bulkstring_to_string(bytes: Vec<u8>) -> anyhow::Result<String> {
    String::from_utf8(bytes).context("building utf8 string from resp bulkstring")
}

struct RedisArrayCommand {
    command: String,
    args: Vec<String>,
}

impl RedisArrayCommand {
    fn from(resp: Vec<RESP>) -> anyhow::Result<Self> {
        let mut resp = resp.into_iter().map(RedisArrayCommand::map_string);

        let command = resp.next().context("getting command")??;
        let (args, errs): (Vec<String>, Vec<anyhow::Error>) =
            resp.partition_map(|result| match result {
                Ok(ok) => Either::Left(ok),
                Err(err) => Either::Right(err),
            });

        // TODO: Helper
        let error: Vec<_> = errs.into_iter().map(|f| f.to_string()).collect();
        if !error.is_empty() {
            bail!(error.join("\n"));
        }

        Ok(RedisArrayCommand { command, args })
    }

    fn map_string(resp: RESP) -> anyhow::Result<String> {
        let RESP::BulkString(Some(bytes)) = resp else {
            bail!("non bulk string for command");
        };

        resp_bulkstring_to_string(bytes)
    }
}

impl RedisMessage {
    #[instrument]
    fn parse_string_message(message: &str) -> anyhow::Result<RedisMessage, TransportError> {
        match message.to_lowercase().as_str() {
            "ping" => Ok(RedisMessage::Ping(None)),
            "pong" => Ok(RedisMessage::Pong),
            "ok" => Ok(RedisMessage::Ok),
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
                Ok(RedisMessage::FullResync {
                    replication_id,
                    offset,
                })
            }
            s => Err(TransportError::UnknownCommand(s.to_string())),
        }
    }

    // TODO: swap_remove instead of clone?
    // break into subfunctions
    #[instrument]
    fn parse_array_message(messages: Vec<RESP>) -> Result<RedisMessage, TransportError> {
        let array_command = RedisArrayCommand::from(messages)?;

        let command = array_command.command.to_uppercase();
        match command.as_str() {
            "PING" => Ok(RedisMessage::Ping(None)),
            "ECHO" => {
                let arg = array_command.args.first().context("echo command arg")?;

                Ok(RedisMessage::Echo(arg.clone()))
            }
            "SET" => {
                let key = array_command.args.first().context("set key arg")?;
                let value = array_command.args.get(1).context("set value arg")?;

                let mut set_data = SetData {
                    key: key.clone(),
                    value: value.clone(),
                    arguments: SetArguments { ttl: None },
                };

                // TODO: there are other args!
                if let Some(ttl_format) = array_command.args.get(2) {
                    let ttl = array_command.args.get(3).context("set ttl")?;
                    let ttl = ttl.parse::<u64>().context("converting ttl to number")?;

                    match ttl_format.to_uppercase().as_ref() {
                        "EX" => {
                            set_data.arguments.ttl = Some(Duration::from_secs(ttl));
                        }
                        "PX" => {
                            set_data.arguments.ttl = Some(Duration::from_millis(ttl));
                        }
                        s => Err(TransportError::UnknownCommand(format!(
                            "unknown args {} for command set",
                            s
                        )))?,
                    }
                }

                Ok(RedisMessage::Set(set_data))
            }
            "GET" => {
                let key = array_command.args.first().context("get key arg")?;

                Ok(RedisMessage::Get(key.clone()))
            }
            "REPLCONF" => {
                let subcommand = array_command
                    .args
                    .first()
                    .context("replconf subcommand arg")?;

                match subcommand.to_uppercase().as_str() {
                    "LISTENING-PORT" => {
                        let port = array_command.args.get(1).context("replconf port")?;
                        let port = port.parse().context("parse port")?;
                        Ok(RedisMessage::ReplConfPort { port })
                    }
                    "CAPA" => {
                        let capa = array_command
                            .args
                            .get(1)
                            .context("replconf capa")?
                            .to_string();

                        Ok(RedisMessage::ReplConfCapa { capa })
                    }
                    s => Err(anyhow::anyhow!("unknown replconf {:?}", s))?,
                }
            }
            "PSYNC" => {
                let replication_id = array_command
                    .args
                    .first()
                    .context("psync repl_id")?
                    .to_string();
                let offset = array_command.args.get(1).context("psync offset")?;
                let offset = offset.parse().context("psync offset parse")?;
                Ok(RedisMessage::Psync {
                    replication_id,
                    offset,
                })
            }
            "INFO" => {
                let subcommand = array_command.args.first().context("info subcommand")?;

                let subcommand = subcommand.to_uppercase();
                match subcommand.as_str() {
                    "REPLICATION" => Ok(RedisMessage::Info(InfoCommand::Replication)),
                    s => Err(TransportError::UnknownCommand(s.to_string())),
                }
            }
            s => Err(TransportError::UnknownCommand(s.to_string())),
        }
    }
}

impl From<RedisMessage> for RESP {
    fn from(val: RedisMessage) -> Self {
        match val {
            RedisMessage::Ping(_) => array![bulk!(b"PING".to_vec())],
            RedisMessage::Pong => simple!("PONG".to_string()),
            RedisMessage::Echo(_) => todo!(),
            RedisMessage::Set(set_data) => {
                array![
                    bulk!(b"SET".to_vec()),
                    bulk!(set_data.key.into_bytes()),
                    bulk!(set_data.value.into_bytes())
                ]
            }
            RedisMessage::Get(_) => todo!(),
            RedisMessage::ReplConfPort { port } => array![
                bulk!(b"REPLCONF".to_vec()),
                bulk!(b"LISTENING-PORT".to_vec()),
                bulk!(port.to_string().into_bytes()),
            ],
            RedisMessage::ReplConfCapa { capa } => array![
                bulk!(b"REPLCONF".to_vec()),
                bulk!(b"CAPA".to_vec()),
                bulk!(capa.to_string().into_bytes()),
            ],
            RedisMessage::Info(_) => todo!(),
            RedisMessage::Ok => bulk!(b"OK".to_vec()),
            RedisMessage::Psync {
                replication_id,
                offset,
            } => array![
                bulk!(b"PSYNC".to_vec()),
                bulk!(replication_id.into_bytes()),
                bulk!(offset.to_string().into_bytes()),
            ],
            RedisMessage::FullResync {
                replication_id,
                offset,
            } => simple!(format!("FULLRESYNC {replication_id} {offset}")),
            RedisMessage::EchoResponse(echo) => bulk!(echo.into_bytes()),
            RedisMessage::Err(error) => err_str!(error),
            RedisMessage::CacheFound(val) => bulk!(val),
            RedisMessage::CacheNotFound => bulk_null!(),
            RedisMessage::InfoResponse(server_mode) => server_mode.to_resp(),
            RedisMessage::DbTransfer(_) => {
                unreachable!("thank god redis for not following their own protocol, won't work")
            }
        }
    }
}

impl ServerMode {
    fn to_resp(&self) -> RESP {
        match self {
            ServerMode::Master(MasterInfo {
                master_replid,
                master_repl_offset,
            }) => {
                let role = "role:master".to_string();
                let master_replid = format!("master_replid:{master_replid}");
                let master_repl_offset = format!("master_repl_offset:{master_repl_offset}");
                let string = [role, master_replid, master_repl_offset].join("\n");
                bulk!(string.into_bytes())
            }
            ServerMode::Slave(_) => bulk!(b"role:slave".to_vec()),
        }
    }
}
