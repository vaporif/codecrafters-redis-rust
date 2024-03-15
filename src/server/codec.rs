use bytes::Buf;
use itertools::{Either, Itertools};
use serde_resp::{array, bulk, bulk_null, de, err_str, ser, simple, RESP};
use std::{
    io::{BufReader, Cursor},
    time::Duration,
};
use tokio_util::codec::{Decoder, Encoder};

use super::{commands::*, core_listener::ServerMode};
use crate::prelude::*;

#[derive(Debug)]
pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = RedisMessage;

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
        let mut buff_reader = BufReader::new(cursor);
        let message: serde_resp::RESP =
            de::from_buf_reader(&mut buff_reader).context("decoding resp")?;

        let final_position = buff_reader.into_inner().position();

        // NOTE: I expect all packets at once
        src.advance(final_position as usize);
        let command = RedisMessage::try_from(message)?;
        Ok(Some(command))
    }
}

impl Encoder<RedisMessage> for RespCodec {
    type Error = anyhow::Error;

    #[instrument]
    fn encode(
        &mut self,
        item: RedisMessage,
        dst: &mut bytes::BytesMut,
    ) -> std::prelude::v1::Result<(), Self::Error> {
        let item: RESP = item.into();
        dst.extend_from_slice(
            &ser::to_string(&item)
                .context("serializing resp")?
                .into_bytes(),
        );
        Ok(())
    }
}

impl TryFrom<RESP> for RedisMessage {
    type Error = anyhow::Error;

    fn try_from(message: RESP) -> std::prelude::v1::Result<Self, Self::Error> {
        match message {
            serde_resp::RESPType::SimpleString(string) => {
                RedisMessage::parse_string_message(&string)
            }
            serde_resp::RESPType::Error(error) => bail!(error),
            serde_resp::RESPType::Integer(number) => bail!("unexpected number {number}"),
            serde_resp::RESPType::BulkString(bytes) => {
                let bytes = bytes.context("empty bytes")?;
                let string = resp_bulkstring_to_string(bytes)?;
                RedisMessage::parse_string_message(&string)
            }
            serde_resp::RESPType::Array(data) => {
                let Some(data) = data else {
                    bail!("no data returned");
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
    fn parse_string_message(message: &str) -> anyhow::Result<RedisMessage> {
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
            s => bail!("unknown string command {}", s),
        }
    }

    // TODO: swap_remove instead of clone?
    // break into subfunctions
    #[instrument]
    fn parse_array_message(messages: Vec<RESP>) -> anyhow::Result<RedisMessage> {
        let array_command = RedisArrayCommand::from(messages)?;

        let command = array_command.command.to_lowercase();
        match command.as_str() {
            "ping" => Ok(RedisMessage::Ping(None)),
            "echo" => {
                let arg = array_command.args.first().context("echo command arg")?;

                Ok(RedisMessage::Echo(arg.clone()))
            }
            "set" => {
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

                    match ttl_format.to_lowercase().as_ref() {
                        "ex" => {
                            set_data.arguments.ttl = Some(Duration::from_secs(ttl));
                        }
                        "px" => {
                            set_data.arguments.ttl = Some(Duration::from_millis(ttl));
                        }
                        _ => bail!("unknown args"),
                    }
                }

                Ok(RedisMessage::Set(set_data))
            }
            "get" => {
                let key = array_command.args.first().context("get key arg")?;

                Ok(RedisMessage::Get(key.clone()))
            }
            "replconf" => {
                let subcommand = array_command
                    .args
                    .first()
                    .context("replconf subcommand arg")?;

                match subcommand.to_lowercase().as_str() {
                    "listening-port" => {
                        let port = array_command.args.get(1).context("replconf port")?;
                        let port = port.parse().context("parse port")?;
                        Ok(RedisMessage::ReplConfPort { port })
                    }
                    "capa" => {
                        let capa = array_command
                            .args
                            .get(1)
                            .context("replconf capa")?
                            .to_string();

                        Ok(RedisMessage::ReplConfCapa { capa })
                    }
                    s => bail!("unknown replconf {:?}", s),
                }
            }
            "psync" => {
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
            "info" => {
                let subcommand = array_command.args.first().context("info subcommand")?;

                let subcommand = subcommand.to_lowercase();
                match subcommand.as_str() {
                    "replication" => Ok(RedisMessage::Info(InfoCommand::Replication)),
                    _ => bail!("unsupported command"),
                }
            }
            s => bail!("unknown array command {:?}", s),
        }
    }
}

impl From<RedisMessage> for RESP {
    fn from(val: RedisMessage) -> Self {
        match val {
            RedisMessage::Ping(_) => array![bulk!(b"PING".to_vec())],
            RedisMessage::Pong => simple!("PONG".to_string()),
            RedisMessage::Echo(_) => todo!(),
            RedisMessage::Set(_) => todo!(),
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
            } => simple!(format!("fullresync {replication_id} {offset}")),
            RedisMessage::EchoResponse(echo) => bulk!(echo.into_bytes()),
            RedisMessage::Err(error) => err_str!(error),
            RedisMessage::CacheFound(val) => bulk!(val),
            RedisMessage::CacheNotFound => bulk_null!(),
            RedisMessage::InfoResponse(server_mode) => server_mode.to_resp(),
        }
    }
}

impl ServerMode {
    fn to_resp(&self) -> RESP {
        match self {
            ServerMode::Master {
                master_replid,
                master_repl_offset,
            } => {
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
