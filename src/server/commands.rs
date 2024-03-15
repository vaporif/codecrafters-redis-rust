use crate::prelude::*;
use resp::Value as RespMessage;
use std::time::Duration;
use tokio::sync::oneshot;

pub type GetReplyChannel = oneshot::Sender<Option<String>>;
pub type SetReplyChannel = oneshot::Sender<Result<()>>;

#[derive(Debug)]
pub enum StoreCommand {
    Get(String, GetReplyChannel),
    Set(SetData, SetReplyChannel),
}

#[derive(Debug)]
pub struct SetData {
    pub key: String,
    pub value: String,
    pub arguments: SetArguments,
}

#[derive(Debug)]
pub struct SetArguments {
    pub ttl: Option<Duration>,
}

#[allow(unused)]
#[derive(Debug)]
pub enum Message {
    Ping(Option<String>),
    Pong,
    Ok,
    Echo(String),
    Set(SetData),
    Get(String),
    ReplConfPort { port: u16 },
    ReplConfCapa { capa: String },
    Info(InfoCommand),
}

#[derive(Debug)]
pub enum InfoCommand {
    Replication,
}

impl From<Message> for RespMessage {
    fn from(val: Message) -> Self {
        match val {
            Message::Ping(_) => RespMessage::Array(vec![RespMessage::Bulk("ping".to_string())]),
            Message::Pong => RespMessage::Bulk("pong".to_string()),
            Message::Echo(_) => todo!(),
            Message::Set(_) => todo!(),
            Message::Get(_) => todo!(),
            Message::ReplConfPort { port } => RespMessage::Array(vec![
                RespMessage::Bulk("replconf".to_string()),
                RespMessage::Bulk("listening-port".to_string()),
                RespMessage::Bulk(port.to_string()),
            ]),
            Message::ReplConfCapa { capa } => RespMessage::Array(vec![
                RespMessage::Bulk("replconf".to_string()),
                RespMessage::Bulk("capa".to_string()),
                RespMessage::Bulk(capa.to_string()),
            ]),
            Message::Info(_) => todo!(),
            Message::Ok => todo!(),
        }
    }
}
