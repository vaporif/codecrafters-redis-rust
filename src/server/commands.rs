use crate::prelude::*;
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
pub enum RedisMessage {
    Ping(Option<String>),
    Pong,
    Ok,
    Echo(String),
    // Error(String),
    Set(SetData),
    Get(String),
    Psync { replication_id: String, offset: i32 },
    FullResync { replication_id: String, offset: i32 },
    ReplConfPort { port: u16 },
    ReplConfCapa { capa: String },
    Info(InfoCommand),
}

#[derive(Debug)]
pub enum InfoCommand {
    Replication,
}
