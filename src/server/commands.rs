use std::time::Duration;

use derive_debug_extras::DebugExtras;

use super::executor::ServerMode;

#[derive(Debug, Clone)]
pub struct SetData {
    pub key: String,
    pub value: String,
    pub arguments: SetArguments,
}

#[derive(Debug, Clone)]
pub struct SetArguments {
    pub ttl: Option<Duration>,
}

#[allow(unused)]
#[derive(DebugExtras)]
pub enum RedisMessage {
    Ping(Option<String>),
    Pong,
    Ok,
    Echo(String),
    EchoResponse(String),
    Err(String),
    Set(SetData),
    Get(String),
    Psync { replication_id: String, offset: i32 },
    FullResync { replication_id: String, offset: i32 },
    DbTransfer(#[debug_ignore] Vec<u8>),
    ReplConfPort { port: u16 },
    ReplConfCapa { capa: String },
    Info(InfoCommand),
    CacheFound(Vec<u8>),
    CacheNotFound,
    InfoResponse(ServerMode),
}

#[derive(Debug)]
pub enum InfoCommand {
    Replication,
}
