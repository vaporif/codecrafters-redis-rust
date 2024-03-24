use std::{time::Duration, usize};

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
#[derive(DebugExtras, Clone)]
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
    ReplConfGetAck,
    ReplConfAck { offset: usize },
    Info(InfoCommand),
    Wait { replica_count: u64, timeout: u64 },
    WaitReply { replica_count: u64 },
    CacheFound(Vec<u8>),
    CacheNotFound,
    InfoResponse(ServerMode),
}

#[derive(Debug, Clone)]
pub enum InfoCommand {
    Replication,
}
