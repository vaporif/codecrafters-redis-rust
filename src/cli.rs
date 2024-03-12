use std::{
    net::{Ipv4Addr, SocketAddrV4},
    usize,
};

use clap::{arg, command, Parser};

#[derive(Parser, Debug)]
#[command(author = "Dmytro Onypko", name = "Redis Sample Server")]
pub struct Cli {
    #[arg(short, long, default_value_t = DEFAULT_SOCKET)]
    pub socket: SocketAddrV4,
    #[arg(short, long, default_value_t = 10_000)]
    pub max_connections: usize,
    #[arg(short, long)]
    pub tokio_console: bool,
}

const DEFAULT_SOCKET: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6379);
