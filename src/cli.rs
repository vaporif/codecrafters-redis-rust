use std::usize;

use clap::{arg, command, Parser};

#[derive(Parser, Debug)]
#[command(author = "Dmytro Onypko", name = "Redis Sample Server")]
pub struct Cli {
    #[arg(short, long, default_value_t = 6379)]
    pub port: u16,
    #[arg(short, long, default_value_t = 10_000)]
    pub max_connections: usize,
    #[arg(short, long)]
    pub tokio_console: bool,
}
