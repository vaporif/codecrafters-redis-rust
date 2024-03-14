use std::net::{Ipv4Addr, SocketAddrV4};

use clap::Parser;
use cli::Cli;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod cli;
pub mod prelude;
mod server;
mod util;
use crate::prelude::*;

fn init_tracing() {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_default_env());

    if cfg!(debug_assertions) {
        subscriber.with(console_subscriber::spawn()).init();
        trace!("tokio console enabled");
    } else {
        subscriber.init();
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let replicaof = cli.replicaof()?;

    let socket = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), cli.port);
    let server = server::Server::new(socket.into(), cli.max_connections, replicaof);

    server.run().await.context("run listener")?;

    Ok(())
}
