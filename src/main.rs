use std::net::{Ipv4Addr, SocketAddrV4};

use clap::Parser;
use cli::Cli;
use server::connections_listen;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod cli;
pub mod prelude;
mod server;
use crate::prelude::*;

fn init_tracing(tokio_console: bool) {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_default_env());

    if tokio_console {
        subscriber.with(console_subscriber::spawn()).init();
        trace!("tokio console enabled");
    } else {
        subscriber.init();
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing(false);
    let cli = Cli::parse();

    let socket = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), cli.port);
    connections_listen(socket.into(), cli.max_connections)
        .await
        .context("listen error")?;

    Ok(())
}
