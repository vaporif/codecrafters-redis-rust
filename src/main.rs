use std::{
    env,
    net::{Ipv4Addr, SocketAddrV4},
};

use clap::Parser;
use redis_starter_rust::prelude::*;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

fn init_tracing() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "trace")
    }
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_default_env());

    #[cfg(tokio_console)]
    {
        subscriber.with(console_subscriber::spawn()).init();
        tracing::trace!("tokio console enabled");
    }

    #[cfg(not(tokio_console))]
    {
        subscriber.init();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = redis_starter_rust::Cli::parse();
    let replicaof = cli.replicaof()?;

    let socket = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), cli.port);
    let server = redis_starter_rust::Server::new(socket.into(), cli.max_connections, replicaof);

    server.run().await.context("run listener")?;

    Ok(())
}
