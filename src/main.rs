use std::{
    env,
    net::{Ipv4Addr, SocketAddrV4},
};

use anyhow::Context;
use tokio::net::TcpListener;

fn init_tracing() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "trace")
    }

    // env::set_var("RUST_BACKTRACE", "full");

    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_default_env());

    #[cfg(feature = "debug")]
    {
        subscriber.with(console_subscriber::spawn()).init();
        println!("tokio console enabled");
    }

    #[cfg(not(feature = "debug"))]
    {
        subscriber.init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use redis_starter_rust::Parser;

    init_tracing();
    let cli = redis_starter_rust::Cli::parse();
    let replicaof = cli.replicaof()?;

    let socket = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), cli.port);

    let listener = TcpListener::bind(socket)
        .await
        .context("listening on port")?;

    let (executor_messenger, join_handle) =
        redis_starter_rust::spawn_actor_executor(replicaof, cli.port, cli.max_connections).await;

    tokio::spawn(async move {
        let mut tcp_server = redis_starter_rust::TcpServer::new(listener, executor_messenger).await;
        tcp_server.start().await;
        tracing::trace!("tcp loop started");
    });

    if let Err(err) = join_handle.await.context("waiting on actor executor") {
        tracing::error!("fatal crash {:?}", err);

        Err(err)?;
    }

    Ok(())
}
