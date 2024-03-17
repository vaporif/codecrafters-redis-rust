use std::{
    env,
    net::{Ipv4Addr, SocketAddrV4},
};

fn init_tracing() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "trace")
    }

    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_default_env());

    #[cfg(console_subscriber)]
    {
        subscriber.with(console_subscriber::spawn()).init();
        tracing::trace!("tokio console enabled");
    }

    #[cfg(not(console_subscriber))]
    {
        // This block will only be compiled in release mode
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
    let server = redis_starter_rust::Server::new(socket.into(), cli.max_connections, replicaof);

    server.run().await?;

    Ok(())
}
