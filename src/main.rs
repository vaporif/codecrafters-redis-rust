use async_channel::bounded;
use clap::Parser;
use cli::Cli;
use server::server_listen;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod cli;
mod connection;
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

    server_listen(cli.socket.into(), cli.max_connections)
        .await
        .context("listen error")?;

    Ok(())
}

enum Message {
    Ping,
    Pong,
}

impl Message {
    fn to_resp(&self) -> anyhow::Result<Message> {
        match self {
            Message::Ping => Ok(Message::Pong),
            _ => bail!("could not be responded"),
        }
    }

    fn to_binary_resp(&self) -> &'static [u8] {
        match self {
            Message::Ping => "Ping".as_bytes(),
            Message::Pong => "Pong".as_bytes(),
        }
    }
}
