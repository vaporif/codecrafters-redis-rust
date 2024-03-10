use async_channel::bounded;
use clap::Parser;
use cli::Cli;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod cli;
pub mod prelude;
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

#[tokio::main()]
async fn main() -> Result<()> {
    init_tracing(false);
    let cli = Cli::parse();

    let (connection_snd, connections_processor) = bounded::<TcpStream>(cli.max_connections);

    let listener = TcpListener::bind(cli.socket)
        .await
        .context("failed to listen")?;
    tokio::spawn(async move {
        loop {
            while let Ok(mut stream) = connections_processor.recv().await {
                trace!("accepted new connection");
                tokio::spawn(async move {
                    if let Err(e) = stream.write_all("+PONG\r\n".as_bytes()).await {
                        eprintln!("Failed to process stream, error {e}")
                    }
                });
            }
        }
    });

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                if let Err(e) = connection_snd.send(stream).await {
                    eprintln!("processing channel error {e}");
                    break;
                }
            }
            Err(e) => eprintln!("Failed to accept connection {e}"),
        }
    }

    Ok(())
}

enum Message {
    Ping,
    Pong,
}

impl Message {
    fn to_resp(&self) -> &'static [u8] {
        match self {
            Message::Ping => "Ping".as_bytes(),
            Message::Pong => "Pong".as_bytes(),
        }
    }
}
