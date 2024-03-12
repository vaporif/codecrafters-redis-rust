use async_channel::{bounded, unbounded, Receiver, Sender};
use std::{collections::HashMap, net::SocketAddr};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::prelude::*;
use connection::ConnectionActor;
mod connection;

pub async fn connections_listen(socket: SocketAddr, max_connections: usize) -> anyhow::Result<()> {
    let listener = TcpListener::bind(socket)
        .await
        .context("failed to listen")?;

    let (connection_snd, connections_processor) = bounded::<ConnectionActor>(max_connections);
    let (store_access_tx, store_access_rx) = unbounded::<StoreCommand>();

    run_connection_actors(connections_processor, store_access_tx);

    let storage_actor = StorageActor::new(store_access_rx);
    storage_actor.run_actor();

    loop {
        match listener.accept().await {
            Ok((tcp_stream, socket)) => {
                let connection = ConnectionActor::new(socket, tcp_stream);

                if let Err(e) = connection_snd.send(connection).await {
                    bail!("processing channel error {e}");
                }
            }
            Err(e) => error!("Failed to accept connection {:?}", e),
        }
    }
}

#[instrument(skip_all)]
fn run_connection_actors(
    receiver_tx: Receiver<ConnectionActor>,
    store_access_tx: Sender<StoreCommand>,
) {
    tokio::spawn(async move {
        trace!("connection actors loop started");
        loop {
            while let Ok(mut connection) = receiver_tx.recv().await {
                trace!("accepted new connection {:?}", &connection);
                let store_access_tx = store_access_tx.clone();
                tokio::spawn(async move {
                    trace!("processing connection {:?}", &connection);
                    if let Err(e) = connection.run_connection_actor(store_access_tx).await {
                        error!("Failed to process connection, error {:?}", e)
                    }
                });
            }
        }
    });
}

// TODO: Add sharding
struct StorageActor {
    data: HashMap<String, String>,
    receive_channel: Receiver<StoreCommand>,
}

impl StorageActor {
    fn new(receive_channel: Receiver<StoreCommand>) -> Self {
        Self {
            data: HashMap::new(),
            receive_channel,
        }
    }

    #[instrument(skip_all)]
    fn run_actor(mut self) {
        tokio::spawn(async move {
            loop {
                trace!("storage actor started");
                while let Ok(command) = self.receive_channel.recv().await {
                    trace!("new command received {:?}", &command);
                    let command_result = match command {
                        StoreCommand::Get(key, reply_channel_tx) => {
                            self.process_get_command(key, reply_channel_tx)
                        }
                        StoreCommand::Set(key, value, reply_channel_tx) => {
                            self.process_set_command(key, value, reply_channel_tx)
                        }
                    };

                    if let Err(e) = command_result {
                        error!("error during command {:?}", e);
                    }
                }
            }
        });
    }

    #[instrument(skip(self))]
    fn process_get_command(
        &self,
        key: String,
        reply_channel_tx: GetReplyChannel,
    ) -> anyhow::Result<()> {
        let data = self.data.get(&key).map(|s| s.to_string());
        reply_channel_tx
            .send(data)
            .map_err(|_| anyhow::Error::msg(format!("could not send result for get {key}")))
    }

    #[instrument(skip(self))]
    fn process_set_command(
        &mut self,
        key: String,
        value: String,
        reply_channel_tx: SetReplyChannel,
    ) -> anyhow::Result<()> {
        self.data
            .entry(key.clone())
            .and_modify(|e| *e = value.clone())
            .or_insert(value);

        reply_channel_tx
            .send(Ok(()))
            .map_err(|_| anyhow::Error::msg(format!("could not send result for set {key}")))
    }
}

pub type GetReplyChannel = oneshot::Sender<Option<String>>;
pub type SetReplyChannel = oneshot::Sender<Result<()>>;

#[derive(Debug)]
pub enum StoreCommand {
    Get(String, GetReplyChannel),
    Set(String, String, SetReplyChannel),
}
