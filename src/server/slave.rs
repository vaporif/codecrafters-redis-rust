use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};

use crate::prelude::*;

use super::{
    cluster,
    codec::RespTcpStream,
    commands::{RedisMessage, SetData},
};

#[allow(unused)]
#[derive(Clone, Debug)]
pub enum Message {
    Set(SetData),
    RefreshOffset,
}

#[allow(unused)]
#[derive(DebugExtras)]
struct SlaveConnectionActor {
    offset: usize,
    #[debug_ignore]
    cluster_hnd: cluster::ActorHandle,
    socket: SocketAddr,
    #[debug_ignore]
    stream: RespTcpStream,
    #[debug_ignore]
    receive: tokio::sync::broadcast::Receiver<Message>,
}

impl SlaveConnectionActor {
    async fn new(
        socket: SocketAddr,
        stream: RespTcpStream,
        cluster_hnd: cluster::ActorHandle,
        receive: tokio::sync::broadcast::Receiver<Message>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            offset: 0,
            cluster_hnd,
            socket,
            stream,
            receive,
        })
    }

    #[instrument]
    async fn run(&mut self) -> anyhow::Result<()> {
        trace!("switched to slave mode");
        while let Ok(message) = self.receive.recv().await {
            trace!("received {:?}", message);
            match message {
                Message::Set(set_data) => {
                    self.stream
                        .send(RedisMessage::Set(set_data))
                        .await
                        .context("sent set to slave")?;
                }
                Message::RefreshOffset => {
                    self.stream.send(RedisMessage::ReplConfGetAck).await?;

                    let RedisMessage::ReplConfAck { offset } = self
                        .stream
                        .next()
                        .await
                        .context("ack any response")?
                        .context("ack response")?
                    else {
                        bail!("not offset");
                    };

                    self.cluster_hnd
                        .send(cluster::Message::NotifyReplicaOffset {
                            socket: self.socket,
                            offset,
                        })
                        .await?;
                }
            }

            trace!("slave processed messages");
        }

        Ok(())
    }
}

pub struct ActorHandle {
    cluster_hnd: cluster::ActorHandle,
    broadcast: tokio::sync::broadcast::Sender<Message>,
}

#[allow(unused)]
// TODO: limit replicas
impl ActorHandle {
    pub fn new(
        cluster_hnd: cluster::ActorHandle,
        broadcast: tokio::sync::broadcast::Sender<Message>,
    ) -> Self {
        Self {
            cluster_hnd,
            broadcast,
        }
    }

    pub async fn run(&self, socket: SocketAddr, slave_stream: RespTcpStream) -> anyhow::Result<()> {
        let cluster_hnd = self.cluster_hnd.clone();
        let mut actor = SlaveConnectionActor::new(
            socket,
            slave_stream,
            cluster_hnd.clone(),
            self.broadcast.subscribe(),
        )
        .await?;
        tokio::spawn(async move {
            if let Err(err) = actor.run().await {
                cluster_hnd
                    .send(cluster::Message::SlaveDisconnected { socket })
                    .await;
            }
        });

        Ok(())
    }

    pub async fn send(&self, message: Message) {
        match self.broadcast.send(message) {
            Ok(count) => trace!("broadcasted to {count} of slaves"),
            Err(_) => info!("no slaves connected"),
        }
    }
}
