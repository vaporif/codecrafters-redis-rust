use async_channel::{Receiver, Sender};
use std::collections::HashMap;
use tokio::{sync::oneshot, time::Instant};

use crate::prelude::*;

use super::commands::*;

pub type GetReplyChannel = oneshot::Sender<Option<String>>;
pub type SetReplyChannel = oneshot::Sender<Result<()>>;

#[derive(Debug)]
pub enum Message {
    Get(String, GetReplyChannel),
    Set(SetData, SetReplyChannel),
}
// TODO: Add sharding & active expiration
pub struct StorageActor {
    data: HashMap<String, String>,
    expire_info: HashMap<String, Instant>,
    receive_channel: Receiver<Message>,
}

impl StorageActor {
    pub fn new(receive_channel: Receiver<Message>) -> Self {
        Self {
            data: HashMap::new(),
            expire_info: HashMap::new(),
            receive_channel,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&mut self) {
        while let Ok(command) = self.receive_channel.recv().await {
            trace!("new command received {:?}", &command);
            let command_result = match command {
                Message::Get(key, reply_channel_tx) => {
                    self.process_get_command(key, reply_channel_tx)
                }
                Message::Set(set_data, reply_channel_tx) => {
                    self.process_set_command(set_data, reply_channel_tx)
                }
            };

            if let Err(e) = command_result {
                error!("error during command {:?}", e);
            }
        }
    }

    #[instrument(skip(self, reply_channel_tx))]
    fn process_get_command(
        &mut self,
        key: String,
        reply_channel_tx: GetReplyChannel,
    ) -> anyhow::Result<()> {
        let data = if self
            .expire_info
            .get(&key)
            .is_some_and(|expire| Instant::now() > *expire)
        {
            self.data.remove_entry(&key);
            None
        } else {
            self.data.get(&key).map(|s| s.to_string())
        };

        reply_channel_tx
            .send(data)
            .map_err(|_| anyhow::Error::msg(format!("could not send result for get {key}")))
    }

    #[instrument(skip(self, reply_channel_tx))]
    fn process_set_command(
        &mut self,
        set_data: SetData,
        reply_channel_tx: SetReplyChannel,
    ) -> anyhow::Result<()> {
        let SetData {
            key,
            value,
            arguments,
        } = set_data;

        if let Some(ttl) = arguments.ttl {
            let expires_at = Instant::now() + ttl;
            self.expire_info
                .entry(key.clone())
                .and_modify(|e| *e = expires_at)
                .or_insert(expires_at);
        }
        self.data
            .entry(key.clone())
            .and_modify(|e| *e = value.clone())
            .or_insert(value);

        reply_channel_tx
            .send(Ok(()))
            .map_err(|_| anyhow::Error::msg(format!("could not send result for set {key}")))
    }
}

#[derive(Clone)]
pub struct StorageActorHandle {
    sender: Sender<Message>,
}

impl StorageActorHandle {
    pub fn new() -> Self {
        let (sender, receive) = async_channel::unbounded();

        let mut actor = StorageActor::new(receive);
        tokio::spawn(async move {
            trace!("storage actor started");
            loop {
                actor.run().await;
            }
        });

        Self { sender }
    }

    pub async fn send(&self, message: Message) -> Result<()> {
        self.sender.send(message).await.context("sending message")
    }
}
