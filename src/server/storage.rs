use std::collections::HashMap;
use tokio::{sync::oneshot, time::Instant};

use crate::{prelude::*, ExecutorMessenger};

use super::commands::*;

pub type GetReplyChannel = oneshot::Sender<Option<String>>;
pub type SetReplyChannel = oneshot::Sender<Result<()>>;

#[derive(DebugExtras)]
pub enum Message {
    Get {
        #[debug_as_display]
        key: String,
        #[debug_ignore]
        channel: GetReplyChannel,
    },
    Set {
        data: SetData,
        #[debug_ignore]
        channel: Option<SetReplyChannel>,
    },
}
// TODO: Add sharding & active expiration
pub struct StorageActor {
    data: HashMap<String, String>,
    expire_info: HashMap<String, Instant>,
    executor_messenger: ExecutorMessenger,
    receive_channel: tokio::sync::mpsc::UnboundedReceiver<Message>,
}

impl StorageActor {
    pub fn new(
        executor_messenger: ExecutorMessenger,
        receive_channel: tokio::sync::mpsc::UnboundedReceiver<Message>,
    ) -> Self {
        Self {
            data: HashMap::new(),
            expire_info: HashMap::new(),
            executor_messenger,
            receive_channel,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(&mut self) {
        trace!("storage actor started");
        while let Some(command) = self.receive_channel.recv().await {
            trace!("new command received {:?}", &command);
            let command_result = match command {
                Message::Get { key, channel } => self.process_get_command(key, channel),
                Message::Set { data, channel } => self.process_set_command(data, channel).await,
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
    async fn process_set_command(
        &mut self,
        data: SetData,
        reply_channel_tx: Option<SetReplyChannel>,
    ) -> anyhow::Result<()> {
        let set_msg = data.clone();
        let SetData {
            key,
            value,
            arguments,
        } = data;

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

        self.executor_messenger
            .publish_msg(super::executor::Message::ForwardSetToReplica(set_msg))
            .await
            .context("could not send forwarding req")?;

        if let Some(reply_channel_tx) = reply_channel_tx {
            reply_channel_tx
                .send(Ok(()))
                .map_err(|_| anyhow::Error::msg(format!("could not send result for set {key}")))?
        }

        trace!("set done");

        Ok(())
    }
}

#[derive(Clone)]
pub struct ActorHandle {
    sender: tokio::sync::mpsc::UnboundedSender<Message>,
}

impl ActorHandle {
    pub fn new(executor_messenger: ExecutorMessenger) -> Self {
        let (sender, receive) = tokio::sync::mpsc::unbounded_channel();

        let mut actor = StorageActor::new(executor_messenger, receive);
        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub async fn send(&self, message: Message) -> Result<()> {
        self.sender.send(message).context("sending message")
    }
}
