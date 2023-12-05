use std::sync::{Mutex, Arc};

use anyhow::Result;
use teloxide::prelude::*;

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub enum State {
    #[default]
    Start,
}


pub async fn run(bot: Bot, ids: Arc<Mutex<Vec<ChatId>>>) -> Result<()> {
    let handler = Update::filter_message().endpoint(
        |bot: Bot, ids: Arc<Mutex<Vec<ChatId>>>, msg: Message| async move {
            ids.lock().unwrap().push(msg.chat.id);
            bot.send_message(msg.chat.id, "Watching 0x03a800e6b5bb61dc18e079c65dabfb19ca22a6f6").await?;
            respond(())
        },
    );

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![ids])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;

    Ok(())
}

