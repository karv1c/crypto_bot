use anyhow::Result;
use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};
use teloxide::{prelude::*, utils::command::BotCommands};

use crate::{schema, RequsetTotalUsdBalance, SharedSettings};

#[derive(Insertable, Debug)]
#[diesel(table_name = schema::tg_chat_ids)]
pub struct NewChatIdEntry {
    pub chat_id: i64,
}

impl NewChatIdEntry {
    fn new(chat_id: i64) -> Self {
        Self { chat_id }
    }
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = schema::tg_chat_ids)]
pub struct ChatIdEntry {
    pub id: i32,
    pub chat_id: i64,
}

#[derive(BotCommands, Clone, Debug)]
#[command(rename_rule = "snake_case")]
pub enum Command {
    Start,
    MaxUsdBuyLimit(f64),
    MinUsdBuyLimit(f64),
    BuySlippageTollernace(i32),
    SellSlippageTollernace(i32),
    MinUsdSellLimit(f64),
    GetTotalUsdBalance,
    MaxRepeatTraders(i32),
    MaxTotalUsdBuyLimit(f64),
    GetSettings,
    AllowSimilarTokens(bool),
    ZeroTradersReplacement(i32),
    InactiveDaysSell(i32),
}

pub async fn run(
    bot: Bot,
    db_pool: Pool<ConnectionManager<PgConnection>>,
    settings: SharedSettings,
    tx_total_usd_balance: tokio::sync::mpsc::Sender<RequsetTotalUsdBalance>,
) -> Result<()> {
    //let _ = settings;

    /* let handler =  |bot: Bot,
                  msg: Message,
                  cmd: Command,
                  db_pool: Pool<ConnectionManager<PgConnection>>,
                  settings: Bucket<'static, &'static str, Json<TradingSettings>>,|
                  async move {
      if let Err(e) = match cmd {
          Command::Start => start(msg, db_pool).await,
          Command::MaxUsdBuyLimit(max_usd_buy_limit) => todo!(),
          Command::MinUsdBuyLimit(min_usd_buy_limit) => todo!(),
          Command::MinUsdBuyLimit(slippage_tollerance) => todo!(),
          Command::SlippageTollernace(_) => todo!(),
      } {
          error!("Error while handling telegram bot request: {e}");
      }
      /* ids.lock().unwrap().push(msg.chat.id);
      bot.send_message(msg.chat.id, "Watching 0x03a800e6b5bb61dc18e079c65dabfb19ca22a6f6").await?;
      respond(())

      Ok(()) */
      Ok(())

    }; */

    let handler = Update::filter_message()
        .filter_command::<Command>()
        .endpoint(
            |bot: Bot,
             settings: SharedSettings,
             msg: Message,
             cmd: Command,
             db_pool: Pool<ConnectionManager<PgConnection>>,
             tx_total_usd_balance: tokio::sync::mpsc::Sender<RequsetTotalUsdBalance>| async move {
                //let settings = settings.lock().unwrap();
                let result = match cmd {
                    Command::Start => start(bot, msg.clone(), db_pool).await,
                    Command::MaxUsdBuyLimit(max_usd_buy_limit) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::MaxUsdBuyLimit(max_usd_buy_limit),
                        )
                        .await
                    }
                    Command::MinUsdBuyLimit(min_usd_buy_limit) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::MinUsdBuyLimit(min_usd_buy_limit),
                        )
                        .await
                    }
                    Command::BuySlippageTollernace(slippage_tollerance) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::BuySlippageTollernace(slippage_tollerance),
                        )
                        .await
                    }
                    Command::SellSlippageTollernace(slippage_tollerance) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::SellSlippageTollernace(slippage_tollerance),
                        )
                        .await
                    }
                    Command::MinUsdSellLimit(min_usd_sell_limit) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::MinUsdSellLimit(min_usd_sell_limit),
                        )
                        .await
                    }
                    Command::GetTotalUsdBalance => {
                        get_usd_balance(bot, msg.clone(), tx_total_usd_balance).await
                    }
                    Command::MaxRepeatTraders(max_repeat_traders) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::MaxRepeatTraders(max_repeat_traders),
                        )
                        .await
                    }
                    Command::MaxTotalUsdBuyLimit(max_total_usd_buy_limit) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::MaxTotalUsdBuyLimit(max_total_usd_buy_limit),
                        )
                        .await
                    }
                    Command::GetSettings => get_settings(bot, msg.clone(), settings.clone()).await,
                    Command::AllowSimilarTokens(enable) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::AllowSimilarTokens(enable),
                        )
                        .await
                    }
                    Command::ZeroTradersReplacement(hours) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::ZeroTradersReplacement(hours),
                        )
                        .await
                    }
                    Command::InactiveDaysSell(days) => {
                        set(
                            bot,
                            msg.clone(),
                            settings.clone(),
                            Command::InactiveDaysSell(days),
                        )
                        .await
                    }
                };

                result.into()
            },
        );

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![settings, db_pool, tx_total_usd_balance])
        .build()
        .dispatch()
        .await;

    //Command::repl(bot, handler).await;

    Ok(())
}

/* async fn handler(
    bot: Bot,
    msg: Message,
    cmd: Command,
    db_pool: Pool<ConnectionManager<PgConnection>>,
) -> ResponseResult<()> {
    let result = match cmd {
        Command::Start => start(bot, msg.clone(), db_pool).await,
        Command::MaxUsdBuyLimit(max_usd_buy_limit) => todo!(),
        Command::MinUsdBuyLimit(min_usd_buy_limit) => todo!(),
        Command::SlippageTollernace(slippage_tollerance) => todo!(),
    };

    Ok(())
} */

pub async fn start(
    bot: Bot,
    msg: Message,
    db_pool: Pool<ConnectionManager<PgConnection>>,
) -> Result<()> {
    let mut conn = db_pool.get()?;
    let new_chat_entry = NewChatIdEntry::new(msg.chat.id.0);
    conn.transaction(|conn| {
        diesel::insert_into(schema::tg_chat_ids::table)
            .values(&new_chat_entry)
            .on_conflict_do_nothing()
            .execute(conn)
    })?;
    bot.send_message(
        msg.chat.id,
        "Watching 0x03a800e6b5bb61dc18e079c65dabfb19ca22a6f6",
    )
    .await?;
    Ok(())
}

pub async fn set(bot: Bot, msg: Message, settings: SharedSettings, command: Command) -> Result<()> {
    let settings_guard = settings.lock().await;
    let mut update_settings = settings_guard.get();
    let response;
    match command {
        Command::MaxUsdBuyLimit(max_usd_buy_limit) => {
            update_settings.max_usd_buy_limit = max_usd_buy_limit;
            response = format!("Max USD buy limit is {max_usd_buy_limit}");
        }
        Command::MinUsdBuyLimit(min_usd_buy_limit) => {
            update_settings.min_usd_buy_limit = min_usd_buy_limit;
            response = format!("Min USD buy limit is {min_usd_buy_limit}");
        }
        Command::BuySlippageTollernace(slippage_tollerance) => {
            update_settings.buy_slippage_tollerance = slippage_tollerance;
            response = format!("Buy slippage tollerance is {slippage_tollerance}");
        }
        Command::SellSlippageTollernace(slippage_tollerance) => {
            update_settings.sell_slippage_tollerance = slippage_tollerance;
            response = format!("Sell slippage tollerance is {slippage_tollerance}");
        }
        Command::MinUsdSellLimit(min_usd_sell_limit) => {
            update_settings.min_usd_sell_limit = min_usd_sell_limit;
            response = format!("Min USD sell limit is {min_usd_sell_limit}");
        }
        Command::MaxRepeatTraders(max_repeat_traders) => {
            update_settings.max_repeat_traders = max_repeat_traders;
            response = format!("Max repeat traders is {max_repeat_traders}");
        }
        Command::MaxTotalUsdBuyLimit(max_total_usd_buy_limit) => {
            update_settings.max_total_usd_buy_limit = max_total_usd_buy_limit;
            response = format!("Max total USD buy limit {max_total_usd_buy_limit}");
        }
        Command::AllowSimilarTokens(enable) => {
            update_settings.allow_similar_tokens = enable;
            let enable = match enable {
                true => "enabled",
                false => "disabled",
            };
            response = format!("Similar tokens are {enable}");
        }
        Command::ZeroTradersReplacement(hours) => {
            update_settings.zero_traders_replacement = hours;
            response = format!("Zero traders replaced allowed in {hours} hours");
        }
        Command::InactiveDaysSell(days) => {
            update_settings.inactive_days_sell = days;
            response = format!("Inactive traders will sell all tokens in {days} days");
        }
        _ => response = format!("No set command"),
    };

    match settings_guard.set(update_settings) {
        Ok(_) => bot.send_message(msg.chat.id, response).await?,
        Err(e) => {
            bot.send_message(msg.chat.id, format!("Error settings new value: {e}"))
                .await?
        }
    };

    /* match settings_guard.set(&"trading_settings", &Json(update_settings)) {
        Ok(_) => {
            settings_guard.flush()?;
            //let x = bot.send_message(msg.chat.id, response).await;
            bot.send_message(msg.chat.id, response).await?
        }
        Err(e) => {
            bot.send_message(msg.chat.id, format!("Error settings new value: {e}"))
                .await?
        }
    }; */
    //let c = z.await?;

    Ok(())
}

pub async fn get_usd_balance(
    bot: Bot,
    msg: Message,
    tx_total_usd_balance: tokio::sync::mpsc::Sender<RequsetTotalUsdBalance>,
) -> Result<()> {
    let chat_id = RequsetTotalUsdBalance {
        bot,
        chat_id: msg.chat.id,
    };
    tx_total_usd_balance.send(chat_id).await?;
    Ok(())
}

pub async fn get_settings(bot: Bot, msg: Message, settings: SharedSettings) -> Result<()> {
    let settings_guard = settings.lock().await;
    let settings = settings_guard.get();
    bot.send_message(msg.chat.id, format!("{settings:?}"))
        .await?;

    //let c = z.await?;

    Ok(())
}
