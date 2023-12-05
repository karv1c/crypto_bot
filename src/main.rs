pub mod common;
mod repeater;
mod request;
mod telegram;
pub mod schema;
mod trader;

//use common::DATABASE_URL;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use tracing::error;
use tracing_panic::panic_hook;
use tracing_subscriber::prelude::*;
use dotenvy::dotenv;
use std::env;
use std::sync::Arc;
use std::sync::Mutex;
use teloxide::prelude::*;

use crate::repeater::RepeaterModule;
pub mod abi;

#[tokio::main]
async fn main() {
    let file_appender = tracing_appender::rolling::daily("logs", "bot.log");
    let stdout_log = tracing_subscriber::fmt::layer().compact();
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let mut file_log = tracing_subscriber::fmt::layer().with_writer(non_blocking);
    file_log.set_ansi(false);
    tracing_subscriber::registry()
        .with(stdout_log.with_filter(tracing_subscriber::filter::LevelFilter::INFO))
        .with(file_log.with_filter(tracing_subscriber::filter::LevelFilter::INFO))
        .init();

    std::panic::set_hook(Box::new(panic_hook));

    let bot = Bot::from_env();
    let telegram_ids = Arc::new(Mutex::new(Vec::new()));

    let pool = get_connection_pool();

    let (tx_tx, rx_tx) = tokio::sync::broadcast::channel(32);
    let rx_tx_2 = tx_tx.subscribe();

    let tx_analyzer = tokio::spawn(async move {
        if let Err(e) = request::run(tx_tx).await {
            error!("Error: {e}");
        }
    });

    let pool_clone = pool.clone();
    let trader = tokio::spawn(async move {
        if let Err(e) = trader::run(rx_tx, pool_clone).await {
            error!("Error: {e}");
        }
    });
    let bot_clone = bot.clone();
    let telegram_ids_clone = telegram_ids.clone();
    let repeater = tokio::spawn(async move {
        let repeater = RepeaterModule::new(4, pool.clone(), bot_clone);
        if let Err(e) =  repeater.run(rx_tx_2, true, telegram_ids_clone).await {
            error!("Error: {e}");
        }
    });

    tokio::spawn(async move {
        if let Err(e) =  telegram::run(bot, telegram_ids).await {
            error!("Error: {e}");
        }
    });

    tokio::select!(
        _ = tx_analyzer => error!("Tx analyzer module terminated with error"),
        _ = trader => error!("Trader module terminated with error"),
        _ = repeater => error!("Repeater module terminated with error")
    )
}

pub fn get_connection_pool() -> Pool<ConnectionManager<PgConnection>> {
    dotenv().ok();
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = ConnectionManager::<PgConnection>::new(db_url);
    Pool::builder()
        //a.connection_customizer(Box::new(ConnectionOptions))
        .test_on_check_out(true)
        .build(manager)
        .expect("Could not build connection pool")
}
