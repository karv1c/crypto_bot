pub mod common;
mod db;
mod repeater;
mod request;
pub mod schema;
mod telegram;
mod trader;

use anyhow::Result;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use dotenvy::dotenv;
use kv::*;
use std::env;
use std::sync::Arc;
use teloxide::prelude::*;
use tracing::error;
use tracing_panic::panic_hook;
use tracing_subscriber::prelude::*;

use crate::repeater::RepeaterModule;
use crate::trader::SelfTx;
use crate::trader::TraderModule;
pub mod abi;

/* pub type SharedSettings =
    Arc<tokio::sync::Mutex<Bucket<'static, &'static str, Json<TradingSettings>>>>; */

pub type SharedSettings =
    Arc<tokio::sync::Mutex<BucketSettings>>;

pub struct BucketSettings(Bucket<'static, &'static str, Json<TradingSettings>>);

impl BucketSettings {
    pub fn new(settings: Bucket<'static, &'static str, Json<TradingSettings>>) -> Self {
        Self(settings)
    }

    pub fn get(&self) -> TradingSettings {
        self.0
            .get(&"trading_settings")
            .ok()
            .flatten()
            .map(|x| x.0)
            .unwrap_or_default()
    }

    pub fn set(&self, update_settings: TradingSettings) -> Result<Option<TradingSettings>, kv::Error> {
        let res = self.0.set(&"trading_settings", &Json(update_settings))?;
        self.0.flush()?;
        Ok(res.map(|json| json.0))
    }
}

/* pub struct SharedSettings(Arc<tokio::sync::Mutex<Bucket<'static, &'static str, Json<TradingSettings>>>>);

impl SharedSettings {
    fn new(settings: Arc<tokio::sync::Mutex<Bucket<'static, &'static str, Json<TradingSettings>>>>) -> Self {
        Self(settings)
    }

    fn get()
} */

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub struct TradingSettings {
    max_usd_buy_limit: f64,
    min_usd_buy_limit: f64,
    buy_slippage_tollerance: i32,
    sell_slippage_tollerance: i32,
    min_usd_sell_limit: f64,
    max_total_usd_buy_limit: f64,
    max_repeat_traders: i32,
    #[serde(default)]
    allow_similar_tokens: bool,
    #[serde(default = "default_zero_traders_replacement_hours")]
    zero_traders_replacement: i32,
    #[serde(default = "default_inactive_days_sell")]
    inactive_days_sell: i32,
}

fn default_zero_traders_replacement_hours() -> i32 {
    999
}

fn default_inactive_days_sell() -> i32 {
    2
}

impl Default for TradingSettings {
    fn default() -> Self {
        Self {
            max_usd_buy_limit: 10.0,
            min_usd_buy_limit: 5.0,
            buy_slippage_tollerance: 10,
            sell_slippage_tollerance: 30,
            min_usd_sell_limit: 3.0,
            max_total_usd_buy_limit: 20.0,
            max_repeat_traders: 4,
            allow_similar_tokens: false,
            zero_traders_replacement: 999,
            inactive_days_sell: 2,
        }
    }
}

pub struct RequsetTotalUsdBalance {
    pub bot: Bot,
    pub chat_id: ChatId,
}

#[tokio::main]
async fn main() -> Result<()> {
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

    let cfg = Config::new("./settings");
    let store = Store::new(cfg)?;

    let bucket = store.bucket::<&str, Json<TradingSettings>>(None)?;
    let settings_bucket = Arc::new(tokio::sync::Mutex::new(BucketSettings::new(bucket)));

    dotenv().ok();
    let bot_env = env::var("TELOXIDE_TOKEN").expect("TELOXIDE_TOKEN must be set");
    let bot = Bot::new(bot_env);
    //let telegram_ids = Arc::new(Mutex::new(Vec::new()));

    let pool = get_connection_pool();

    let (tx_total_usd_balance, rx_total_usd_balance) = tokio::sync::mpsc::channel(4);
    let (tx_repeated_tx, rx_repeated_tx) = tokio::sync::mpsc::channel::<SelfTx>(4);

    let (tx_tx, rx_tx) = tokio::sync::broadcast::channel(32);
    let rx_tx_2 = tx_tx.subscribe();

    let tx_analyzer = tokio::spawn(async move {
        if let Err(e) = request::run(tx_tx).await {
            error!("Error: {e}");
        }
    });

    let bot_clone = bot.clone();

    let pool_clone = pool.clone();
    let settings = settings_bucket.clone();
    let trader = tokio::spawn(async move {
        let mut trader = TraderModule::new(
            rx_total_usd_balance,
            rx_repeated_tx,
            pool_clone.clone(),
            bot_clone,
            settings,
        );
        if let Err(e) = trader.run(rx_tx, pool_clone).await {
            error!("Error: {e}");
        }
    });
    let bot_clone = bot.clone();
    //let telegram_ids_clone = telegram_ids.clone();
    let pool_clone = pool.clone();
    let settings = settings_bucket.clone();

    let repeater = tokio::spawn(async move {
        let repeater = RepeaterModule::new(pool_clone, bot_clone, tx_repeated_tx);
        if let Err(e) = repeater.run(rx_tx_2, true, settings).await {
            error!("Error: {e}");
        }
    });

    tokio::spawn(async move {
        if let Err(e) =
            telegram::run(bot, pool.clone(), settings_bucket, tx_total_usd_balance).await
        {
            error!("Error: {e}");
        }
    });

    tokio::select!(
        _ = tx_analyzer => error!("Tx analyzer module terminated with error"),
        _ = trader => error!("Trader module terminated with error"),
        _ = repeater => error!("Repeater module terminated with error")
    );
    Ok(())
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
