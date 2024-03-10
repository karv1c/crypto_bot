use crate::common::*;
use crate::schema::{repeat_traders, tg_chat_ids};
use crate::telegram::ChatIdEntry;
use crate::trader::SelfTx;
use crate::SharedSettings;
use anyhow::{bail, Result};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use ethers::abi::Token;
use ethers::prelude::*;
use k256::ecdsa::SigningKey;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use teloxide::prelude::*;
use tokio::select;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct SwapQueue {
    tx_queue: Arc<Mutex<VecDeque<SwapTx>>>,
}

impl SwapQueue {
    pub fn new() -> Self {
        Self {
            tx_queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.tx_queue.lock().unwrap().len()
    }

    pub async fn pop_front(&self) -> SwapTx {
        let fut = PopFront::new(self);
        fut.await
    }

    pub fn push(&self, value: SwapTx) {
        self.tx_queue.lock().unwrap().push_back(value)
    }
}

pub struct PopFront {
    queue: SwapQueue,
    sleeper: Pin<Box<tokio::time::Sleep>>,
}

impl PopFront {
    fn new(queue: &SwapQueue) -> PopFront {
        PopFront {
            queue: queue.clone(),
            sleeper: Box::pin(tokio::time::sleep(std::time::Duration::from_millis(300))),
        }
    }
}

impl Future for PopFront {
    type Output = SwapTx;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let tx = self.queue.tx_queue.lock().unwrap().pop_front();

        match tx {
            Some(tx) => Poll::Ready(tx),
            None => {
                if let Poll::Ready(_) = self.sleeper.as_mut().poll(cx) {
                    self.sleeper
                        .as_mut()
                        .reset(tokio::time::Instant::now() + std::time::Duration::from_millis(300))
                }
                Poll::Pending
                //cx.waker().wake_by_ref();
                //Poll::Pending
            }
        }
    }
}

pub struct RepeaterModule {
    //pub provider: SignerMiddleware<Provider<Http>, Wallet<SigningKey>>,
    pub tx_queue: SwapQueue,
    pub db_pool: Pool<ConnectionManager<PgConnection>>,
    pub bot: Bot,
    pub tx_repeat_tx: tokio::sync::mpsc::Sender<SelfTx>,
}

impl RepeaterModule {
    pub fn new(
        db_pool: Pool<ConnectionManager<PgConnection>>,
        bot: Bot,
        tx_repeat_tx: tokio::sync::mpsc::Sender<SelfTx>,
    ) -> Self {
        Self {
            db_pool,
            tx_queue: SwapQueue::new(),
            bot,
            tx_repeat_tx,
        }
    }

    pub async fn run(
        &self,
        mut rx_tx: tokio::sync::broadcast::Receiver<SwapTx>,
        enabled: bool,
        settings: SharedSettings,
    ) -> Result<()> {
        let tx_queue: SwapQueue = self.tx_queue.clone();
        let db_pool = self.db_pool.clone();
        let bot = self.bot.clone();
        let tx_repeated_tx = self.tx_repeat_tx.clone();
        tokio::spawn(async move {
            //let ids = ids.clone();
            loop {
                let tx = tx_queue.pop_front().await;
                if let Err(e) = handle_transaction(
                    db_pool.clone(),
                    tx,
                    settings.clone(),
                    bot.clone(),
                    tx_repeated_tx.clone(),
                )
                .await
                {
                    error!("Error while handling tx: {e}");
                }
            }
        });
        loop {
            select! {
                tx = rx_tx.recv() => {
                    match tx {
                        Ok(tx) => {
                            if enabled {
                                self.tx_queue.push(tx);
                                /* if let Err(e) = self.handle_transaction(tx).await {
                                    error!("Error while handling tx: {e}");
                                } */
                            }
                        }
                        Err(e) => {
                            error!("Error receiving tx: {e}, queue len: {}", self.tx_queue.len());
                            if matches!(e, tokio::sync::broadcast::error::RecvError::Lagged(_)) {
                                continue;
                            }
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub async fn handle_transaction(
    db_pool: Pool<ConnectionManager<PgConnection>>,
    swap_tx: SwapTx,
    settings: SharedSettings,
    bot: Bot,
    tx_repeated_tx: tokio::sync::mpsc::Sender<SelfTx>,
) -> Result<()> {
    let timer_tx_recieved = std::time::Instant::now();
    let settings_bucket = settings.lock().await;
    let settings = settings_bucket.get();

    //let max_repeat = settings.max_repeat_traders as i64;

    let Some(trader) = swap_tx.need_repeat(db_pool.clone(), &settings).await? else {
        return Ok(());
    };

    info!(timer_need_repeat=%timer_tx_recieved.elapsed().as_millis());

    let tx = swap_tx.tx();
    info!(hash=?tx.hash, method=?swap_tx.method, action=?swap_tx.action, "Trying to repeat");

    if !trader.max_single_buy.is_normal() || !trader.min_single_buy.is_normal() {
        bail!("Max single buy or min single buy is NaN");
    }

    if !swap_tx.is_buy() {
        if trader.token_amount == 0.0 {
            warn!("Trader have no tokens in db");
            return Ok(());
        }
    }

    let repeat_address = H160::from_str(REAPEAT_ADDRESS)?;
    let pancake_swap_address = H160::from_str(PANCAKESWAP_ADDRESS)?;

    info!("Traders input data: {:?}", swap_tx.input);
    let mut repeat_input_tokens = swap_tx.input.clone();
    repeat_input_tokens.iter_mut().for_each(|token| {
        if token.clone().into_address().is_some() {
            *token = Token::Address(repeat_address);
        }
    });

    let prim_decimals = swap_tx.prim_decimals().await?;
    let token_decimals = swap_tx.token_decimals().await?;

    let denominator = swap_tx.demonitor().await?;

    let usd_in_prim = 1.0 / denominator;

    let usd_buy_max_limit = settings.max_usd_buy_limit;
    let usd_buy_min_limit = settings.min_usd_buy_limit;
    let buy_slippage_tollerance = settings.buy_slippage_tollerance;
    let sell_slippage_tollerance = settings.sell_slippage_tollerance;
    let min_usd_sell_limit = settings.min_usd_sell_limit;

    let max_cash =
        U256::from((usd_buy_max_limit * usd_in_prim * 10.0_f64.powi(prim_decimals as i32)) as u128);
    let max_single_buy =
        U256::from((trader.max_single_buy * 10.0_f64.powi(prim_decimals as i32)) as u128);
    let min_cash =
        U256::from((usd_buy_min_limit * usd_in_prim * 10.0_f64.powi(prim_decimals as i32)) as u128);
    let min_single_buy =
        U256::from((trader.min_single_buy * 10.0_f64.powi(prim_decimals as i32)) as u128);
    let trader_token_amount =
        U256::from((trader.token_amount * 10.0_f64.powi(token_decimals as i32)) as u128);

    let reapeat_amount = |x: U256| {
        (max_cash - min_cash)
            .checked_div((max_single_buy - min_single_buy) * (x.saturating_sub(min_single_buy)))
            .unwrap_or_default()
            + min_cash
    };

    let token_allowed = swap_tx
        .token_allowance(REAPEAT_ADDRESS, PANCAKESWAP_ADDRESS)
        .await?;

    let function = swap_tx.function();

    let repeat_value = if tx.value.is_zero() {
        U256::zero()
    } else {
        reapeat_amount(tx.value)
    };

    info!(%repeat_value, value=%tx.value);

    let provider = swap_tx.provider();
    let eth_balance = provider.get_balance(provider.address(), None).await?;

    info!(%eth_balance);

    let token_balance = swap_tx.token_balance(REAPEAT_ADDRESS).await?;

    if !swap_tx.is_buy() {
        if token_balance <= U256::zero() {
            info!("Zero token balace");
            return Ok(());
        }
    }

    let mut conn = db_pool.get()?;
    let keyhash = swap_tx.get_keyhash();
    let keyhash_token_balance: f64 = conn.transaction(|conn| {
        repeat_traders::table
            .filter(repeat_traders::keyhash.eq(keyhash.clone()))
            .select(repeat_traders::token_amount)
            .first::<f64>(conn)
    })?;

    let keyhash_token_balance =
        U256::from((keyhash_token_balance * 10.0f64.powi(token_decimals as i32)) as u128);

    let prim_balance = swap_tx.prim_balance(REAPEAT_ADDRESS).await?;

    if swap_tx.is_buy() {
        let is_exceeded = swap_tx
            .is_exceeded(settings.max_total_usd_buy_limit, keyhash_token_balance)
            .await?;

        info!(timer_is_exceeded=%timer_tx_recieved.elapsed().as_millis());
        if is_exceeded {
            info!("Total token USD buy limit is exceeded");
            return Ok(());
        }

        if prim_balance <= U256::zero() {
            info!("Zero prim balace");
            return Ok(());
        }
    }

    let amount_in_balance = if swap_tx.is_buy() {
        prim_balance
    } else {
        keyhash_token_balance.min(token_balance)
    };
    let mut repeat_value = repeat_value.min(eth_balance);

    match &swap_tx.method {
        SwapMethod::SwapExactTokensForTokens
        | SwapMethod::SwapExactTokensForETH
        | SwapMethod::SwapExactTokensForTokensSupportingFeeOnTransferTokens
        | SwapMethod::SwapExactTokensForETHSupportingFeeOnTransferTokens => {
            let Some(trader_amount_in) = swap_tx.input[0].clone().into_uint() else {
                bail!("First input token is not Uint: {:?}", swap_tx.method);
            };
            let Some(trader_amount_out_min) = swap_tx.input[1].clone().into_uint() else {
                bail!("Second input token is not Uint: {:?}", swap_tx.method);
            };

            match &swap_tx.action {
                SwapAction::Buy => {
                    let new_amount_in = reapeat_amount(trader_amount_in);
                    if new_amount_in > amount_in_balance {
                        bail!("Not enough prim tokens {new_amount_in} > {amount_in_balance}");
                    }

                    info!(%new_amount_in);

                    repeat_input_tokens[0] = Token::Uint(new_amount_in);

                    let ratio = new_amount_in * 1000 / trader_amount_in;

                    let new_amount_out_min = trader_amount_out_min
                        .checked_mul(ratio)
                        .unwrap_or(U256::zero())
                        .checked_div(U256::from(1000_i64))
                        .unwrap_or(U256::zero());

                    let slippage_tollerance = new_amount_out_min
                        .checked_div(U256::from(100))
                        .unwrap_or_default()
                        .checked_mul(U256::from(buy_slippage_tollerance))
                        .unwrap_or_default();

                    repeat_input_tokens[1] =
                        Token::Uint(new_amount_out_min.saturating_sub(slippage_tollerance));
                }
                SwapAction::Sell => {
                    let min_token_sell = swap_tx.min_token_sell(min_usd_sell_limit).await?;
                    let new_amount_in = U256::from(
                        (trader_amount_in.min(U256::from(U128::MAX)).as_u128() as f64
                            / trader_token_amount.min(U256::from(U128::MAX)).as_u128() as f64
                            * keyhash_token_balance.min(U256::from(U128::MAX)).as_u128() as f64)
                            as u128,
                    )
                    .max(min_token_sell)
                    .min(amount_in_balance);

                    info!(%new_amount_in);

                    repeat_input_tokens[0] = Token::Uint(new_amount_in);

                    let ratio = new_amount_in * 1_000 / trader_amount_in;

                    let new_amount_out_min = trader_amount_out_min
                        .checked_mul(ratio)
                        .unwrap_or(U256::zero())
                        .checked_div(U256::from(1000_i64))
                        .unwrap_or(U256::zero());

                    let slippage_tollerance = new_amount_out_min
                        .checked_div(U256::from(100))
                        .unwrap_or_default()
                        .checked_mul(U256::from(sell_slippage_tollerance))
                        .unwrap_or_default();

                    repeat_input_tokens[1] =
                        Token::Uint(new_amount_out_min.saturating_sub(slippage_tollerance));
                }
                _ => unreachable!(),
            }
        }
        SwapMethod::SwapTokensForExactTokens | SwapMethod::SwapTokensForExactETH => {
            let Some(trader_amount_out) = swap_tx.input[0].clone().into_uint() else {
                bail!("First input token is not Uint: {:?}", swap_tx.method);
            };
            let Some(trader_amount_in_max) = swap_tx.input[1].clone().into_uint() else {
                bail!("Second input token is not Uint: {:?}", swap_tx.method);
            };

            match &swap_tx.action {
                SwapAction::Buy => {
                    let new_amount_in_max = reapeat_amount(trader_amount_in_max);
                    if new_amount_in_max > amount_in_balance {
                        bail!("Not enough prim tokens {new_amount_in_max} > {amount_in_balance}");
                    }

                    let slippage_tollerance = new_amount_in_max
                        .checked_div(U256::from(100))
                        .unwrap_or_default()
                        .checked_mul(U256::from(buy_slippage_tollerance))
                        .unwrap_or_default();

                    repeat_input_tokens[1] =
                        Token::Uint(new_amount_in_max.saturating_add(slippage_tollerance));

                    let ratio = new_amount_in_max * 1000 / trader_amount_in_max;

                    let new_amount_out = trader_amount_out
                        .checked_mul(ratio)
                        .unwrap_or(U256::zero())
                        .checked_div(U256::from(1000_i64))
                        .unwrap_or(U256::zero());

                    repeat_input_tokens[0] = Token::Uint(new_amount_out);
                }
                SwapAction::Sell => {
                    let min_token_sell = swap_tx.min_token_sell(min_usd_sell_limit).await?;
                    //let min_token_sell = U256::from((min_usd_sell_limit * 10_f64.powi(18)) as u128)/token_price;
                    let new_amount_in_max = U256::from(
                        (trader_amount_in_max.min(U256::from(U128::MAX)).as_u128() as f64
                            / trader_token_amount.min(U256::from(U128::MAX)).as_u128() as f64
                            * keyhash_token_balance.min(U256::from(U128::MAX)).as_u128() as f64)
                            as u128,
                    )
                    .max(min_token_sell)
                    .min(amount_in_balance);

                    let slippage_tollerance = new_amount_in_max
                        .checked_div(U256::from(100))
                        .unwrap_or_default()
                        .checked_mul(U256::from(sell_slippage_tollerance))
                        .unwrap_or_default();

                    repeat_input_tokens[1] =
                        Token::Uint(new_amount_in_max.saturating_add(slippage_tollerance));

                    let ratio = new_amount_in_max * 1000 / trader_amount_in_max;

                    let new_amount_out = trader_amount_out
                        .checked_mul(ratio)
                        .unwrap_or(U256::zero())
                        .checked_div(U256::from(1000_i64))
                        .unwrap_or(U256::zero());

                    repeat_input_tokens[0] = Token::Uint(new_amount_out);
                }
                _ => unreachable!(),
            }
        }
        SwapMethod::SwapExactETHForTokens
        | SwapMethod::SwapExactETHForTokensSupportingFeeOnTransferTokens => {
            if eth_balance < U256::from(40000000000000000_i64) {
                if matches!(swap_tx.action, SwapAction::Buy) {
                    bail!("Eth only left for comission: {:?}", swap_tx.method);
                }
            }

            let Some(trader_amount_out_min) = swap_tx.input[0].clone().into_uint() else {
                bail!("First input token is not Uint: {:?}", swap_tx.method);
            };

            let ratio = repeat_value * 1000 / tx.value;

            let new_amount_out_min = trader_amount_out_min
                .checked_mul(ratio)
                .unwrap_or(U256::zero())
                .checked_div(U256::from(1000_i64))
                .unwrap_or(U256::zero());

            let slippage_tollerance = new_amount_out_min
                .checked_div(U256::from(100))
                .unwrap_or_default()
                .checked_mul(U256::from(buy_slippage_tollerance))
                .unwrap_or_default();

            repeat_input_tokens[0] =
                Token::Uint(new_amount_out_min.saturating_sub(slippage_tollerance));
        }
        SwapMethod::SwapETHForExactTokens => {
            if eth_balance < U256::from(40000000000000000_i64) {
                if matches!(swap_tx.action, SwapAction::Buy) {
                    bail!("Eth only left for comission: {:?}", swap_tx.method);
                }
            }
            let Some(trader_amount_out) = swap_tx.input[0].clone().into_uint() else {
                bail!("First input token is not Uint: {:?}", swap_tx.method);
            };
            let ratio = repeat_value * 1000 / tx.value;

            let new_amount_out = trader_amount_out
                .checked_mul(ratio)
                .unwrap_or(U256::zero())
                .checked_div(U256::from(1000_i64))
                .unwrap_or(U256::zero());

            repeat_input_tokens[0] = Token::Uint(new_amount_out);
        }
    }
    info!(timer_calculate_new_tx=%timer_tx_recieved.elapsed().as_millis());

    info!(%token_allowed);

    if token_allowed < U256::MAX {
        info!("token_allowed: {token_allowed:?}/{:?}", U256::MAX);
        swap_tx.token_approve_max(PANCAKESWAP_ADDRESS).await?;
    }

    let slippage_tollerance = repeat_value
        .checked_div(U256::from(100))
        .unwrap_or_default()
        .checked_mul(U256::from(buy_slippage_tollerance))
        .unwrap_or_default();

    repeat_value = repeat_value
        .checked_add(slippage_tollerance)
        .unwrap_or_default();

    info!("Repeat input data: {:?}", repeat_input_tokens);
    info!("Repeat value: {:?}", repeat_value);

    let gas_limit = tx.gas.max(U256::from(600000_u32));
    let gas_price = tx.gas_price.unwrap_or(U256::from(4000000000_u64));
    let gas_price = U256::from(gas_price + U256::from(1000000000_u64));

    info!(?tx);

    let repeat_input = function.encode_input(&repeat_input_tokens)?;
    let repeat_tx = TransactionRequest::new()
        .data(repeat_input)
        .value(repeat_value)
        .from(repeat_address)
        .gas(gas_limit)
        .gas_price(gas_price)
        .to(pancake_swap_address);
    info!("repeating tx...");
    info!(?repeat_tx);

    let receipt = send_tx(provider, repeat_tx).await?;
    info!(timer_send_tx=%timer_tx_recieved.elapsed().as_millis());

    let self_tx = SelfTx {
        tx: receipt.transaction_hash,
        key_hash: Some(keyhash),
        action: None,
        amount: None,
    };

    tx_repeated_tx.send(self_tx).await?;

    let tx = provider.get_transaction(receipt.transaction_hash).await?;
    info!(timer_get_tx=%timer_tx_recieved.elapsed().as_millis());
    let mut conn = db_pool.get()?;
    let chat_ids = conn.transaction(|conn| tg_chat_ids::table.load::<ChatIdEntry>(conn))?;

    for chat_id_entry in chat_ids.iter() {
        let response = match receipt.status {
            Some(result) => {
                if result.is_zero() {
                    "Failed tx"
                } else {
                    "Repeated tx"
                }
            }
            None => "Undefined tx result",
        };
        let chat_id = chat_id_entry.chat_id;
        bot.send_message(ChatId(chat_id), format!("{response}: https://bscscan.com/tx/{:?} \nOriginal tx: https://bscscan.com/tx/{:?}", receipt.transaction_hash, swap_tx.tx.hash)).await?;
    }

    info!("{tx:?}");
    Ok(())
}

pub async fn send_tx(
    provider: &SignerMiddleware<Provider<Http>, Wallet<SigningKey>>,
    repeat_tx: TransactionRequest,
) -> Result<TransactionReceipt> {
    let pending = provider.send_transaction(repeat_tx, None).await?.await?;

    match pending {
        Some(result) => Ok(result),
        None => bail!("tx dropped from mempool"),
    }
}
