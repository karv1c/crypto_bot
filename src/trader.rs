use crate::abi::Erc20Token;
use crate::common::*;
use crate::db::DbPoolConnection;
use crate::schema::repeat_traders;
use crate::schema::tokens;
use crate::schema::traders;
use crate::schema::transactions;
use crate::telegram::ChatIdEntry;
use crate::RequsetTotalUsdBalance;
use crate::SharedSettings;
use anyhow::{anyhow, bail, Result};
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use diesel::PgConnection;
use ethers::abi::Token;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use teloxide::requests::Requester;
use teloxide::types::ChatId;
use teloxide::Bot;
use tokio::{select, sync::broadcast::Receiver};
use tracing::error;
use tracing::info;

use ethers::{
    abi::ParamType::{self, Uint as ParamUint},
    prelude::*,
};
use std::str::FromStr;

pub const LOG_PARAMS: [ParamType; 4] = [
    ParamUint(256),
    ParamUint(256),
    ParamUint(256),
    ParamUint(256),
];

use crate::schema;

#[derive(Queryable, Selectable, Insertable, Debug)]
#[diesel(table_name = schema::transactions)]
pub struct TxEntry {
    pub id: i64,
    pub trader: String,
    pub prim: String,
    pub token: String,
    pub key_hash: String,
    pub tx_hash: String,
    pub price: f64,
    pub price_usd: f64,
    pub token_amount: f64,
    pub prim_amount: f64,
    pub usd_amount: f64,
    pub swap_action: String,
    pub ts: i64,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = schema::transactions)]
pub struct NewTxEntry {
    pub trader: String,
    pub prim: String,
    pub token: String,
    pub key_hash: String,
    pub tx_hash: String,
    pub price: f64,
    pub price_usd: f64,
    pub token_amount: f64,
    pub prim_amount: f64,
    pub usd_amount: f64,
    pub swap_action: String,
    pub ts: i64,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = schema::tokens)]
pub struct NewTokenEntry {
    pub token: String,
    pub creation_ts: i64,
    pub tradable: Option<bool>,
    pub symbol: Option<String>,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = schema::tokens)]
pub struct TokenEntry {
    pub id: i32,
    pub token: String,
    pub creation_ts: i64,
    pub tradable: Option<bool>,
    pub symbol: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = schema::repeat_traders)]
pub struct NewRepeatTraderEntry {
    pub keyhash: String,
    pub token_amount: f64,
}

#[derive(Queryable, Selectable, AsChangeset, Debug)]
#[diesel(table_name = schema::repeat_traders)]
pub struct RepeatTraderEntry {
    pub id: i32,
    pub keyhash: String,
    pub token_amount: f64,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = schema::traders)]
pub struct TraderEntry {
    pub id: i64,
    pub trader: String,
    pub token: String,
    pub key_hash: String,
    pub buy_count: i32,
    pub sell_count: i32,
    pub deposit: f64,
    pub token_amount: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub sum_buy_usd_amount: f64,
    pub sum_buy_token_amount: f64,
    pub wmean_buy: f64,
    pub sum_sell_usd_amount: f64,
    pub sum_sell_token_amount: f64,
    pub wmean_sell: f64,
    pub wmean_ratio: f32,
    pub max_single_buy: f64,
    pub min_single_buy: f64,
    pub max_single_sell: f64,
    pub min_single_sell: f64,
    pub max_buy_seq: f64,
    pub cur_buy_seq: f64,
    pub max_sell_seq: f64,
    pub cur_sell_seq: f64,
    pub active: bool,
    pub profit: f32,
    pub buy_sell: f32,
    pub ts: i64,
}

impl TraderEntry {
    pub async fn sell_traders_token(
        &self,
        amount_in: U256,
        context: Arc<SwapContext>,
    ) -> Result<TransactionReceipt> {
        let repeat_address = H160::from_str(REAPEAT_ADDRESS).unwrap();
        let token_address = H160::from_str(&self.token).unwrap();
        for case in SellAllCase::all() {
            let amount_out_min = match case.compute_amount {
                true => {
                    let amount_out_min = context
                        .router_contract
                        .get_amounts_out(amount_in, vec![token_address, case.address])
                        .call()
                        .await;
                    let Ok(amount_out_min) = amount_out_min else {
                        continue;
                    };
                    let Some(amount_out_min) = amount_out_min.last() else {
                        continue;
                    };
                    *amount_out_min
                }
                false => U256::zero(),
            };
            let sell_tx = context.router_contract.swap_exact_tokens_for_eth(
                amount_in,
                amount_out_min,
                vec![token_address, case.address],
                repeat_address,
                U256::from(now() + 3 * 3600),
            );
            let gas_limit = U256::from(2000000_u32);
            let gas_price = U256::from(3000000000_u32);
            let receipt = sell_tx
                .gas_price(gas_price)
                .from(repeat_address)
                .gas(gas_limit)
                .send()
                .await?
                .await;

            let receipt = match receipt {
                Ok(r) => r,
                Err(e) => {
                    error!("Error selling traders tokens {e}");
                    continue;
                }
            };

            let Some(receipt) = receipt else {
                bail!("No receipt for sell all traders tokens {case:?}, Token: {token_address:#x}");
            };
            if let Some(status) = receipt.status {
                if !status.is_zero() {
                    return Ok(receipt);
                }
            }
        }
        bail!("All attempts to sell traders token failed. Token: {token_address:#x}");
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = schema::traders)]
pub struct NewTraderEntry {
    pub trader: String,
    pub token: String,
    pub key_hash: String,
    pub deposit: f64,
    pub token_amount: f64,
    pub buy_count: i32,
    pub sell_count: i32,
    pub min_price: f64,
    pub max_price: f64,
    pub sum_buy_usd_amount: f64,
    pub sum_buy_token_amount: f64,
    pub wmean_buy: f64,
    pub sum_sell_usd_amount: f64,
    pub sum_sell_token_amount: f64,
    pub wmean_sell: f64,
    pub wmean_ratio: f32,
    pub max_single_buy: f64,
    pub min_single_buy: f64,
    pub max_single_sell: f64,
    pub min_single_sell: f64,
    pub max_buy_seq: f64,
    pub cur_buy_seq: f64,
    pub max_sell_seq: f64,
    pub cur_sell_seq: f64,
    pub active: bool,
    pub profit: f32,
    pub buy_sell: f32,
    pub ts: i64,
}

pub struct TraderModule {
    pub context: Option<Arc<SwapContext>>,
    pub rx_total_usd_balance: tokio::sync::mpsc::Receiver<RequsetTotalUsdBalance>,
    pub db_pool: Pool<ConnectionManager<PgConnection>>,
    pub rx_repeated_tx: tokio::sync::mpsc::Receiver<SelfTx>,
    pub self_tx_pool: HashMap<H256, SelfTx>,
    pub bot: Bot,
    pub settings: SharedSettings,
}

#[derive(Clone)]
pub struct SelfTx {
    pub tx: H256,
    pub key_hash: Option<String>,
    pub action: Option<SwapAction>,
    pub amount: Option<f64>,
}

impl SelfTx {
    fn complete(&self, other: SelfTx) -> Self {
        Self {
            tx: self.tx,
            key_hash: self.key_hash.clone().or(other.key_hash),
            action: self.action.clone().or(other.action),
            amount: self.amount.or(other.amount),
        }
    }

    fn is_full(&self) -> bool {
        self.key_hash.is_some() && self.action.is_some() && self.amount.is_some()
    }
}

pub async fn get_transaction_confirmation(
    tx: Result<SwapTx, tokio::sync::broadcast::error::RecvError>,
) -> Option<SwapTx> {
    if let Ok(mut swap_tx) = tx {
        let receipt = swap_tx.get_reciept().await;

        if receipt.is_some() {
            swap_tx.receipt = receipt;
            return Some(swap_tx);
        }
    }
    None
}

impl TraderModule {
    pub fn new(
        rx_total_usd_balance: tokio::sync::mpsc::Receiver<RequsetTotalUsdBalance>,
        rx_repeated_tx: tokio::sync::mpsc::Receiver<SelfTx>,
        db_pool: Pool<ConnectionManager<PgConnection>>,
        bot: Bot,
        settings: SharedSettings,
    ) -> Self {
        Self {
            context: None,
            rx_total_usd_balance,
            db_pool,
            rx_repeated_tx,
            self_tx_pool: HashMap::new(),
            bot,
            settings,
        }
    }

    pub async fn handle_approved_transaction(
        &mut self,
        swap_tx: Option<SwapTx>,
        db_pool: Pool<ConnectionManager<PgConnection>>,
    ) -> Result<()> {
        let Some(swap_tx) = swap_tx else {
            return Ok(());
        };

        if self.context.is_none() {
            self.context = Some(swap_tx.context.clone());
        }

        let Some(receipt) = swap_tx.receipt.clone() else {
            return Ok(());
        };

        let decimals_in = swap_tx.decimals_in().await?;
        let decimals_out = swap_tx.decimals_out().await?;

        let wallet_address = swap_tx.from.clone();

        let prim_address = swap_tx.prim_address();
        let token_address = swap_tx.token_address();

        let usd_per_prim = swap_tx.demonitor().await?;

        let key_hash = swap_tx.get_keyhash();

        let logs = receipt.logs;
        let Some(last_swap_log) = logs
            .iter()
            .filter(|log| {
                log.topics
                    .first()
                    .is_some_and(|hash| H256::from_str(SWAP_HASH).unwrap().eq(hash))
            })
            .last()
        else {
            return Ok(());
        };

        let (amount_in, amount_out) = match swap_tx.method {
            SwapMethod::SwapETHForExactTokens => {
                let Token::Uint(amount_out) = swap_tx.input[0] else {
                    bail!("First input token is not Uint, {:?}", swap_tx.method);
                };
                let amount_in = logs
                    .iter()
                    .filter(|log| {
                        log.topics
                            .first()
                            .is_some_and(|hash| H256::from_str(DEPOSIT_HASH).unwrap().eq(hash))
                    })
                    .map(|x| U256::from_big_endian(&x.data))
                    .fold(U256::zero(), |acc, x| acc + x);
                (amount_in, amount_out)
            }
            SwapMethod::SwapExactETHForTokens
            | SwapMethod::SwapExactETHForTokensSupportingFeeOnTransferTokens => {
                let amount_in = swap_tx.tx.value;
                let mut amount_out = logs
                    .iter()
                    .filter(|log| {
                        log.topics
                            .first()
                            .is_some_and(|hash| H256::from_str(TRANSFER_HASH).unwrap().eq(hash))
                    })
                    .filter(|log| log.topics[2].eq(&wallet_address.into()))
                    .map(|log| U256::from_big_endian(&log.data))
                    .fold(U256::zero(), |acc, x| acc + x);
                if amount_out.is_zero() {
                    let amount_tokens = ethers::abi::decode(&LOG_PARAMS, &last_swap_log.data)?;
                    let amount_out_swap = amount_tokens[2..]
                        .into_iter()
                        .filter_map(|token| token.clone().into_uint())
                        .max()
                        .ok_or(anyhow!("No Uint in remaining tokens, {:?}", swap_tx.method))?;
                    amount_out = amount_out_swap;
                }
                (amount_in, amount_out)
            }
            SwapMethod::SwapExactTokensForETH | SwapMethod::SwapExactTokensForTokens => {
                let Token::Uint(amount_in) = swap_tx.input[0] else {
                    bail!("First input token is not Uint, {:?}", swap_tx.method);
                };
                let mut amount_out = logs
                    .iter()
                    .filter(|log| {
                        log.topics
                            .first()
                            .is_some_and(|hash| H256::from_str(TRANSFER_HASH).unwrap().eq(hash))
                    })
                    .filter(|log| log.topics[2].eq(&wallet_address.into()))
                    .map(|log| U256::from_big_endian(&log.data))
                    .fold(U256::zero(), |acc, x| acc + x);

                if amount_out.is_zero() {
                    let amount_tokens = ethers::abi::decode(&LOG_PARAMS, &last_swap_log.data)?;
                    let amount_out_swap = amount_tokens[2..]
                        .into_iter()
                        .filter_map(|token| token.clone().into_uint())
                        .max()
                        .ok_or(anyhow!("No Uint in remaining tokens, {:?}", swap_tx.method))?;
                    amount_out = amount_out_swap;
                }
                (amount_in, amount_out)
            }
            SwapMethod::SwapExactTokensForETHSupportingFeeOnTransferTokens
            | SwapMethod::SwapExactTokensForTokensSupportingFeeOnTransferTokens => {
                let amount_in = logs
                    .iter()
                    .filter(|log| {
                        log.topics
                            .first()
                            .is_some_and(|hash| H256::from_str(TRANSFER_HASH).unwrap().eq(hash))
                    })
                    .filter(|x| x.topics[1].eq(&wallet_address.into()))
                    .map(|x| U256::from_big_endian(&x.data))
                    .fold(U256::zero(), |acc, x| acc + x);

                let mut amount_out = logs
                    .iter()
                    .filter(|log| {
                        log.topics
                            .first()
                            .is_some_and(|hash| H256::from_str(TRANSFER_HASH).unwrap().eq(hash))
                    })
                    .filter(|log| log.topics[2].eq(&wallet_address.into()))
                    .map(|log| U256::from_big_endian(&log.data))
                    .fold(U256::zero(), |acc, x| acc + x);

                if amount_out.is_zero() {
                    let amount_tokens = ethers::abi::decode(&LOG_PARAMS, &last_swap_log.data)?;
                    let amount_out_swap = amount_tokens[2..]
                        .into_iter()
                        .filter_map(|token| token.clone().into_uint())
                        .max()
                        .ok_or(anyhow!("No Uint in remaining tokens, {:?}", swap_tx.method))?;

                    amount_out = amount_out_swap;
                }
                (amount_in, amount_out)
            }
            SwapMethod::SwapTokensForExactETH | SwapMethod::SwapTokensForExactTokens => {
                let amount_in = logs
                    .iter()
                    .filter(|log| {
                        log.topics
                            .first()
                            .is_some_and(|hash| H256::from_str(TRANSFER_HASH).unwrap().eq(hash))
                    })
                    .filter(|x| x.topics[1].eq(&wallet_address.into()))
                    .map(|x| U256::from_big_endian(&x.data))
                    .fold(U256::zero(), |acc, x| acc + x);
                let mut amount_out = logs
                    .iter()
                    .filter(|log| {
                        log.topics
                            .first()
                            .is_some_and(|hash| H256::from_str(TRANSFER_HASH).unwrap().eq(hash))
                    })
                    .filter(|log| log.topics[2].eq(&wallet_address.into()))
                    .map(|log| U256::from_big_endian(&log.data))
                    .fold(U256::zero(), |acc, x| acc + x);

                if amount_out.is_zero() {
                    let Token::Uint(amount_out_swap) = swap_tx.input[0] else {
                        bail!("First input token is not Uint, {:?}", swap_tx.method);
                    };
                    amount_out = amount_out_swap;
                }

                (amount_in, amount_out)
            }
        };

        let price;

        let amount_in = (amount_in.min(U256::from(U128::MAX)).as_u128() as f64)
            / 10_f64.powi(decimals_in as i32);
        let amount_out = (amount_out.min(U256::from(U128::MAX)).as_u128() as f64)
            / 10_f64.powi(decimals_out as i32);

        let token_amount;
        let prim_amount;
        match swap_tx.action {
            SwapAction::Buy => {
                price = amount_in / amount_out;
                prim_amount = amount_in;
                token_amount = amount_out;
            }
            SwapAction::Sell => {
                price = amount_out / amount_in;
                prim_amount = amount_out;
                token_amount = amount_in;
            }
            _ => {
                bail!("Method is not swap");
            }
        }

        if wallet_address == H160::from_str(REAPEAT_ADDRESS)? {
            let self_tx = SelfTx {
                tx: swap_tx.tx.hash,
                key_hash: None,
                action: Some(swap_tx.action.clone()),
                amount: Some(token_amount),
            };
            self.add_self_tx(Some(self_tx)).await?;
        }

        let conn = &mut db_pool.get()?;

        let trader_address = wallet_address.as_bytes().to_vec();
        let prim = prim_address.as_bytes().to_vec();
        let token = token_address.as_bytes().to_vec();
        let usd_amount = prim_amount * usd_per_prim;

        let ts = if let Some(block) = receipt.block_number {
            let res = swap_tx.context.provider.get_block(block).await?;
            res.map(|block| block.timestamp.as_u64() as i64)
                .unwrap_or(now())
        } else {
            now()
        };

        let tx_entry = NewTxEntry {
            trader: encode_hex(&trader_address),
            prim: encode_hex(&prim),
            token: encode_hex(&token),
            key_hash: key_hash.clone(),
            tx_hash: encode_hex(swap_tx.tx.hash.as_bytes()),
            price,
            token_amount,
            prim_amount,
            swap_action: swap_tx.action.to_string(),
            ts,
            price_usd: price * usd_per_prim,
            usd_amount,
        };

        conn.transaction(|conn| {
            diesel::insert_into(schema::transactions::table)
                .values(tx_entry)
                .execute(conn)
        })?;

        let trader: Option<TraderEntry> = conn.transaction(|conn| {
            diesel::QueryDsl::filter(
                schema::traders::table,
                schema::traders::key_hash.eq(key_hash.clone()),
            )
            .first::<TraderEntry>(conn)
            .optional()
        })?;

        match trader {
            Some(trader) => {
                let buy_count: i32;
                let sell_count: i32;
                let new_deposit: f64;
                let new_token_amount: f64;
                let min_price: f64 = trader.min_price.min(price);
                let max_price: f64 = trader.max_price.max(price);
                let sum_buy_usd_amount: f64;
                let sum_buy_token_amount: f64;
                let sum_sell_usd_amount: f64;
                let sum_sell_token_amount: f64;
                let max_single_buy: f64;
                let min_single_buy: f64;
                let max_single_sell: f64;
                let min_single_sell: f64;
                let max_buy_seq: f64;
                let cur_buy_seq: f64;
                let max_sell_seq: f64;
                let cur_sell_seq: f64;
                let ts = ts;
                match swap_tx.action {
                    SwapAction::Buy => {
                        new_deposit = trader.deposit - prim_amount;
                        new_token_amount = trader.token_amount + token_amount;
                        buy_count = trader.buy_count + 1;
                        sell_count = trader.sell_count;
                        sum_buy_usd_amount = trader.sum_buy_usd_amount + usd_amount;
                        sum_buy_token_amount = trader.sum_buy_token_amount + token_amount;
                        sum_sell_usd_amount = trader.sum_sell_usd_amount;
                        sum_sell_token_amount = trader.sum_sell_token_amount;
                        max_single_buy = trader.max_single_buy.max(prim_amount);
                        min_single_buy = if trader.min_single_buy == 0.0 {
                            prim_amount
                        } else {
                            trader.min_single_buy.min(prim_amount)
                        };
                        max_single_sell = trader.max_single_sell;
                        min_single_sell = trader.min_single_sell;
                        max_sell_seq = trader.max_sell_seq;
                        cur_buy_seq = trader.cur_buy_seq + prim_amount;
                        max_buy_seq = trader.max_buy_seq.max(cur_buy_seq);
                        cur_sell_seq = 0.0;
                    }
                    SwapAction::Sell => {
                        new_deposit = trader.deposit + prim_amount;
                        new_token_amount = (trader.token_amount - token_amount).max(0.0);
                        buy_count = trader.buy_count;
                        sell_count = trader.sell_count + 1;
                        sum_buy_usd_amount = trader.sum_buy_usd_amount;
                        sum_buy_token_amount = trader.sum_buy_token_amount;
                        sum_sell_usd_amount = trader.sum_sell_usd_amount + usd_amount;
                        sum_sell_token_amount = trader.sum_sell_token_amount + token_amount;
                        max_single_buy = trader.max_single_buy;
                        min_single_buy = trader.min_single_buy;
                        max_single_sell = trader.max_single_sell.max(prim_amount);
                        min_single_sell = if trader.min_single_sell == 0.0 {
                            prim_amount
                        } else {
                            trader.min_single_sell.min(prim_amount)
                        };
                        max_buy_seq = trader.max_buy_seq;
                        cur_sell_seq = trader.cur_sell_seq + prim_amount;
                        max_sell_seq = trader.max_sell_seq.max(cur_sell_seq);
                        cur_buy_seq = 0.0;
                    }
                    _ => bail!("Method is not swap"),
                }
                let wmean_buy = if (sum_buy_usd_amount / sum_buy_token_amount).is_finite() {
                    sum_buy_usd_amount / sum_buy_token_amount
                } else {
                    0.0
                };

                let wmean_sell = if (sum_sell_usd_amount / sum_sell_token_amount).is_finite() {
                    sum_sell_usd_amount / sum_sell_token_amount
                } else {
                    0.0
                };
                let wmean_ratio = if (wmean_sell / wmean_buy).is_finite() {
                    wmean_sell / wmean_buy
                } else {
                    0.0
                } as f32;

                let buy_sell = (buy_count as f32) / (sell_count as f32);

                let profit = ((buy_count + sell_count) as f32) / 2.0 * wmean_ratio;

                if let Err(e) = conn.transaction(|conn| {
                    diesel::update(schema::traders::table.find(trader.id))
                        .set((
                            schema::traders::buy_count.eq(buy_count),
                            schema::traders::sell_count.eq(sell_count),
                            schema::traders::deposit.eq(new_deposit),
                            schema::traders::token_amount.eq(new_token_amount),
                            schema::traders::min_price.eq(min_price),
                            schema::traders::max_price.eq(max_price),
                            schema::traders::sum_buy_usd_amount.eq(sum_buy_usd_amount),
                            schema::traders::sum_buy_token_amount.eq(sum_buy_token_amount),
                            schema::traders::wmean_buy.eq(wmean_buy),
                            schema::traders::sum_sell_usd_amount.eq(sum_sell_usd_amount),
                            schema::traders::sum_sell_token_amount.eq(sum_sell_token_amount),
                            schema::traders::wmean_sell.eq(wmean_sell),
                            schema::traders::wmean_ratio.eq(wmean_ratio),
                            schema::traders::max_single_buy.eq(max_single_buy),
                            schema::traders::min_single_buy.eq(min_single_buy),
                            schema::traders::max_single_sell.eq(max_single_sell),
                            schema::traders::min_single_sell.eq(min_single_sell),
                            schema::traders::max_buy_seq.eq(max_buy_seq),
                            schema::traders::cur_buy_seq.eq(cur_buy_seq),
                            schema::traders::max_sell_seq.eq(max_sell_seq),
                            schema::traders::cur_sell_seq.eq(cur_sell_seq),
                            schema::traders::profit.eq(profit),
                            schema::traders::buy_sell.eq(buy_sell),
                            schema::traders::ts.eq(ts),
                        ))
                        .execute(conn)
                }) {
                    error!("Error updating trader: {e}");
                }
            }
            None => {
                let buy_count;
                let sell_count;
                let new_deposit;
                let new_token_amount;
                let wmean_buy;
                let wmean_sell;
                //let wmean_ratio: Option<f64>;
                let min_price = price;
                let max_price = price;
                let sum_buy_usd_amount: f64;
                let sum_buy_token_amount: f64;
                let sum_sell_usd_amount: f64;
                let sum_sell_token_amount: f64;
                let max_single_buy: f64;
                let min_single_buy: f64;
                let max_single_sell: f64;
                let min_single_sell: f64;
                let max_buy_seq: f64;
                let cur_buy_seq: f64;
                let max_sell_seq: f64;
                let cur_sell_seq: f64;
                let profit = 0.0;
                let buy_sell = 0.0;
                let ts = ts;
                match swap_tx.action {
                    SwapAction::Buy => {
                        new_deposit = -usd_amount;
                        new_token_amount = token_amount;
                        sum_buy_usd_amount = usd_amount;
                        sum_buy_token_amount = token_amount;
                        max_single_buy = usd_amount;
                        min_single_buy = usd_amount;
                        sum_sell_usd_amount = 0.0;
                        sum_sell_token_amount = 0.0;
                        max_single_sell = 0.0;
                        min_single_sell = 0.0;
                        max_buy_seq = usd_amount;
                        cur_buy_seq = usd_amount;
                        max_sell_seq = 0.0;
                        cur_sell_seq = 0.0;
                        wmean_buy = if (usd_amount / token_amount).is_finite() {
                            usd_amount / token_amount
                        } else {
                            0.0
                        };
                        wmean_sell = 0.0;
                        buy_count = 1;
                        sell_count = 0;
                    }
                    SwapAction::Sell => {
                        new_deposit = usd_amount;
                        new_token_amount = 0.0;
                        sum_sell_usd_amount = usd_amount;
                        sum_buy_usd_amount = 0.0;
                        sum_sell_token_amount = token_amount;
                        sum_buy_token_amount = 0.0;
                        max_single_buy = 0.0;
                        min_single_buy = 0.0;
                        max_single_sell = usd_amount;
                        min_single_sell = usd_amount;
                        max_buy_seq = 0.0;
                        cur_buy_seq = 0.0;
                        max_sell_seq = usd_amount;
                        cur_sell_seq = usd_amount;
                        wmean_sell = if (usd_amount / token_amount).is_finite() {
                            usd_amount / token_amount
                        } else {
                            0.0
                        };
                        wmean_buy = 0.0;
                        buy_count = 0;
                        sell_count = 1;
                    }
                    _ => bail!("Method is not swap"),
                }
                let wmean_ratio = 0.0;

                let trader_entry = NewTraderEntry {
                    trader: encode_hex(&trader_address),
                    token: encode_hex(&token),
                    key_hash,
                    deposit: new_deposit,
                    token_amount: new_token_amount,
                    buy_count,
                    sell_count,
                    min_price,
                    max_price,
                    sum_buy_usd_amount,
                    sum_buy_token_amount,
                    wmean_buy,
                    sum_sell_usd_amount,
                    sum_sell_token_amount,
                    wmean_sell,
                    wmean_ratio,
                    max_single_buy,
                    min_single_buy,
                    max_single_sell,
                    min_single_sell,
                    max_buy_seq,
                    cur_buy_seq,
                    max_sell_seq,
                    cur_sell_seq,
                    active: false,
                    profit,
                    buy_sell,
                    ts,
                };

                conn.transaction(|conn| {
                    diesel::insert_into(schema::traders::table)
                        .values(trader_entry)
                        .execute(conn)
                })?;
            }
        }
        Ok(())
    }

    pub async fn clean_inactive_traders(
        &self,
        db_pool: Pool<ConnectionManager<PgConnection>>,
    ) -> Result<()> {
        let Some(ref context) = self.context else {
            return Ok(());
        };
        let mut conn = db_pool.get()?;
        let repeat_traders =
            conn.transaction(|conn| repeat_traders::table.load::<RepeatTraderEntry>(conn))?;
        for repeat_traders_entry in repeat_traders {
            let keyhash = repeat_traders_entry.keyhash;
            let trader_entry: TraderEntry = conn.transaction(|conn| {
                traders::table
                    .filter(traders::key_hash.eq(keyhash.clone()))
                    //.select((traders::id, traders::ts))
                    .first::<TraderEntry>(conn)
            })?;

            let settings_bucket = self.settings.lock().await;
            let settings = settings_bucket.get();
            let inactive_days_sell = settings.inactive_days_sell as i64;

            if now().saturating_sub(trader_entry.ts) > 60 * 60 * 24 * inactive_days_sell {
                conn.transaction(|conn| {
                    diesel::update(schema::traders::table.find(trader_entry.id))
                        .set(schema::traders::active.eq(false))
                        .execute(conn)?;
                    diesel::delete(
                        schema::repeat_traders::table
                            .filter(schema::repeat_traders::keyhash.eq(keyhash)),
                    )
                    .execute(conn)
                })?;

                let repeat_address = H160::from_str(REAPEAT_ADDRESS).unwrap();
                let provider = context.provider.clone();
                let token_address = H160::from_str(&trader_entry.token).unwrap();
                let token_contract = Erc20Token::new(token_address, provider.clone());

                let token_decimals = token_contract.decimals().call().await? as i32;

                let keyhash_token_balance = U256::from(
                    (repeat_traders_entry.token_amount * 10.0f64.powi(token_decimals as i32))
                        as u128,
                );
                let token_balance = token_contract.balance_of(repeat_address).await?;

                info!(
                    "Selling tokens of inactive trader {:?}",
                    trader_entry.trader
                );

                let amount_in = keyhash_token_balance.min(token_balance);
                trader_entry
                    .sell_traders_token(amount_in, context.clone())
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn clean_db(&self, db_pool: Pool<ConnectionManager<PgConnection>>) -> Result<()> {
        let mut conn = db_pool.get()?;
        let traders_count: usize = conn.transaction(|conn| {
            diesel::delete(traders::table.filter(traders::ts.lt(now() - 60 * 60 * 24 * 30)))
                .execute(conn)
        })?;

        let tx_count: usize = conn.transaction(|conn| {
            diesel::delete(
                transactions::table.filter(transactions::ts.lt(now() - 60 * 60 * 24 * 30)),
            )
            .execute(conn)
        })?;

        info!(%traders_count, %tx_count, "Database cleaned.");
        Ok(())
    }

    pub async fn check_repeat_traders_availability(
        &self,
        //db_pool: Pool<ConnectionManager<PgConnection>>,
    ) -> Result<()> {
        let mut db_conn = DbPoolConnection::new(&self.db_pool)?;

        let mut repeat = db_conn.count_repeat_traders()?;
        let settings = self.settings.lock().await;
        let settings = settings.get();

        if repeat >= settings.max_repeat_traders as i64 {
            let Some(context) = self.context.clone() else {
                return Ok(());
            };

            /* message = format!(
                "{message}{symbol}: {}$ \n",
                (token_usd_balance / U256::pow(U256::from(10), U256::from(18))).as_u64()
            ); */

            let active_traders = db_conn.get_active_traders()?;
            let repeat_traders = db_conn.get_repeat_traders()?;
            let mut repeat_traders_with_usd = Vec::new();
            for repeat in &repeat_traders {
                let Some(trader) = db_conn.get_trader_entry_by_keyhash(&repeat.keyhash)? else {
                    continue;
                };

                let Some(token) = db_conn.get_token_by_trader(&trader)? else {
                    continue;
                };
                /* let x = db_conn.get_token_by_trader(&trader)?;
                let Some(token) = db_conn.get_token_by_trader(&trader)? else {
                    continue;
                }; */
                let token_usd_balance = context
                    .get_token_usd_balance(H160::from_str(&token.token).unwrap())
                    .await
                    .unwrap_or_default();
                let token_usd_balance =
                    (token_usd_balance / U256::pow(U256::from(10), U256::from(18))).as_u64();

                repeat_traders_with_usd.push((repeat.clone(), token_usd_balance))
            }

            let null_repeat_traders = repeat_traders_with_usd
                .into_iter()
                .filter(|repeat| repeat.1 == 0)
                .map(|repeat| repeat.0.keyhash.clone())
                .collect::<Vec<_>>();
            let mut traders = active_traders
                .into_iter()
                .filter(|trader| null_repeat_traders.contains(&trader.key_hash))
                .collect::<Vec<_>>();
            traders.sort_by(|a, b| a.ts.cmp(&b.ts));
            let Some(first_trader) = traders.first() else {
                return Ok(());
            };
            if now().saturating_sub(first_trader.ts)
                < 60 * 60 * settings.zero_traders_replacement as i64
            {
                return Ok(());
            }
            db_conn.remove_unacceptable_repeat_trader(&first_trader)?;
        }

        let mut repeat = db_conn.count_repeat_traders()?;

        let repeat_tokens = db_conn.get_repeat_tokens()?;

        let mut best_traders =
            db_conn.get_best_trader(repeat_tokens, settings.allow_similar_tokens)?;
        best_traders.sort_by(|a, b| b.profit.total_cmp(&a.profit));
        for best_trader in &best_traders {
            if repeat >= settings.max_repeat_traders as i64 {
                return Ok(());
            }
            let token = db_conn.get_token_by_trader(best_trader)?;
            if let Some(token) = token {
                if now() - token.creation_ts > 21 * 24 * 3600 {
                    db_conn.add_repeat_trader(&best_trader.key_hash, best_trader.id)?;
                    repeat += 1;
                }
            }
        }

        Ok(())
    }

    pub async fn add_self_tx(&mut self, self_tx: Option<SelfTx>) -> Result<()> {
        let Some(self_tx) = self_tx else {
            return Ok(());
        };
        match self.self_tx_pool.entry(self_tx.tx) {
            Entry::Occupied(entry) => {
                let partial_self_tx = entry.remove();
                let completed_self_tx = partial_self_tx.complete(self_tx);
                if completed_self_tx.is_full() {
                    let mut conn = self.db_pool.get()?;
                    conn.transaction(|conn| {
                        let mut repeat_trader: RepeatTraderEntry = repeat_traders::table
                            .filter(repeat_traders::keyhash.eq(completed_self_tx.key_hash.unwrap()))
                            .first::<RepeatTraderEntry>(conn)?;

                        if completed_self_tx.action.unwrap().is_buy() {
                            repeat_trader.token_amount += completed_self_tx.amount.unwrap();
                        } else {
                            repeat_trader.token_amount -= completed_self_tx.amount.unwrap();
                        }

                        diesel::update(repeat_traders::table.find(repeat_trader.id))
                            .set(repeat_trader)
                            .execute(conn)
                    })?;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(self_tx);
            }
        }
        Ok(())
    }

    pub async fn handle_total_usd_balance_request(
        &self,
        req: Option<RequsetTotalUsdBalance>,
    ) -> Result<()> {
        let Some(req) = req else {
            bail!("No request");
        };

        let Some(context) = self.context.clone() else {
            bail!("No request");
        };

        let mut conn = self.db_pool.get()?;
        let mut total_usd = U256::zero();
        let mut message = String::new();
        let repeat_traders =
            conn.transaction(|conn| repeat_traders::table.load::<RepeatTraderEntry>(conn))?;

        let mut tokens = Vec::new();

        for repeat_trader in repeat_traders {
            let token: String = conn.transaction(|conn| {
                traders::table
                    .filter(traders::key_hash.eq(repeat_trader.keyhash))
                    .select(traders::token)
                    .first::<String>(conn)
            })?;
            tokens.push(token);
        }

        tokens.sort();
        tokens.dedup();

        for token in tokens {
            let symbol: String = conn
                .transaction(|conn| {
                    tokens::table
                        .filter(tokens::token.eq(token.clone()))
                        .select(tokens::symbol)
                        .first::<Option<String>>(conn)
                })?
                .unwrap_or(format!("0x{token}"));
            let token_address = H160::from_str(&token).unwrap();
            let token_usd_balance = context
                .get_token_usd_balance(token_address)
                .await
                .unwrap_or_default();
            message = format!(
                "{message}{symbol}: {}$ \n",
                (token_usd_balance / U256::pow(U256::from(10), U256::from(18))).as_u64()
            );
            total_usd += token_usd_balance;
        }

        let wbnb_usd_balance = context.get_token_usd_balance(*WBNB_ADDRESS).await?;
        message = format!(
            "{message}\"WBNB\": {}$ \n",
            (wbnb_usd_balance / U256::pow(U256::from(10), U256::from(18))).as_u64()
        );

        total_usd += wbnb_usd_balance;

        let usdt_balance = context.usdt_balance().await?;
        message = format!(
            "{message}\"USDT\": {}$ \n",
            (usdt_balance / U256::pow(U256::from(10), U256::from(18))).as_u64()
        );
        total_usd += usdt_balance;

        let eth_usd_balance = context.eth_usd_balance().await?;
        message = format!(
            "{message}\"BNB\": {}$ \n",
            (eth_usd_balance / U256::pow(U256::from(10), U256::from(18))).as_u64()
        );

        total_usd += eth_usd_balance;

        let usd_balance = (total_usd / U256::pow(U256::from(10), U256::from(18))).as_u64();
        message = format!("{message}\"Total USD balance\": {usd_balance}$ \n");

        req.bot.send_message(req.chat_id, message).await?;

        Ok(())
    }

    pub async fn run(
        &mut self,
        mut rx_tx: Receiver<SwapTx>,
        db_pool: Pool<ConnectionManager<PgConnection>>,
    ) -> Result<()> {
        let mut repeat_traders_clear_interval =
            tokio::time::interval(std::time::Duration::from_secs(60 * 60));

        let mut db_clear_interval =
            tokio::time::interval(std::time::Duration::from_secs(60 * 60 * 24));

        let mut repeat_traders_add_interval =
            tokio::time::interval(std::time::Duration::from_secs(60));

        loop {
            select! {
                m = rx_tx.recv() => {
                    let receipt_tx = get_transaction_confirmation(m).await;
                    if let Err(e) = self.handle_approved_transaction(receipt_tx, db_pool.clone()).await {
                        error!("Error handling approved tx: {e}");
                    }
                },
                _ = repeat_traders_clear_interval.tick() => if let Err(e) = self.clean_inactive_traders(db_pool.clone()).await {
                    let _ = notify_bot(self.bot.clone(), db_pool.clone(), &e).await;
                    error!("Error cleaning repeat traders: {e}");
                },
                m = self.rx_total_usd_balance.recv() => {
                    if let Err(e) = self.handle_total_usd_balance_request(m).await {
                        error!("Error handling request total usd balance: {e}");
                    }
                },
                m = self.rx_repeated_tx.recv() => {
                    if let Err(e) = self.add_self_tx(m).await {
                        error!("Error adding self_tx from repeater: {e}");
                    }
                },
                _ = db_clear_interval.tick() => if let Err(e) = self.clean_db(db_pool.clone()).await {
                    error!("Error cleaning databases: {e}");
                },
                _ = repeat_traders_add_interval.tick() => {
                    if let Err(e) = self.check_repeat_traders_availability().await {
                        error!("Error adding new reapeat trader: {e}");

                    }
                },
            }
        }
    }
}

pub async fn notify_bot<D: std::fmt::Display>(
    bot: Bot,
    db_pool: Pool<ConnectionManager<PgConnection>>,
    msg: D,
) -> Result<()> {
    let mut conn = db_pool.get()?;
    let chat_ids = conn.transaction(|conn| schema::tg_chat_ids::table.load::<ChatIdEntry>(conn))?;
    for chat_id_entry in chat_ids.iter() {
        let chat_id = chat_id_entry.chat_id;
        bot.send_message(ChatId(chat_id), format!("{msg}")).await?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct SellAllCase {
    pub compute_amount: bool,
    pub address: H160,
}

impl SellAllCase {
    pub fn all() -> [SellAllCase; 4] {
        [
            Self::new(false, *WBNB_ADDRESS),
            Self::new(true, *WBNB_ADDRESS),
            Self::new(false, *BP_BSC_USD_ADDRESS),
            Self::new(true, *BP_BSC_USD_ADDRESS),
        ]
    }

    fn new(compute_amount: bool, address: H160) -> Self {
        Self {
            compute_amount,
            address,
        }
    }
}
