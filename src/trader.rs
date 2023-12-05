use crate::common::*;
use crate::schema::repeat_traders;
use crate::schema::traders;
use anyhow::{Result, anyhow, bail};
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::Pool;
use diesel::PgConnection;
use ethers::abi::Token;
use tokio::{select, sync::broadcast::Receiver};
use tracing::error;

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
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = schema::tokens)]
pub struct TokenEntry {
    pub id: i32,
    pub token: String,
    pub creation_ts: i64,
    pub tradable: Option<bool>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = schema::repeat_traders)]
pub struct NewRepeatTraderEntry {
    pub keyhash: String
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = schema::repeat_traders)]
pub struct RepeatTraderEntry {
    pub id: i32,
    pub keyhash: String
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
    pub ts: i64,
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
    pub ts: i64,
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

pub async fn handle_approved_transaction(
    swap_tx: Option<SwapTx>,
    db_pool: Pool<ConnectionManager<PgConnection>>,
) -> Result<()> {
    let Some(swap_tx) = swap_tx else {
        return Ok(());
    };

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

    let amount_in = (amount_in.min(U256::from(U128::MAX)).as_u128() as f64) / 10_f64.powi(decimals_in as i32);
    let amount_out = (amount_out.as_u128() as f64) / 10_f64.powi(decimals_out as i32);

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

    let conn = &mut db_pool.get()?;

    let trader_address = wallet_address.as_bytes().to_vec();
    let prim = prim_address.as_bytes().to_vec();
    let token = token_address.as_bytes().to_vec();
    let usd_amount = prim_amount * usd_per_prim;

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
        ts: now(),
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
            let ts = now();
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
            let ts = now();
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

pub async fn clean_inactive_traders(db_pool: Pool<ConnectionManager<PgConnection>>) -> Result<()> {
    let mut conn = db_pool.get()?;
    let repeat_traders = conn.transaction(|conn| {
        repeat_traders::table.load::<(i32, String)>(conn)
    })?;
    for (_, keyhash) in repeat_traders {
        let (trader_id, last_update): (i64, i64) = conn.transaction(|conn| {
            traders::table.filter(traders::key_hash.eq(keyhash.clone())).select((traders::id, traders::ts)).first(conn)
        })?;
        if now().saturating_sub(last_update)  > 60 * 60 * 24 * 2 {
            conn.transaction(|conn| {
                diesel::update(schema::traders::table.find(trader_id))
                    .set(schema::traders::active.eq(false))
                    .execute(conn)?;
                diesel::delete(
                    schema::repeat_traders::table
                        .filter(schema::repeat_traders::keyhash.eq(keyhash)),
                )
                .execute(conn)
            })?;
        }
    }
    Ok(())

}

pub async fn run(
    mut rx_tx: Receiver<SwapTx>,
    db_pool: Pool<ConnectionManager<PgConnection>>,
) -> Result<()> {
    let mut repeat_traders_clear_interval = tokio::time::interval(std::time::Duration::from_secs(60 * 60));
    loop {
        select! {
            m = rx_tx.recv() => {
                let receipt_tx = get_transaction_confirmation(m).await;
                if let Err(e) = handle_approved_transaction(receipt_tx, db_pool.clone()).await {
                    error!("Error handling approved tx: {e}");
                }
            },
            _ = repeat_traders_clear_interval.tick() => if let Err(e) = clean_inactive_traders(db_pool.clone()).await {
                error!("Error cleaning repeat traders: {e}");
            }
        }
    }
}
