use diesel::{prelude::*, r2d2::{ConnectionManager, Pool, PooledConnection}, Connection, PgConnection};
use anyhow::Result;

use crate::{common::now, schema, trader::{NewRepeatTraderEntry, NewTokenEntry, RepeatTraderEntry, TokenEntry, TraderEntry}};
pub type PgConnectionManager = ConnectionManager<PgConnection>;
pub type DbPool = Pool<PgConnectionManager>;
pub type DbConnection = PooledConnection<PgConnectionManager>;

pub struct DbPoolConnection {
    conn: DbConnection,//&'a mut DbConnection,
    //db_pool: DbPool,
}

impl DbPoolConnection {
    pub fn new(db_pool: &DbPool) -> Result<Self> {
        let conn = db_pool.get()?;
        Ok(Self {
            //db_pool: db_pool.clone(),
            conn,
            //db_pool: db_pool.clone(),
        })
    }

    /// Get optional trader entry structure specified ny key hash
    pub fn get_trader_entry_by_keyhash(&mut self, key_hash: &String) -> Result<Option<TraderEntry>> {
        //let conn = &mut self.db_pool.get()?;
        Ok(self.conn.transaction(|conn| {
            schema::traders::table
                .filter(schema::traders::key_hash.eq(key_hash))
                .first::<TraderEntry>(conn)
                .optional()
        })?)
    }

    /// Get token amount of specidfied key hash
    pub fn get_token_amount_by_keyhash(&mut self, key_hash: &String) -> Result<f64> {
        //let conn = &mut self.db_pool.get()?;
        Ok(self.conn.transaction(|conn| {
            schema::repeat_traders::table
                .filter(schema::repeat_traders::keyhash.eq(key_hash))
                .select(schema::repeat_traders::token_amount)
                .first::<f64>(conn)
        })?)
    }


    /* trader.buy_count > 5
            && trader.sell_count > 5
            && trader.wmean_ratio > 1.6
            && trader.wmean_ratio < 10.0
            && trader.sum_buy_usd_amount / trader.sum_sell_usd_amount < 0.6 */
    pub fn get_best_trader(&mut self, tokens: Vec<String>, allow_similar_tokens: bool) -> Result<Vec<TraderEntry>> {
        let tokens = if allow_similar_tokens {
            &[]
        } else {
            tokens.as_slice()
        };

        let date = now() - 60 * 60 * 24;
        Ok(self.conn.transaction(|conn| {
            schema::traders::table
                .filter(schema::traders::buy_count.gt(5))
                .filter(schema::traders::sell_count.gt(5))
                .filter(schema::traders::wmean_ratio.gt(1.6))
                .filter(schema::traders::wmean_ratio.lt(10.0))
                .filter(schema::traders::buy_sell.lt(0.6))
                .filter(schema::traders::token.ne_all(tokens))
                .filter(schema::traders::ts.gt(date))
                .order(schema::traders::profit.desc())
                .get_results::<TraderEntry>(conn)
        })?)
    }

    /// Add new trader entry to repeat traders and change it to active
    pub fn add_repeat_trader(&mut self, key_hash: &String, trader_id: i64) -> Result<usize> {
        //let conn = &mut self.db_pool.get()?;
        let new_repeat = NewRepeatTraderEntry {
            keyhash: key_hash.clone(),
            token_amount: 0.0,
        };
        self.conn.transaction(|conn| {
            diesel::insert_into(schema::repeat_traders::table)
                .values(new_repeat)
                .execute(conn)
        })?;
        Ok(self.conn.transaction(|conn| {
            diesel::update(schema::traders::table.find(trader_id))
                .set(schema::traders::active.eq(true))
                .execute(conn)
        })?)
    }

    /// Remove trader entry from repeat traders and change it to inactive
    pub fn remove_unacceptable_repeat_trader(&mut self, trader_entry: &TraderEntry) -> Result<usize> {
        //let conn = &mut self.db_pool.get()?;
        Ok(self.conn.transaction(|conn| {
            diesel::update(schema::traders::table.find(trader_entry.id))
                .set(schema::traders::active.eq(false))
                .execute(conn)?;
            diesel::delete(
                schema::repeat_traders::table
                    .filter(schema::repeat_traders::keyhash.eq(trader_entry.key_hash.clone())),
            )
            .execute(conn)
        })?)
    }

    /// Get all active traders
    pub fn get_active_traders(&mut self) -> Result<Vec<TraderEntry>> {
        //let conn = &mut self.db_pool.get()?;
        Ok(self.conn.transaction(|conn| {
            schema::traders::table
                .filter(schema::traders::active.eq(true))
                .get_results::<TraderEntry>(conn)
        })?)
    }

    /// Get all repeat traders
    pub fn get_repeat_traders(&mut self) -> Result<Vec<RepeatTraderEntry>> {
        //let conn = &mut self.db_pool.get()?;
        Ok(self.conn.transaction(|conn| {
            schema::repeat_traders::table
                .get_results::<RepeatTraderEntry>(conn)
        })?)
    }

    /// Count all repeating traders
    pub fn count_repeat_traders(&mut self) -> Result<i64> {
        //let conn = &mut self.db_pool.get()?;
        Ok(self.conn.transaction(|conn| schema::repeat_traders::table.count().get_result(conn))?)
    }

    /// Get trading token of specified trader
    pub fn get_token_by_trader(&mut self, trader_entry: &TraderEntry) -> Result<Option<TokenEntry>> {
        //let conn = &mut self.db_pool.get()?;
        Ok(self.conn.transaction(|conn| {
            schema::tokens::table
                .filter(schema::tokens::token.eq(trader_entry.token.clone()))
                .first::<TokenEntry>(conn)
                .optional()
        })?)
    }

    /// Get all repeating tokens
    pub fn get_repeat_tokens(&mut self) -> Result<Vec<String>> {
        //let conn = &mut self.db_pool.get()?;
        Ok(self.conn.transaction(|conn| {
            schema::traders::table
                .filter(schema::traders::active.eq(true))
                .select(schema::traders::token)
                .get_results::<String>(conn)
        })?)
    }

    /// Insert new token
    pub fn insert_new_token(&mut self, token: &String, creation_ts: i64, symbol: Option<String>) -> Result<TokenEntry> {
        //let conn = &mut self.db_pool.get()?;
        let new_token = NewTokenEntry {
            token: token.clone(),
            creation_ts,
            tradable: None,
            symbol,
        };

        Ok(self.conn.transaction(|conn| {
            diesel::insert_into(schema::tokens::table)
                .values(new_token)
                .get_result::<TokenEntry>(conn)
        })?)
    }
}
