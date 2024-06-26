use anyhow::{anyhow, bail, Result};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use dotenvy::dotenv;
use ethers::abi::{Function, Token};
use ethers::prelude::*;
use k256::ecdsa::SigningKey;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fmt::Write;
use std::{collections::HashMap, fs::File, io::BufReader, path::Path, sync::Arc};
use strum::IntoEnumIterator;
use strum::{AsRefStr, Display, EnumIter, EnumString, IntoStaticStr};
use thiserror::Error;
use tracing::{error, info, warn};

pub const JSON_RPC_VERSION: &str = "2.0";
pub const TX_POOL_CONTENT: &str = "txpool_content";
pub const PANCAKESWAP_ADDRESS: &str = "0x10ed43c718714eb63d5aa57b78b54704e256024e";
pub const SWAP_HASH: &str = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
pub const TRANSFER_HASH: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
pub const DEPOSIT_HASH: &str = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c";
pub const REAPEAT_ADDRESS: &str = "0x03a800E6B5bB61dc18e079c65dabfB19CA22A6f6";

pub const HTTP_URL: &str = "http://127.0.0.1:8545";
pub const WS_URL: &str = "ws://127.0.0.1:8546";

lazy_static! {
    pub static ref WBNB_ADDRESS: H160 =
        H160::from_str("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c").unwrap();
    pub static ref BP_BSC_USD_ADDRESS: H160 =
        H160::from_str("0x55d398326f99059ff775485246999027b3197955").unwrap();
    static ref PEG_BUSD_ADDRESS: H160 =
        H160::from_str("0xe9e7cea3dedca5984780bafc599bd69add087d56").unwrap();
}

use std::str::FromStr;

use crate::db::DbPoolConnection;
use crate::trader::TraderEntry;
use crate::{abi::*, TradingSettings};

#[derive(
    Serialize,
    Deserialize,
    EnumIter,
    IntoStaticStr,
    AsRefStr,
    EnumString,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Debug,
    Display,
)]
pub enum SwapMethod {
    #[strum(serialize = "swapETHForExactTokens")]
    SwapETHForExactTokens,
    #[strum(serialize = "swapExactETHForTokens")]
    SwapExactETHForTokens,
    #[strum(serialize = "swapExactETHForTokensSupportingFeeOnTransferTokens")]
    SwapExactETHForTokensSupportingFeeOnTransferTokens,
    #[strum(serialize = "swapExactTokensForETH")]
    SwapExactTokensForETH,
    #[strum(serialize = "swapExactTokensForETHSupportingFeeOnTransferTokens")]
    SwapExactTokensForETHSupportingFeeOnTransferTokens,
    #[strum(serialize = "swapExactTokensForTokens")]
    SwapExactTokensForTokens,
    #[strum(serialize = "swapExactTokensForTokensSupportingFeeOnTransferTokens")]
    SwapExactTokensForTokensSupportingFeeOnTransferTokens,
    #[strum(serialize = "swapTokensForExactETH")]
    SwapTokensForExactETH,
    #[strum(serialize = "swapTokensForExactTokens")]
    SwapTokensForExactTokens,
}

impl SwapMethod {
    pub fn define_action(
        eth: &[H160; 3],
        first_token_address: &H160,
        last_token_address: &H160,
    ) -> SwapAction {
        if eth.contains(first_token_address) && eth.contains(last_token_address) {
            return SwapAction::SwapEq;
        }
        if eth.contains(first_token_address) {
            return SwapAction::Buy;
        }
        if eth.contains(last_token_address) {
            return SwapAction::Sell;
        }
        SwapAction::Unknown
    }

    pub fn initialize_method_ids() -> HashMap<String, SwapMethod> {
        let mut map = HashMap::new();
        map.insert(
            "b6f9de95".to_string(),
            SwapMethod::SwapExactETHForTokensSupportingFeeOnTransferTokens,
        );
        map.insert(
            "5c11d795".to_string(),
            SwapMethod::SwapExactTokensForTokensSupportingFeeOnTransferTokens,
        );
        map.insert("38ed1739".to_string(), SwapMethod::SwapExactTokensForTokens);
        map.insert("7ff36ab5".to_string(), SwapMethod::SwapExactETHForTokens);
        map.insert("8803dbee".to_string(), SwapMethod::SwapTokensForExactTokens);
        map.insert(
            "791ac947".to_string(),
            SwapMethod::SwapExactTokensForETHSupportingFeeOnTransferTokens,
        );
        map.insert("18cbafe5".to_string(), SwapMethod::SwapExactTokensForETH);
        map.insert("fb3bdb41".to_string(), SwapMethod::SwapETHForExactTokens);
        map.insert("4a25d94a".to_string(), SwapMethod::SwapTokensForExactETH);
        map
    }
}

#[derive(Debug, Clone, EnumString, Display)]
pub enum SwapAction {
    Buy,
    Sell,
    Unknown,
    SwapEq,
}

impl SwapAction {
    pub fn is_buy(&self) -> bool {
        matches!(&self, SwapAction::Buy)
    }
}

#[derive(Debug, Clone)]
pub struct SwapAddresses {
    pub prim_address: H160,
    pub token_address: H160,
}

#[derive(Debug, Clone)]
pub struct SwapContracts {
    pub prim_contract: Erc20Token<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>,
    pub token_contract: Erc20Token<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>,
}

#[derive(Debug, Clone)]
pub struct SwapTx {
    pub method: SwapMethod,
    pub input: Vec<Token>,
    pub tx: Transaction,
    pub receipt: Option<TransactionReceipt>,
    pub from: H160,
    pub action: SwapAction,
    //pub addresses: [H160; 2],
    pub addresses: SwapAddresses,
    pub contracts: SwapContracts,
    pub context: Arc<SwapContext>,
    pub function: Function,
}

impl SwapTx {
    pub async fn get_reciept(&self) -> Option<TransactionReceipt> {
        let mut attempts = 0;
        while let Ok(receipt) = self
            .context
            .provider
            .get_transaction_receipt(self.tx.hash)
            .await
        {
            if attempts == 10 {
                return None;
            }
            let Some(receipt) = receipt else {
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                attempts += 1;
                continue;
            };
            if let Some(status) = receipt.status {
                let status = !status.is_zero();
                return status.then_some(receipt);
            }
        }
        None
    }

    pub fn tx(&self) -> &Transaction {
        &self.tx
    }

    pub fn provider(&self) -> &SignerMiddleware<Provider<Http>, Wallet<SigningKey>> {
        &self.context.provider
    }

    pub fn function(&self) -> &Function {
        &self.function
    }

    pub fn need_process(&self) -> bool {
        matches!(self.action, SwapAction::Buy | SwapAction::Sell)
    }

    pub fn decode_from(tx: Transaction, context: Arc<SwapContext>) -> Result<SwapTx, SwapError> {
        if tx.input.len() < 5 {
            return Err(SwapError::DecodeInputError(
                "Decode input is too short".to_string(),
            ));
        }
        let method_id = &tx.input[..4];
        let data = &tx.input[4..];
        let method = context
            .function_map
            .get(&encode_hex(method_id))
            .cloned()
            .ok_or(SwapError::NotSwapMethod)?;

        let function = context
            .functions
            .iter()
            .find(|f| *f.name == method.to_string())
            .cloned()
            .ok_or(SwapError::NotFoundInAbi(method.to_string()))?;

        let input = function.decode_input(data)?;

        let addresses = input
            .iter()
            .find_map(|token| token.clone().into_array())
            .ok_or(SwapError::TokenNotFound(input.clone()))?;

        let Token::Address(address_in) = addresses
            .first()
            .cloned()
            .ok_or(SwapError::AddressNotFound("first".to_string()))?
        else {
            return Err(SwapError::TokenNotFound(addresses));
        };
        let Token::Address(address_out) = addresses
            .last()
            .cloned()
            .ok_or(SwapError::AddressNotFound("last".to_string()))?
        else {
            return Err(SwapError::TokenNotFound(addresses));
        };

        let action = SwapMethod::define_action(&context.eth, &address_in, &address_out);
        //let from = tx.from;
        let from = input
            .iter()
            .find_map(|token| token.clone().into_address())
            .ok_or(SwapError::TokenNotFound(input.clone()))?;
        /* let output_tokens_to = input.iter().find_map(|token| token.clone().into_address()).ok_or(SwapError::TokenNotFound(input.clone()))?;

        if !from.eq(&output_tokens_to) {
            return Err(SwapError::SwapToDifferentAddress);
        } */

        let addresses = if action.is_buy() {
            SwapAddresses {
                prim_address: address_in,
                token_address: address_out,
            }
        } else {
            SwapAddresses {
                prim_address: address_out,
                token_address: address_in,
            }
        };

        //let contracts = [, ];

        let contracts = if action.is_buy() {
            SwapContracts {
                prim_contract: Erc20Token::new(address_in, context.provider.clone()),
                token_contract: Erc20Token::new(address_out, context.provider.clone()),
            }
        } else {
            SwapContracts {
                token_contract: Erc20Token::new(address_in, context.provider.clone()),
                prim_contract: Erc20Token::new(address_out, context.provider.clone()),
            }
        };

        Ok(SwapTx {
            method,
            input,
            tx,
            receipt: None,
            from,
            action,
            addresses,
            contracts,
            context,
            function,
        })
    }

    pub fn get_keyhash(&self) -> String {
        let mut hasher: Sha256 = Digest::new();

        let wallet_address = self.from.clone();
        let token_address = self.token_address();

        Digest::update(&mut hasher, &wallet_address);
        Digest::update(&mut hasher, token_address);

        let key_hash = Digest::finalize(hasher).to_vec();

        encode_hex(&key_hash)
    }

    pub async fn need_repeat(
        &self,
        db_pool: Pool<ConnectionManager<PgConnection>>,
        settings: &TradingSettings,
    ) -> Result<Option<TraderEntry>> {
        let mut db_conn = DbPoolConnection::new(&db_pool)?;

        if self.from == H160::from_str(REAPEAT_ADDRESS)? {
            return Ok(None);
        }

        let key_hash = self.get_keyhash();
        let trader = db_conn.get_trader_entry_by_keyhash(&key_hash)?;

        let Some(trader) = trader else {
            return Ok(None);
        };

        if trader.active {
            if trader.wmean_ratio < 1.2
                && trader.sum_buy_usd_amount / trader.sum_sell_usd_amount > 1.1
            {
                let token_decimals = self.token_decimals().await?;
                let keyhash_token_balance = db_conn.get_token_amount_by_keyhash(&key_hash)?;

                let keyhash_token_balance = U256::from(
                    (keyhash_token_balance * 10.0f64.powi(token_decimals as i32)) as u128,
                );

                let tokens_left = self.token_balance(REAPEAT_ADDRESS).await?;

                info!("Selling tokens of {:?}", trader.trader);

                db_conn.remove_unacceptable_repeat_trader(&trader)?;
                let amount_in = keyhash_token_balance.min(tokens_left);
                let receipt = trader
                    .sell_traders_token(amount_in, self.context.clone())
                    .await?;

                let tx = self
                    .context
                    .provider
                    .get_transaction(receipt.transaction_hash)
                    .await?;
                let buy_sell_ratio = trader.sum_buy_usd_amount / trader.sum_sell_usd_amount;
                info!(wmean_ratio=%trader.wmean_ratio, %buy_sell_ratio, "Trader's parameters are not met required anymore");
                info!("{tx:?}");

                return Ok(None);
            }
            return Ok(Some(trader));
        }

        if trader.buy_count > 5
            && trader.sell_count > 5
            && trader.wmean_ratio > 1.6
            && trader.wmean_ratio < 10.0
            && trader.sum_buy_usd_amount / trader.sum_sell_usd_amount < 0.6
        {
            let token = db_conn.get_token_by_trader(&trader)?;

            if token.is_none() {
                let creation_ts = self.get_creation_ts().await?;
                let symbol = self.symbol().await;
                db_conn.insert_new_token(&trader.token, creation_ts, symbol.clone())?;
            }

            /* let token = match token {
                Some(token) => token,
                None => {
                    let creation_ts = self.get_creation_ts().await?;
                    let symbol = self.symbol().await;
                    db_conn.insert_new_token(&trader.token, creation_ts, symbol.clone())?
                }
            }; */

            /* if now() - token.creation_ts < 21 * 24 * 3600 {
                return Ok(None);
            }

            if !settings.allow_similar_tokens {
                let tokens = db_conn.get_repeat_tokens()?;
                if tokens.contains(&token.token) {
                    return Ok(None);
                }
            }

            let repeat = db_conn.count_repeat_traders()?;

            if repeat >= settings.max_repeat_traders as i64 {
                let active_traders = db_conn.get_active_traders()?;
                let repeat_traders = db_conn.get_repeat_traders()?;
                let null_repeat_traders = repeat_traders
                    .into_iter()
                    .filter(|repeat| repeat.token_amount == 0.0)
                    .map(|repeat| repeat.keyhash)
                    .collect::<Vec<_>>();
                let mut traders = active_traders
                    .into_iter()
                    .filter(|trader| null_repeat_traders.contains(&trader.key_hash))
                    .collect::<Vec<_>>();
                traders.sort_by(|a, b| a.ts.cmp(&b.ts));
                let Some(first_trader) = traders.first() else {
                    return Ok(None);
                };
                if now().saturating_sub(first_trader.ts)
                    < 60 * 60 * settings.zero_traders_replacement as i64
                {
                    return Ok(None);
                }
                db_conn.remove_unacceptable_repeat_trader(&first_trader)?;
            }

            db_conn.add_repeat_trader(&key_hash, trader.id)?;

            let convertable_min_token = self.min_token_sell(5.0).await;

            return match convertable_min_token {
                Ok(convertable_min_token) if !convertable_min_token.is_zero() => Ok(Some(trader)),
                _ => Ok(None),
            }; */
        }
        Ok(None)
    }

    pub async fn get_creation_ts(&self) -> Result<i64> {
        let client = reqwest::Client::new();
        let token_address = format!("{:?}", self.token_address());
        info!("token address: {token_address}");
        let res = client
            .get(format!(
                "https://api.bscscan.com/api?module=contract&action=getcontractcreation&contractaddresses={}&apikey={}",
                token_address, self.context.bscscan_api,
            ))
            .send()
            .await?;
        let text = res.text().await?;
        let res = serde_json::from_str::<BscScanResponse>(&text)?;

        let contract_creation = match res.result {
            BscScanResult::GetContractCreation(s) => s,
            _ => {
                bail!("recieved result is not a contract creation");
            }
        };
        info!("{contract_creation:?}");
        let Some(tx_hash) = contract_creation.first().map(|c| c.tx_hash.clone()) else {
            bail!("No Tx hash in contract creation response");
        };

        let req = BscRequest::new(
            "eth_getTransactionByHash",
            vec![BscRequestParam::Hash(&tx_hash)],
            1,
        );
        let response = req.request().await?;

        if let Some(BscResponseResult::Tx(tx)) = response.result {
            let tx = tx
                .block_hash
                .ok_or(anyhow!("No transaction for creation tx hash"))?;
            let block_hash_req = &format!("{tx:?}");

            let req = BscRequest::new(
                "eth_getBlockByHash",
                vec![
                    BscRequestParam::Hash(block_hash_req),
                    BscRequestParam::Bool(false),
                ],
                1,
            );
            let response = req.request().await?;
            let result = response
                .result
                .ok_or(anyhow!("No result for get block by hash request"))?;

            if let BscResponseResult::Block(block) = result {
                return Ok(block.timestamp.as_u64() as i64);
            }
            bail!("Block doesn't match response result");
        }

        bail!("No transaction for creation tx hash");
    }

    pub async fn decimals_in(&self) -> Result<u32> {
        if self.is_buy() {
            self.decimals(true).await
        } else {
            self.decimals(false).await
        }
    }

    pub async fn decimals_out(&self) -> Result<u32> {
        if self.is_buy() {
            self.decimals(false).await
        } else {
            self.decimals(true).await
        }
    }

    pub async fn decimals(&self, prim: bool) -> Result<u32> {
        let contract = if prim {
            &self.contracts.prim_contract
        } else {
            &self.contracts.token_contract
        };

        let decimals = contract.decimals().call().await.map(|dec| dec as u32)?;
        Ok(decimals)
    }

    pub async fn symbol(&self) -> Option<String> {
        let contract = &self.contracts.token_contract;
        contract.symbol().await.ok()
    }

    pub async fn prim_decimals(&self) -> Result<u32> {
        self.decimals(true).await
    }

    pub async fn token_decimals(&self) -> Result<u32> {
        self.decimals(false).await
    }

    pub async fn token_balance(&self, address: &str) -> Result<U256> {
        let address = H160::from_str(address)?;
        let balance = self
            .contracts
            .token_contract
            .balance_of(address)
            .call()
            .await?;
        Ok(balance)
    }

    pub async fn token_usd_balance(
        &self,
        token_balance: U256,
        addresses: Vec<H160>,
    ) -> Result<U256> {
        let token_amount = self
            .context
            .router_contract
            .get_amounts_out(token_balance, addresses)
            .call()
            .await?;
        info!(?token_amount);
        let token_usd_balance = token_amount
            .last()
            .cloned()
            .ok_or(anyhow!("No last token amount"))?;
        Ok(token_usd_balance)
    }

    pub async fn prim_balance(&self, address: &str) -> Result<U256> {
        let address = H160::from_str(address)?;
        let balance = self
            .contracts
            .prim_contract
            .balance_of(address)
            .call()
            .await?;
        Ok(balance)
    }

    pub async fn token_allowance(&self, address: &str, router_address: &str) -> Result<U256> {
        let address = H160::from_str(address)?;
        let router_address = H160::from_str(router_address)?;
        let allowance = self
            .contracts
            .token_contract
            .allowance(address, router_address)
            .call()
            .await?;
        Ok(allowance)
    }

    pub async fn token_approve_max(&self, address: &str) -> Result<()> {
        let address = H160::from_str(address)?;
        let result = self
            .contracts
            .token_contract
            .approve(address, U256::MAX)
            .send()
            .await?
            .await?;

        match result {
            Some(receipt) => {
                let tx = self
                    .provider()
                    .get_transaction(receipt.transaction_hash)
                    .await;
                info!("{tx:?}");
            }
            None => bail!("No approval recepit"),
        }
        Ok(())
    }

    pub fn prim_address(&self) -> H160 {
        self.addresses.prim_address
    }

    pub fn is_prim_bnb(&self) -> bool {
        let address = *WBNB_ADDRESS;
        self.addresses.prim_address == address
    }

    pub fn token_address(&self) -> H160 {
        self.addresses.token_address
    }

    pub fn is_buy(&self) -> bool {
        self.action.is_buy()
    }

    pub async fn demonitor(&self) -> Result<f64> {
        if self.is_prim_bnb() {
            let addresses = vec![*WBNB_ADDRESS, *BP_BSC_USD_ADDRESS];
            let bnb_price = self
                .context
                .router_contract
                .get_amounts_out(U256::from(1), addresses)
                .call()
                .await?;
            Ok(bnb_price[1].as_u32() as f64)
        } else {
            Ok(1.0)
        }
    }

    /// Minimum sell token amount in USD
    pub async fn min_token_sell(&self, min_usd_sell_amount: f64) -> Result<U256> {
        let token_address = self.token_address();
        let addresses = vec![*BP_BSC_USD_ADDRESS, token_address];
        if let Ok(min_token_amount) = self
            .context
            .router_contract
            .get_amounts_out(
                U256::from((min_usd_sell_amount * 10.0_f64.powi(18)) as u128),
                addresses,
            )
            .call()
            .await
        {
            if let Some(min_token_amount) = min_token_amount.last() {
                let addresses = vec![*BP_BSC_USD_ADDRESS, *WBNB_ADDRESS, token_address];
                if let Ok(min_token_amount_wbnb) = self
                    .context
                    .router_contract
                    .get_amounts_out(
                        U256::from((min_usd_sell_amount * 10.0_f64.powi(18)) as u128),
                        addresses,
                    )
                    .call()
                    .await
                {
                    if let Some(min_token_amount_wbnb) = min_token_amount_wbnb.last() {
                        let result = min_token_amount.max(min_token_amount_wbnb);
                        return Ok(*result);
                    }
                }
                return Ok(*min_token_amount);
            }
            bail!("No last token amount");
        } else {
            let addresses = vec![*BP_BSC_USD_ADDRESS, *WBNB_ADDRESS, token_address];
            let min_token_amount_wbnb = self
                .context
                .router_contract
                .get_amounts_out(
                    U256::from((min_usd_sell_amount * 10.0_f64.powi(18)) as u128),
                    addresses,
                )
                .call()
                .await;
            if let Ok(min_token_amount_wbnb) = min_token_amount_wbnb {
                if let Some(min_token_amount_wbnb) = min_token_amount_wbnb.last() {
                    return Ok(*min_token_amount_wbnb);
                }
            }

            bail!("No last token amount");
        }
    }

    /// Check if maximum token buy limitis exceeded in USD
    pub async fn is_exceeded(
        &self,
        max_total_usd_token_buy: f64,
        token_balance: U256,
    ) -> Result<bool> {
        let token_address = self.token_address();
        let addresses = vec![token_address, *BP_BSC_USD_ADDRESS];

        info!(?token_balance, ?token_address, ?addresses);

        if token_balance.is_zero() {
            return Ok(false);
        }

        let token_usd_balance =
            if let Ok(token_usd_balance) = self.token_usd_balance(token_balance, addresses).await {
                let addresses = vec![token_address, *WBNB_ADDRESS, *BP_BSC_USD_ADDRESS];
                let token_usd_balance_wbnb = self
                    .token_usd_balance(token_balance, addresses)
                    .await
                    .unwrap_or_default();

                let result = token_usd_balance.max(token_usd_balance_wbnb);

                result
            } else {
                let addresses = vec![token_address, *WBNB_ADDRESS, *BP_BSC_USD_ADDRESS];
                let token_usd_balance = self.token_usd_balance(token_balance, addresses).await?;
                token_usd_balance
            };

        let max_usd_token_amount =
            U256::from((max_total_usd_token_buy * 10.0_f64.powi(18)) as u128);
        info!(%token_usd_balance, %max_usd_token_amount);
        if token_usd_balance > max_usd_token_amount {
            return Ok(true);
        }
        Ok(false)
    }

    /* pub async fn format_tg_response(&self) -> Result<String> {
        let mut result;
        let action = self.action;
        result = if action.is_buy() {
            format!("{result}BUY \n")
        } else {
            format!("{result}SELL \n")
        }
    } */
}

#[derive(Serialize, Deserialize)]
pub struct BscRequest<'a> {
    pub jsonrpc: &'static str,
    pub method: &'a str,
    pub params: Vec<BscRequestParam<'a>>,
    pub id: i32,
}

impl<'a> BscRequest<'a> {
    pub fn new(method: &'a str, params: Vec<BscRequestParam<'a>>, id: i32) -> Self {
        Self {
            jsonrpc: JSON_RPC_VERSION,
            method,
            params,
            id,
        }
    }

    pub async fn request(self) -> Result<BscResponse> {
        let client = reqwest::Client::new();

        let res = client
            .post("https://bsc-dataseed.bnbchain.org")
            .json(&self)
            .send()
            .await?;

        let text = res.text().await?;
        let res = serde_json::from_str(&text)?;
        Ok(res)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BscResponse {
    pub jsonrpc: String,
    pub result: Option<BscResponseResult>,
    pub id: i32,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum BscRequestParam<'a> {
    Hash(&'a str),
    Bool(bool),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum BscResponseResult {
    Tx(Transaction),
    Block(Block<TxHash>),
}

#[derive(Debug)]
pub struct SwapContext {
    pub function_map: HashMap<String, SwapMethod>,
    pub functions: Vec<Function>,
    pub provider: Arc<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>,
    pub router_contract: Pancake<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>,
    pub eth: [H160; 3],
    pub bscscan_api: String,
}

impl SwapContext {
    pub async fn initialize() -> Result<Arc<Self>> {
        dotenv().ok();
        let signing_key = env::var("SIGNING_KEY").expect("SIGNING_KEY must be set");
        let bscscan_api = env::var("BSCSCAN_API").expect("BSCSCAN_API must be set");

        let function_map = SwapMethod::initialize_method_ids();
        let eth = [*BP_BSC_USD_ADDRESS, *WBNB_ADDRESS, *PEG_BUSD_ADDRESS];
        let client = reqwest::Client::new();
        let res = client
            .get(format!(
                "https://api.bscscan.com/api?module=contract&action=getabi&address={}&apikey={}",
                PANCAKESWAP_ADDRESS, bscscan_api
            ))
            .send()
            .await?;
        let text = res.text().await?;
        let res: BscScanResponse = serde_json::from_str(&text)?;
        let abi = match res.result {
            BscScanResult::String(s) => s,
            _ => {
                bail!("recieved abi is not String");
            }
        };

        let contract_abi = serde_json::from_str::<ethers::abi::Contract>(&abi)?;

        let functions = SwapMethod::iter()
            .filter_map(|method_name| contract_abi.function(method_name.into()).ok().cloned())
            .collect::<Vec<_>>();

        let signer = signing_key.parse::<LocalWallet>().unwrap();

        let provider = Arc::new(
            Provider::<Http>::try_from(HTTP_URL)
                .expect("could not instantiate HTTP Provider")
                .with_signer(signer.with_chain_id(56_u64)),
        );

        let router_contract = Pancake::new(H160::from_str(PANCAKESWAP_ADDRESS)?, provider.clone());

        /* let token_aadr = H160::from_str("8a7b5fe6f43db70affc51f3881ff79354640f3e7").unwrap();
        let token = Erc20Token::new(token_aadr, provider.clone());
        let token_decimals = token.decimals().await? as i32;
        //let balance = token.balance_of(H160::from_str(REAPEAT_ADDRESS)?).call().await?;
        let keyhash_token_balance =
            U256::from((3400.0 * 10.0f64.powi(token_decimals as i32)) as u128);
        println!("{keyhash_token_balance}");
        let addresses = vec![token_aadr, *BP_BSC_USD_ADDRESS];
        let token_amount = router_contract
            .get_amounts_out(keyhash_token_balance, addresses)
            .call()
            .await?;

        info!(?token_amount); */
        for address in eth {
            let prim_contract = Erc20Token::new(address, provider.clone());

            let Ok(prim_allowed) = prim_contract
                .allowance(
                    H160::from_str(REAPEAT_ADDRESS)?,
                    H160::from_str(PANCAKESWAP_ADDRESS)?,
                )
                .call()
                .await
            else {
                bail!("Error calling allowance function");
            };

            if prim_allowed < U256::MAX {
                let router_address = H160::from_str(PANCAKESWAP_ADDRESS)?;
                let approve = prim_contract.approve(router_address, U256::MAX);
                let sent = approve.send().await;

                let result = match sent {
                    Ok(result) => result.await,
                    Err(e) => {
                        bail!("Error sending approval: {e}");
                    }
                };

                match result {
                    Ok(receipt) => match receipt {
                        Some(receipt) => {
                            let tx = provider.get_transaction(receipt.transaction_hash).await;
                            info!("Approval: {tx:?}");
                        }
                        None => warn!("No approval recepit"),
                    },
                    Err(e) => error!("{e}"),
                }
            }
        }

        info!("Default sender: {:?}", provider.default_sender());

        Ok(Arc::new(Self {
            functions,
            provider,
            function_map,
            eth,
            router_contract,
            bscscan_api,
        }))
    }

    pub async fn get_token_usd_balance(&self, token: H160) -> Result<U256> {
        let repeat_address = H160::from_str(REAPEAT_ADDRESS).unwrap();
        let provider = self.provider.clone();
        let token_contract = Erc20Token::new(token, provider.clone());
        let balance = token_contract.balance_of(repeat_address).call().await?;
        if balance.is_zero() {
            return Ok(U256::zero());
        }
        let mut addresses = if token == *WBNB_ADDRESS {
            vec![*WBNB_ADDRESS, *BP_BSC_USD_ADDRESS]
        } else {
            vec![token, *BP_BSC_USD_ADDRESS]
        };

        if token != *WBNB_ADDRESS {
            if let Ok(token_amount) = self
                .router_contract
                .get_amounts_out(balance, addresses.clone())
                .call()
                .await
            {
                if let Some(token_usd_balance) = token_amount.last() {
                    error!("No last {token:?} usd amount");
                    addresses = vec![token, *WBNB_ADDRESS, *BP_BSC_USD_ADDRESS];
                    let token_amount = self
                        .router_contract
                        .get_amounts_out(balance, addresses)
                        .call()
                        .await
                        .unwrap_or_default();
                    let Some(token_usd_balance_wbnb) = token_amount.last() else {
                        error!("No last {token:?} usd wbnb amount");
                        return Ok(*token_usd_balance);
                    };

                    let result = token_usd_balance.max(token_usd_balance_wbnb);

                    return Ok(*result);
                };
            } else {
                addresses = vec![token, *WBNB_ADDRESS, *BP_BSC_USD_ADDRESS];
            }
        }
        let token_amount = self
            .router_contract
            .get_amounts_out(balance, addresses)
            .call()
            .await?;
        let Some(token_usd_balance) = token_amount.last() else {
            bail!("No last token amount");
        };

        Ok(*token_usd_balance)
    }

    pub async fn usdt_balance(&self) -> Result<U256> {
        let repeat_address = H160::from_str(REAPEAT_ADDRESS).unwrap();
        let provider = self.provider.clone();
        let token_contract = Erc20Token::new(*BP_BSC_USD_ADDRESS, provider.clone());

        let balance = token_contract.balance_of(repeat_address).call().await?;
        Ok(balance)
    }

    pub async fn eth_usd_balance(&self) -> Result<U256> {
        let repeat_address = H160::from_str(REAPEAT_ADDRESS).unwrap();
        let eth_balance = self.provider.get_balance(repeat_address, None).await?;
        let addresses = vec![*WBNB_ADDRESS, *BP_BSC_USD_ADDRESS];
        let bnb_price = self
            .router_contract
            .get_amounts_out(eth_balance, addresses)
            .call()
            .await?;
        Ok(bnb_price[1])
    }
}

#[derive(Error, Debug)]
pub enum SwapError {
    #[error("This is not a swap method")]
    NotSwapMethod,
    #[error("No method name: {0} in abi")]
    NotFoundInAbi(String),
    #[error("No {0} address in address array")]
    AddressNotFound(String),
    #[error("Not found token that can be parsed into array: {0:?}")]
    TokenNotFound(Vec<Token>),
    #[error("Decode function error")]
    DecodeError(#[from] ethers::abi::ethabi::Error),
    #[error("Decode input is too short")]
    DecodeInputError(String),
    #[error("From address and token receive address is different")]
    SwapToDifferentAddress,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BscScanResponse {
    pub status: String,
    pub message: String,
    pub result: BscScanResult,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum BscScanResult {
    String(String),
    GetContractCreation(Vec<GetContractCreation>),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetContractCreation {
    pub contract_address: String,
    pub contract_creator: String,
    pub tx_hash: String,
}

pub fn read_abi_from_file<P: AsRef<Path>>(path: P) -> Result<ethers::abi::Contract> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let u = serde_json::from_reader(reader)?;
    Ok(u)
}

pub fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        write!(&mut s, "{:02x}", b).unwrap();
    }
    s
}

/* #[derive(Debug, Clone, Serialize, Deserialize, AsExpression, FromSqlRow, PartialEq)]
#[diesel(sql_type = diesel::sql_types::Text)]
pub struct StringArray<T>(Vec<T>);

impl<T: AsRef<str>> From<Vec<T>> for StringArray<T> {
    fn from(v: Vec<T>) -> Self {
        Self(v)
    }
}

impl<T: AsRef<str>> std::ops::Deref for StringArray<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
} */

/* impl<T: AsRef<str> + std::fmt::Debug> ToSql<Text, Sqlite> for StringArray<T>
where
    String: ToSql<Text, Sqlite>,
{
    fn to_sql<'b>(&self, out: &mut Output<'b, '_, Sqlite>) -> serialize::Result {
        let mut s = String::new();

        for (i, x) in self.0.iter().enumerate() {
            s += x.as_ref();

            if i != self.0.len() - 1 {
                s += ",";
            }
        }

        out.set_value(s);

        Ok(serialize::IsNull::No)
    }
} */

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("Error getting timestamp")
        .as_secs() as i64
}
