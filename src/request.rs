use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use ethers::prelude::*;
use tokio::sync::broadcast::Sender;
use tracing::error;

use ethers::providers::{Provider, Ws};

use crate::common::{SwapContext, SwapError, SwapTx, PANCAKESWAP_ADDRESS, WS_URL};

pub async fn run(tx_tx: Sender<SwapTx>) -> Result<()> {
    let pancakeswap_address = H160::from_str(PANCAKESWAP_ADDRESS).expect("Bad exchange address");

    let swap_context = SwapContext::initialize().await?;

    let provider = Arc::new(Provider::<Ws>::connect(WS_URL).await?);

    let mut stream = provider.subscribe_full_pending_txs().await?;

    loop {
        while let Some(transaction) = stream.next().await {
            if let Some(to) = &transaction.to {
                if to != &pancakeswap_address {
                    continue;
                }
                match SwapTx::decode_from(transaction.clone(), swap_context.clone()) {
                    Ok(swap_tx) => {
                        if !swap_tx.need_process() {
                            continue;
                        }

                        if let Err(e) = tx_tx.send(swap_tx) {
                            error!("Error sending tx: {e}");
                        }
                    }
                    Err(e) => {
                        if !matches!(e, SwapError::NotSwapMethod) {
                            error!(hash=?transaction.hash ,"Error decoding tx: {e}");
                        }
                    }
                }
            }
        }
    }
}
