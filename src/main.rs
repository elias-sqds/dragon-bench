use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, error::Error};

use futures::stream::StreamExt;
use solana_client::nonblocking::rpc_client::{self, RpcClient};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::signature::Signature;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{Level, error, info, instrument};
use tracing_subscriber::FmtSubscriber;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestFilterBlocksMeta};
use solana_transaction_status::{EncodedTransaction, TransactionDetails};
use solana_client::rpc_config::RpcBlockConfig;

#[derive(Default, Debug, Clone)]
struct BlockInfo {
    transactions: Vec<Signature>,
    blockhash: Option<String>,
    is_complete: bool,
}

#[instrument]
async fn run_test() -> Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();

    info!("Starting Dragon's Mouth stream connection test");

    // Configure the Dragon's Mouth gRPC client
    let grpc_url = std::env::var("DRAGONS_MOUTH_URL").expect("DRAGONS_MOUTH_URL is not set");
    let grpc_token = std::env::var("DRAGONS_MOUTH_TOKEN").expect("DRAGONS_MOUTH_TOKEN is not set");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL is not set");
    let rpc_client = Arc::new(rpc_client::RpcClient::new(rpc_url));

    info!("Connecting to Dragon's Mouth at {}", grpc_url);

    let tls_config = ClientTlsConfig::new().with_native_roots();
    let mut client = GeyserGrpcClient::build_from_shared(grpc_url).expect("build_from_shared failed")
        .x_token(Some(grpc_token))
        .expect("x-token failed")
        .tls_config(tls_config)
        .expect("tls config failed")
        .connect()
        .await
        .expect("Failed to connect to geyser");

    let mut transactions_map = HashMap::new();
    transactions_map.insert(
        "transactions".to_string(),
        SubscribeRequestFilterTransactions {
            // vote: Some(false),
            failed: Some(false),
            ..Default::default()
        },
    );

    let mut blocks_meta_map = HashMap::new();
    blocks_meta_map.insert(
        "blocks_meta".to_string(),
        SubscribeRequestFilterBlocksMeta {},
    );

    let req = SubscribeRequest {
        transactions: transactions_map,
        blocks_meta: blocks_meta_map,
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };

    info!("Establishing subscription stream with request: {:#?}", req);

    let (_, stream) = client
        .subscribe_with_request(Some(req.clone()))
        .await
        .expect("subscribe failed");

    info!("Geyser stream established");

    let blocks = Arc::new(Mutex::new(HashMap::<u64, BlockInfo>::new()));
    let blocks_clone = blocks.clone();

    let handle = tokio::spawn(async move {
        let mut stream = stream;
        let blocks = blocks_clone;

        info!("Starting to process stream messages");

        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    if let Some(update) = msg.update_oneof {
                        match update {
                            yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Transaction(tx) => {
                                let signature_bytes = tx.transaction.unwrap().signature;
                                let signature = solana_sdk::signature::Signature::try_from(signature_bytes)
                                    .unwrap();
                                let slot = tx.slot;

                                let mut blocks_lock = blocks.lock().await;
                                let block_data = blocks_lock.entry(slot).or_default();
                                block_data.transactions.push(signature);
                                
                                // tracing::info!("Transaction added to slot {}: {}", slot, signature);
                            },
                            yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::BlockMeta(meta) => {
                                let mut blocks_lock = blocks.lock().await;
                                let block_info = blocks_lock.entry(meta.slot).or_default();
                                block_info.blockhash = Some(meta.blockhash);
                                block_info.is_complete = true;

                                // Verify block against RPC
                                verify_block(&rpc_client, meta.slot, block_info).await;
                            },
                            _ => {
                                tracing::debug!("Received other update type");
                            }
                        }
                    }
                }
                Err(err) => {
                    error!("Stream error: {:?}", err);
                }
            }
        }
        info!("Stream ended");
    });

    // Print periodic statistics
    tokio::spawn(async move {
        let report_interval = Duration::from_secs(30);
        let mut interval = time::interval(report_interval);

        loop {
            interval.tick().await;
            let blocks_lock = blocks.lock().await;
            let complete_blocks = blocks_lock.iter()
                .filter(|(_, info)| info.is_complete)
                .count();
            
            info!(
                "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                 📊 Status Update\n\
                 ┣━━ Blocks ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                 ┃ Total Tracked       │ {:>6}\n\
                 ┃ Complete            │ {:>6}\n\
                 ┃ Incomplete          │ {:>6}\n\
                 ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                blocks_lock.len(),
                complete_blocks,
                blocks_lock.len() - complete_blocks
            );
        }
    });

    // Wait for the stream processing task
    handle.await?;

    info!("Test completed");
    Ok(())
}

async fn verify_block(rpc_client: &Arc<RpcClient>, slot: u64, block_info: &BlockInfo) {
    // Add longer delay to allow RPC to process the block
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Add config to support versioned transactions
    let config = RpcBlockConfig {
        encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
        transaction_details: Some(TransactionDetails::Full),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
        ..Default::default()
    };

    match rpc_client.get_block_with_config(slot, config).await {
        Ok(block) => {
            // Count only successful transactions
            let rpc_tx_count_before_filter = block.transactions.clone().unwrap().len();
            let rpc_tx_count = block.transactions.clone().unwrap().iter()
                .filter(|tx| tx.meta.as_ref().map_or(false, |meta| meta.err.is_none()))
                .count();
            info!("RPC tx count before filter: {}", rpc_tx_count_before_filter);
            info!(" RPC tx count after filter: {}", rpc_tx_count);
            let geyser_tx_count = block_info.transactions.len();
            let coverage = geyser_tx_count as f64 / rpc_tx_count as f64 * 100.0;

            // Calculate missing transactions first
            let geyser_tx_set: std::collections::HashSet<_> = block_info.transactions
                .iter()
                .map(|sig| sig.to_string())
                .collect();

            let missing_txs: Vec<_> = block.transactions.unwrap().iter()
                .filter_map(|tx| {
                    // Skip failed transactions
                    if !tx.meta.as_ref().map_or(false, |meta| meta.err.is_none()) {
                        return None;
                    }

                    match &tx.transaction {
                        EncodedTransaction::Binary(_, _) | EncodedTransaction::LegacyBinary(_) => {
                            tx.transaction.decode().map(|decoded_tx| {
                                decoded_tx.signatures.first().map(|sig| sig.to_string())
                            }).flatten()
                        },
                        EncodedTransaction::Json(ui_tx) => {
                            ui_tx.signatures.first().map(|sig| sig.to_string())
                        },
                        _ => None,
                    }
                })
                .filter(|sig| !geyser_tx_set.contains(sig))
                .collect();

            let missing_count = missing_txs.len();

            // Now print the verification report with missing transactions count
            info!(
                "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                 📦 Block {} Verification Report\n\
                 ┣━━ Status ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                 ┃ Blockhash Match    │ {}\n\
                 ┃ RPC Transactions   │ {:>6}\n\
                 ┃ Geyser Txs         │ {:>6}\n\
                 ┃ Missing Txs        │ {:>6} ({})\n\
                 ┃ Coverage           │ {:>6.2}%\n\
                 ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                slot,
                if block.blockhash == *block_info.blockhash.as_ref().unwrap() { "✅" } else { "❌" },
                rpc_tx_count,
                geyser_tx_count,
                missing_count,
                if !missing_txs.is_empty() { 
                    format!("first: {}", missing_txs.first().unwrap())
                } else {
                    "none".to_string()
                },
                coverage
            );
        }
        Err(err) => {
            if err.to_string().contains("Block not available") {
                info!(
                    "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                     ⏳ Block {} not yet available, retrying in 4s...\n\
                     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    slot
                );
                // Retry with longer delay
                tokio::time::sleep(Duration::from_secs(4)).await;
                match rpc_client.get_block_with_config(slot, config).await {
                    Ok(rpc_block) => {
                        // Count only successful transactions
                        let rpc_tx_count = rpc_block.transactions.clone().unwrap().iter()
                            .filter(|tx| tx.meta.as_ref().map_or(false, |meta| meta.err.is_none()))
                            .count();
                        let geyser_tx_count = block_info.transactions.len();
                        let coverage = geyser_tx_count as f64 / rpc_tx_count as f64 * 100.0;

                        // Calculate missing transactions first
                        let geyser_tx_set: std::collections::HashSet<_> = block_info.transactions
                            .iter()
                            .map(|sig| sig.to_string())
                            .collect();

                        let missing_txs: Vec<_> = rpc_block.transactions.unwrap().iter()
                            .filter_map(|tx| {
                                // Skip failed transactions
                                if !tx.meta.as_ref().map_or(false, |meta| meta.err.is_none()) {
                                    return None;
                                }

                                match &tx.transaction {
                                    EncodedTransaction::Binary(_, _) | EncodedTransaction::LegacyBinary(_) => {
                                        tx.transaction.decode().map(|decoded_tx| {
                                            decoded_tx.signatures.first().map(|sig| sig.to_string())
                                        }).flatten()
                                    },
                                    EncodedTransaction::Json(ui_tx) => {
                                        ui_tx.signatures.first().map(|sig| sig.to_string())
                                    },
                                    _ => None,
                                }
                            })
                            .filter(|sig| !geyser_tx_set.contains(sig))
                            .collect();

                        let missing_count = missing_txs.len();

                        info!(
                            "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                             📦 Block {} Verification Report\n\
                             ┣━━ Status ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                             ┃ Blockhash Match    │ {}\n\
                             ┃ RPC Transactions   │ {:>6}\n\
                             ┃ Geyser Txs         │ {:>6}\n\
                             ┃ Missing Txs        │ {:>6} ({})\n\
                             ┃ Coverage           │ {:>6.2}%\n\
                             ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                            slot,
                            if rpc_block.blockhash == *block_info.blockhash.as_ref().unwrap() { "✅" } else { "❌" },
                            rpc_tx_count,
                            geyser_tx_count,
                            missing_count,
                            if !missing_txs.is_empty() { 
                                format!("first: {}", missing_txs.first().unwrap())
                            } else {
                                "none".to_string()
                            },
                            coverage
                        );
                    }
                    Err(retry_err) => {
                        error!("Failed to fetch block {} from RPC after retry: {:?}", slot, retry_err);
                    }
                }
            } else {
                error!(
                    "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                     ❌ Error fetching block {}\n\
                     ┃ {}\n\
                     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    slot,
                    err
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set default tracing subscriber");

    info!("Starting Dragon's Mouth reliability test...");
    run_test().await
}
