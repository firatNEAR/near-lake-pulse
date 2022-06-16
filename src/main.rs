use actix_web::{get, App, HttpServer, Responder};
use clap::Parser;
use lazy_static::lazy_static;
use prometheus::{Encoder, IntCounter, IntGauge};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, Mutex};
use tracing::info;
use tracing_subscriber::EnvFilter;

use configs::Opts;
use near_lake_framework::{near_indexer_primitives, LakeConfig};

mod configs;

lazy_static! {
    static ref TOTAL_ACCOUNTS_COUNTER_BY_CREATOR: IntCounter = IntCounter::new(
        "total_accounts_counter_by_creator",
        "Total account count by creator"
    )
    .unwrap();
    // static ref TOTAL_HISTOGRAM: Histogram = Histogram::with_opts(HistogramOpts::new(
    //     "total_account_histogram",
    //     "Total account count by creator histogram"
    // ))
    // .into();
    static ref TOTAL_NUMBER_OF_TRANSACTION_BY_CREATED: IntCounter = IntCounter::new(
        "total_transaction_counter_by_created",
        "Total number of transaction by created"
    ).unwrap();
    static ref DAILY_ACTIVE_ACCOUNTS: IntGauge = IntGauge::new(
        "daily_active_accounts",
        "Daily active accounts"
    )
    .unwrap();
}
// The timestamp (nanos) when transfers were enabled in the Mainnet after community voting
// Tuesday, 13 October 2020 18:38:58.293
pub const TRANSFERS_ENABLED: Duration = Duration::from_nanos(1602614338293769340);
pub const DAY: Duration = Duration::from_secs(60 * 60 * 24);
#[derive(Debug, Clone)]
struct Stats {
    pub total_account_by_creator: u64,
    pub created_accounts: HashSet<String>,
    pub daily_active_accounts: HashSet<String>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            total_account_by_creator: 0,
            created_accounts: HashSet::new(),
            daily_active_accounts: HashSet::new(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), tokio::io::Error> {
    init_tracing();

    let opts: Opts = Opts::parse();
    let http_port = opts.http_port;
    let config: LakeConfig = opts.chain_id.into();
    let (_, stream) = near_lake_framework::streamer(config);

    // Register custom metrics to a custom registry.
    prometheus::default_registry()
        .register(Box::new(TOTAL_ACCOUNTS_COUNTER_BY_CREATOR.clone()))
        .unwrap();
    prometheus::default_registry()
        .register(Box::new(TOTAL_NUMBER_OF_TRANSACTION_BY_CREATED.clone()))
        .unwrap();
    prometheus::default_registry()
        .register(Box::new(DAILY_ACTIVE_ACCOUNTS.clone()))
        .unwrap();
    let stats: Arc<Mutex<Stats>> = Arc::new(Mutex::new(Stats::new()));
    let watching_list = opts
        .accounts
        .split(',')
        .map(|elem| {
            near_indexer_primitives::types::AccountId::from_str(elem).expect("AccountId is invalid")
        })
        .collect();

    tokio::spawn(async move { listen_blocks(stream, watching_list, Arc::clone(&stats)).await });

    HttpServer::new(|| App::new().service(metrics))
        .bind(("0.0.0.0", http_port))?
        .run()
        .await
        .unwrap();

    Ok(())
}
#[get("/metrics")]
async fn metrics() -> impl Responder {
    let mut buffer = Vec::<u8>::new();
    let encoder = prometheus::TextEncoder::new();
    loop {
        match encoder.encode(&prometheus::gather(), &mut buffer) {
            Ok(_) => break,
            Err(err) => {
                eprintln!("{:?}", err);
            }
        }
    }
    String::from_utf8(buffer.clone()).unwrap()
}

fn init_tracing() {
    let mut env_filter = EnvFilter::new("near_lake_framework=info");

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}
async fn listen_blocks(
    mut stream: mpsc::Receiver<near_indexer_primitives::StreamerMessage>,
    watching_list: Vec<near_indexer_primitives::types::AccountId>,
    stats: Arc<Mutex<Stats>>,
) {
    eprintln!("listen_blocks");
    // This will be a map of correspondence between transactions and receipts
    let mut tx_receipt_ids = HashMap::<String, String>::new();
    // This will be a list of receipt ids we're following
    let mut wanted_receipt_ids = HashSet::<String>::new();
    // This is the block_timestamp of the starting block
    let mut block_start_timestamp: Duration =
        Duration::from_nanos(stream.recv().await.unwrap().block.header.timestamp_nanosec);
    // Boilerplate code to listen the stream
    while let Some(streamer_message) = stream.recv().await {
        for shard in streamer_message.shards {
            let chunk = if let Some(chunk) = shard.chunk {
                chunk
            } else {
                continue;
            };

            for transaction in chunk.transactions {
                // Check if transaction signer id is one of the list we are interested in or if transaction signer id is one of the accounts created by the master account
                if is_tx_signer_watched(&transaction, &watching_list)
                    || is_tx_signer_in_created_accounts(
                        &transaction,
                        &stats.lock().await.created_accounts,
                    )
                {
                    // extract receipt_id transaction was converted into
                    let converted_into_receipt_id = transaction
                        .outcome
                        .execution_outcome
                        .outcome
                        .receipt_ids
                        .first()
                        .expect("`receipt_ids` must contain one Receipt Id")
                        .to_string();
                    // add `converted_into_receipt_id` to the list of receipt ids we are interested in
                    wanted_receipt_ids.insert(converted_into_receipt_id.clone());
                    // add key value pair of transaction hash and in which receipt id it was converted for further lookup
                    tx_receipt_ids.insert(
                        converted_into_receipt_id,
                        transaction.transaction.hash.to_string(),
                    );
                }
            }

            for execution_outcome in shard.receipt_execution_outcomes {
                if let Some(receipt_id) =
                    wanted_receipt_ids.take(&execution_outcome.receipt.receipt_id.to_string())
                {
                    // log the tx because we've found it
                    info!(
                        target: "indexer_example",
                        "Transaction hash {:?} related to {} executed with status {:?}",
                        tx_receipt_ids.get(receipt_id.as_str()),
                        &execution_outcome.receipt.predecessor_id,
                        execution_outcome.execution_outcome.outcome.status
                    );
                    let mut stats_lock = stats.lock().await;
                    // if the transaction is initiated by the master account increase the number of the created accounts
                    if watching_list.contains(&execution_outcome.receipt.predecessor_id) {
                        TOTAL_ACCOUNTS_COUNTER_BY_CREATOR.inc();
                        stats_lock.total_account_by_creator += 1;
                        stats_lock
                            .created_accounts
                            .insert(execution_outcome.receipt.receiver_id.to_string());
                    }
                    //If the transaction is created by a child account increase the total transaction counter and include them in daily active accounts
                    if stats_lock
                        .created_accounts
                        .contains(&execution_outcome.receipt.predecessor_id.to_string())
                    {
                        stats_lock
                            .daily_active_accounts
                            .insert(execution_outcome.receipt.predecessor_id.to_string());
                        TOTAL_NUMBER_OF_TRANSACTION_BY_CREATED.inc();
                        DAILY_ACTIVE_ACCOUNTS
                            .set(stats_lock.daily_active_accounts.len().try_into().unwrap());
                        //if we are at the end of the day, reset the block_start_timestamp and clear the daily active accounts
                        if block_start_timestamp + DAY
                            == Duration::from_nanos(streamer_message.block.header.timestamp_nanosec)
                        {
                            DAILY_ACTIVE_ACCOUNTS.set(0);
                            block_start_timestamp = Duration::from_nanos(
                                streamer_message.block.header.timestamp_nanosec,
                            );
                            stats_lock.daily_active_accounts.clear();
                        }
                    }
                    drop(stats_lock);
                    if let near_indexer_primitives::views::ReceiptEnumView::Action {
                        signer_id,
                        ..
                    } = &execution_outcome.receipt.receipt
                    {
                        eprintln!("{}", signer_id);
                    }
                    eprintln!(
                        "Current account number:{:?} and Current total transactions by created accounts {:?}, current total daily active users {:?}",
                        TOTAL_ACCOUNTS_COUNTER_BY_CREATOR.get(),
                        TOTAL_NUMBER_OF_TRANSACTION_BY_CREATED.get(),
                        DAILY_ACTIVE_ACCOUNTS.get(),
                    );
                    // remove tx from hashmap
                    tx_receipt_ids.remove(receipt_id.as_str());
                }
            }
        }
    }
}
fn is_tx_signer_watched(
    tx: &near_indexer_primitives::IndexerTransactionWithOutcome,
    watching_list: &[near_indexer_primitives::types::AccountId],
) -> bool {
    watching_list.contains(&tx.transaction.signer_id)
}

fn is_tx_signer_in_created_accounts(
    tx: &near_indexer_primitives::IndexerTransactionWithOutcome,
    created_accounts: &HashSet<String>,
) -> bool {
    created_accounts.contains(&tx.transaction.signer_id.to_string())
}
