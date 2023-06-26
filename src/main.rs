use arrow::{
    array::{ArrayRef, Float64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use reqwest::Error;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use tokio::time::{sleep, Duration};

use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct MempoolEntry {
    vsize: u32,
    weight: u32,
    time: u64,
    height: u32,
    descendantcount: u32,
    descendantsize: u32,
    ancestorcount: u32,
    ancestorsize: u32,
    wtxid: String,
    fees: Fees,
    depends: Vec<String>,
    spentby: Vec<String>,
    #[serde(rename = "bip125-replaceable")]
    bip125_replaceable: bool,
    unbroadcast: bool,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct Fees {
    base: f64,
    modified: f64,
    ancestor: f64,
    descendant: f64,
}

#[derive(Deserialize, Debug)]
//#[allow(dead_code)]
pub struct ChainInfo {
    pub chain: String,
    pub blocks: u64,
    pub headers: u64,
    pub bestblockhash: String,
    pub difficulty: f64,
    pub time: u64,
    pub mediantime: u64,
    pub verificationprogress: f64,
    pub initialblockdownload: bool,
    pub chainwork: String,
    pub size_on_disk: u64,
    pub pruned: bool,
    pub warnings: String,
}

type Mempool = HashMap<String, MempoolEntry>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Set the bitcoind REST URL. run bitcoin core 25.0+ with -rest

    let mut prev_height = 0u64;
    let mut this_height;
    let mut prev_mempool: Mempool = HashMap::new();
    let mut this_mempool: Mempool;

    loop {
        // check height
        this_height = load_height().await?;

        if prev_height != this_height {
            prev_mempool = HashMap::new();
            println!("new_height: {:?}", this_height);
        }

        let start = Instant::now();
        this_mempool = load_mempool().await?;
        let duration = start.elapsed();

        let batch = create_batchrecord_delta_from_pools(&prev_mempool, &this_mempool);

        println!(
            "batch_num_rows: {:?}, load_mempool_duration_millis: {:?}",
            batch.num_rows(),
            duration.as_millis()
        );

        save_batchrecord_deltas_to_parquet(batch);

        sleep(Duration::from_secs(3)).await;

        prev_height = this_height;
        prev_mempool = this_mempool;
    }
}

fn save_batchrecord_deltas_to_parquet(batch: RecordBatch) {
    let file = std::fs::File::create("data/test.parquet").unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn load_height() -> Result<u64, Error> {
    let chaininfo = "http://127.0.0.1:8332/rest/chaininfo.json";
    let response = reqwest::Client::new().get(chaininfo).send().await?;

    if !response.status().is_success() {
        eprintln!("Error requesting chaininfo: {}", response.status());
    }

    let chaininfo: ChainInfo = response.json().await?;
    Ok(chaininfo.blocks)
}

async fn load_mempool() -> Result<Mempool, Error> {
    let mempool = "http://127.0.0.1:8332/rest/mempool/contents.json";
    let response = reqwest::Client::new().get(mempool).send().await?;

    if !response.status().is_success() {
        panic!("Error downloading mempool: {}", response.status());
    }

    // Deserialize the JSON response into a HashSet
    Ok(response.json().await?)
}

fn create_batchrecord_delta_from_pools(
    prev_mempool: &Mempool,
    this_mempool: &Mempool,
) -> RecordBatch {
    let (keys_removed, keys_added) = delta_of_keys(&prev_mempool, &this_mempool);

    // TODO: combine into one batch:

    let capacity = keys_removed.len() + keys_added.len();

    let mut txid_values: Vec<String> = Vec::with_capacity(capacity);
    let mut weight_values: Vec<f64> = Vec::with_capacity(capacity);
    let mut fee_sat_values: Vec<f64> = Vec::with_capacity(capacity);
    let mut first_seen_timestamp_values: Vec<u64> = Vec::with_capacity(capacity);

    for txid in keys_removed.iter() {
        let entry = prev_mempool.get(txid).unwrap();
        let weight: f64 = entry.weight as f64 * (-1.);

        txid_values.push(txid.clone());
        weight_values.push(weight);
        fee_sat_values.push(entry.fees.base * 1e8);
        first_seen_timestamp_values.push(entry.time);
    }

    for txid in keys_added.iter() {
        let entry = this_mempool.get(txid).unwrap();
        let weight = entry.weight as f64;

        txid_values.push(txid.clone());
        weight_values.push(weight);
        fee_sat_values.push(entry.fees.base * 1e8);
        first_seen_timestamp_values.push(entry.time);
    }

    let txid_array: ArrayRef = Arc::new(StringArray::from(txid_values));
    let weight_array: ArrayRef = Arc::new(Float64Array::from(weight_values));
    let fee_sat_array: ArrayRef = Arc::new(Float64Array::from(fee_sat_values));
    let first_seen_timestamp_array: ArrayRef =
        Arc::new(UInt64Array::from(first_seen_timestamp_values));

    let schema = Schema::new(vec![
        Field::new("txid", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, false),
        Field::new("fee_sat", DataType::Float64, false),
        Field::new("first_seen_timestamp", DataType::UInt64, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            txid_array,
            weight_array,
            fee_sat_array,
            first_seen_timestamp_array,
        ],
    )
    .unwrap();

    batch
}

fn delta_of_keys<T>(
    map1: &HashMap<String, T>,
    map2: &HashMap<String, T>,
) -> (Vec<String>, Vec<String>) {
    let mut intersection = HashSet::new();
    let mut keys_added = Vec::new();
    let mut keys_removed = Vec::new();

    for key in map1.keys() {
        if map2.contains_key(key) {
            intersection.insert(key.to_string());
        } else {
            keys_removed.push(key.to_string());
        }
    }

    for key in map2.keys() {
        if !intersection.contains(key) {
            keys_added.push(key.to_string());
        }
    }

    (keys_removed, keys_added)
}
