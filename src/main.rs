use arrow::{
    array::{ArrayRef, Float64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use reqwest::Error;
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc, time::Instant};
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Set the bitcoind REST URL. run bitcoin core 25.0+ with -rest

    let chaininfo = "http://127.0.0.1:8332/rest/chaininfo.json";
    let response = reqwest::Client::new().get(chaininfo).send().await?;

    if response.status().is_success() {
        let chaininfo: ChainInfo = response.json().await?;
        println!("chain_blocks: {}", chaininfo.blocks);
    } else {
        eprintln!("Error requesting chaininfo: {}", response.status());
    }

    loop {
        tick().await?;
        sleep(Duration::from_secs(3)).await;
    }
}

async fn tick() -> Result<(), Error> {
    let start = Instant::now();
    let mempool = "http://127.0.0.1:8332/rest/mempool/contents.json";
    let response = reqwest::Client::new().get(mempool).send().await?;
    let duration = start.elapsed();

    if !response.status().is_success() {
        panic!("Error downloading mempool: {}", response.status());
    }

    // Deserialize the JSON response into a HashSet
    let mempool: HashMap<String, MempoolEntry> = response.json().await?;
    println!("mempool_len: {} ", mempool.len());
    let capacity = mempool.len();

    let mut txid_values: Vec<String> = Vec::with_capacity(capacity);
    let mut weight_values: Vec<f64> = Vec::with_capacity(capacity);
    let mut fee_sat_values: Vec<f64> = Vec::with_capacity(capacity);
    let mut first_seen_timestamp_values: Vec<u64> = Vec::with_capacity(capacity);
    let mut fee_sum_btc = 0.;

    for (txid, entry) in mempool.iter() {
        println!("Transaction ID: {}", txid);
        txid_values.push(txid.clone());
        weight_values.push(entry.weight as f64);
        fee_sat_values.push(entry.fees.base * 1e8);
        first_seen_timestamp_values.push(entry.time);
        fee_sum_btc += entry.fees.base;
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

    let file = std::fs::File::create("data/test.parquet").unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();

    println!("fee_sum_btc: {:?}", fee_sum_btc);
    println!("duration_millis: {:?}", duration.as_millis());

    Ok(())
}
