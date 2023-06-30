use arrow::{
    array::{ArrayRef, Float64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use bitcoin::{Denomination, Txid};
use bitcoincore_rest::{responses::GetMempoolEntryResult, Error, Network, RestApi, RestClient};
use chrono::Timelike;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

type Mempool = HashMap<Txid, GetMempoolEntryResult>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let rest_client = RestClient::network_default(Network::Bitcoin);

    let mut prev_height = 0u64;
    let mut this_height;
    let mut prev_mempool: Mempool = HashMap::new();
    let mut this_mempool: Mempool;
    let mut prev_timestamp = 0i64;

    loop {
        // execute once per quarter-minute (:00/:15/:30/:45), preventing double execution
        let now = chrono::Utc::now();
        if now.second() % 15 != 0 || prev_timestamp == now.timestamp() {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            continue;
        }

        // check height
        this_height = rest_client.get_chain_info().await?.blocks;
        let is_new_height = prev_height != this_height;
        if is_new_height {
            prev_mempool = HashMap::new();
            println!("new_height: {:?}", this_height);
        }

        let start = Instant::now();
        this_mempool = rest_client.get_mempool().await?;
        let duration = start.elapsed();

        let batch = create_batchrecord_delta_from_pools(&prev_mempool, &this_mempool);

        println!(
            "batch_num_rows: {:?}, load_mempool_duration_millis: {:?}",
            batch.num_rows(),
            duration.as_millis()
        );

        let mut filename = std::path::PathBuf::new();
        let day = now.format("%Y/%m/%d").to_string();
        let timestamp = now.timestamp();
        let suffix = if is_new_height { "full" } else { "delta" };
        filename.push("data");
        filename.extend(day.split('/'));
        filename.push(format!("{this_height}_{timestamp}_{suffix}.parquet"));
        save_batchrecord_deltas_to_parquet(batch, filename);

        prev_height = this_height;
        prev_mempool = this_mempool;
        prev_timestamp = now.timestamp();
    }
}

fn save_batchrecord_deltas_to_parquet(batch: RecordBatch, filename: std::path::PathBuf) {
    // make sure the folder exists
    std::fs::create_dir_all(filename.parent().unwrap()).unwrap();

    let file = std::fs::File::create(filename).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn create_batchrecord_delta_from_pools(
    prev_mempool: &Mempool,
    this_mempool: &Mempool,
) -> RecordBatch {
    let (keys_removed, keys_added) = delta_of_keys(prev_mempool, this_mempool);

    // TODO: combine into one batch:

    let capacity = keys_removed.len() + keys_added.len();

    let mut txid_values: Vec<String> = Vec::with_capacity(capacity);
    let mut weight_values: Vec<f64> = Vec::with_capacity(capacity);
    let mut fee_sat_values: Vec<f64> = Vec::with_capacity(capacity);
    let mut first_seen_timestamp_values: Vec<u64> = Vec::with_capacity(capacity);

    for txid in keys_removed.iter() {
        let entry = prev_mempool.get(txid).unwrap();
        let weight: f64 = entry.weight.unwrap() as f64 * (-1.);

        txid_values.push(txid.to_string());
        weight_values.push(weight);
        fee_sat_values.push(entry.fees.base.to_float_in(Denomination::Satoshi));
        first_seen_timestamp_values.push(entry.time);
    }

    for txid in keys_added.iter() {
        let entry = this_mempool.get(txid).unwrap();
        let weight = entry.weight.unwrap() as f64;

        txid_values.push(txid.to_string());
        weight_values.push(weight);
        fee_sat_values.push(entry.fees.base.to_float_in(Denomination::Satoshi));
        first_seen_timestamp_values.push(entry.time);
    }

    let txid_array: ArrayRef = Arc::new(StringArray::from(txid_values));
    let weight_array: ArrayRef = Arc::new(Float64Array::from(weight_values));
    let fee_sat_array: ArrayRef = Arc::new(Float64Array::from(fee_sat_values));
    let first_seen_at_array: ArrayRef = Arc::new(UInt64Array::from(first_seen_timestamp_values));

    let schema = Schema::new(vec![
        Field::new("txid", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, false),
        Field::new("fee_sat", DataType::Float64, false),
        Field::new("first_seen_at", DataType::UInt64, true),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![txid_array, weight_array, fee_sat_array, first_seen_at_array],
    )
    .unwrap()
}

fn delta_of_keys(map1: &Mempool, map2: &Mempool) -> (Vec<Txid>, Vec<Txid>) {
    let mut intersection = HashSet::new();
    let mut keys_added = Vec::new();
    let mut keys_removed = Vec::new();

    for key in map1.keys() {
        if map2.contains_key(key) {
            intersection.insert(key);
        } else {
            keys_removed.push(*key);
        }
    }

    for key in map2.keys() {
        if !intersection.contains(key) {
            keys_added.push(*key);
        }
    }

    (keys_removed, keys_added)
}
