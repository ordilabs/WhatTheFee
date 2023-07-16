use bitcoin::{Denomination, Txid};
use bitcoincore_rest::{responses::GetMempoolEntryResult, Error, RestApi, RestClient};
use chrono::Timelike;
use polars::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};
use tracing::info;

type Mempool = HashMap<Txid, GetMempoolEntryResult>;

pub struct Record;

impl Record {
    #[tracing::instrument]
    pub async fn record(data_dir: String, bitcoin_core_endpoint: String) -> Result<(), Error> {
        let rest_client = RestClient::new(bitcoin_core_endpoint);

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
                info!("new_height: {:?}", this_height);
            }

            let start = Instant::now();
            this_mempool = rest_client.get_mempool().await?;
            let duration = start.elapsed();

            let delta = Self::create_delta_from_pools(&prev_mempool, &this_mempool);

            info!(
                "delta_height: {:?}, load_mempool_duration_millis: {:?}",
                delta.height(),
                duration.as_millis()
            );

            let mut filename = std::path::PathBuf::new();
            let day = now.format("%Y/%m/%d").to_string();
            let timestamp = now.timestamp();
            let suffix = if is_new_height { "full" } else { "delta" };
            filename.push("data");
            filename.extend(day.split('/'));
            filename.push(format!("{this_height}_{timestamp}_{suffix}.parquet"));
            Self::save_dataframe_delta_to_parquet(delta, filename);

            prev_height = this_height;
            prev_mempool = this_mempool;
            prev_timestamp = now.timestamp();
        }
    }

    fn save_dataframe_delta_to_parquet(mut delta: DataFrame, filename: std::path::PathBuf) {
        // make sure the folder exists
        std::fs::create_dir_all(filename.parent().unwrap()).unwrap();
        let file = std::fs::File::create(filename).unwrap();
        ParquetWriter::new(file)
            .with_compression(polars::prelude::ParquetCompression::Zstd(Default::default()))
            .with_statistics(true)
            .finish(&mut delta)
            .map_err(PolarsError::from)
            .unwrap();
    }

    fn create_delta_from_pools(prev_mempool: &Mempool, this_mempool: &Mempool) -> DataFrame {
        let (keys_removed, keys_added) = Self::delta_of_keys(prev_mempool, this_mempool);

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

        DataFrame::new(vec![
            Series::new("txid", &txid_values),
            Series::new("weight", &weight_values),
            Series::new("fee_sat", &fee_sat_values),
            Series::new("first_seen_at", first_seen_timestamp_values),
        ])
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
}
