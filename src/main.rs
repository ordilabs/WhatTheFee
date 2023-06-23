use reqwest::Error;
use serde::Deserialize;
use std::{collections::HashMap, time::Instant};

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
#[allow(dead_code)]
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
        println!("Current height: {}", chaininfo.blocks);
    } else {
        eprintln!("Error requesting chaininfo: {}", response.status());
    }

    let start = Instant::now();    
    let mempool = "http://127.0.0.1:8332/rest/mempool/contents.json";
    let response = reqwest::Client::new().get(mempool).send().await?;
    let duration = start.elapsed();

    if response.status().is_success() {
        // Deserialize the JSON response into a HashSet
        let mempool: HashMap<String, MempoolEntry> = response.json().await?;

        println!("Mempool contains {} transactions", mempool.len());
        let mut fee_sum = 0.;
        for (_txid, entry) in mempool.iter() {
            //println!("Transaction ID: {}", txid);
            fee_sum += entry.fees.base;
        }
        
        println!("Total fees: {:?}", fee_sum);
        println!("Time elapsed: {:?}", duration);
    } else {
        eprintln!("Error downloading mempool: {}", response.status());
    }

    Ok(())
}
