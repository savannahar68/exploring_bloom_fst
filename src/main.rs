use bloomfilter::Bloom;
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use memory_stats::memory_stats;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task;
use warp::Filter;

type SegmentSummaries = Arc<Mutex<Vec<SegmentSummary>>>;
type SegmentCounter = Arc<Mutex<u32>>;

// Define the SegmentSummary structure
#[derive(Debug)]
struct SegmentSummary {
    segment_number: u32,
    start_time: u64,
    end_time: u64,
    terms_bloom_filter: Bloom<[u8]>,
}

#[derive(Serialize)]
struct QueryResponse {
    exists: bool,
    segment_number: u32,
}

#[derive(Deserialize)]
struct QueryParams {
    id: u32,
    term: String,
}

#[tokio::main]
async fn main() {
    let segment_summaries: SegmentSummaries = Arc::new(Mutex::new(Vec::new()));
    let segment_counter: SegmentCounter = Arc::new(Mutex::new(0u32));

    let summaries_filter = warp::any().map(move || Arc::clone(&segment_summaries));
    let counter_filter = warp::any().map(move || Arc::clone(&segment_counter));
    // Define the /create_segment endpoint

    // Define the /create_segment endpoint
    let create_segment = warp::path!("create_segment")
        .and(warp::post())
        .and(summaries_filter.clone())
        .and(counter_filter.clone())
        .and_then(handle_create_segment);

    let query_segment = warp::path!("query_segment")
        .and(warp::get())
        .and(warp::query::<QueryParams>())
        .and(summaries_filter.clone())
        .and_then(handle_query_segment);

    let routes = create_segment.or(query_segment);
    println!("Server running on http://127.0.0.1:7878");
    warp::serve(routes).run(([127, 0, 0, 1], 7878)).await;
}

// Handler for creating a new SegmentSummary
async fn handle_create_segment(
    summaries: SegmentSummaries,
    counter: SegmentCounter,
) -> Result<impl warp::Reply, Infallible> {
    // Increment the segment counter safely
    let segment_number = {
        let mut cnt = counter.lock().unwrap();
        *cnt += 1;
        *cnt
    };
    let start_time = current_time();
    // Clone summaries Arc for the asynchronous task
    let summaries_clone = Arc::clone(&summaries);
    // Asynchronously build the Bloom filter to avoid blocking the server
    let bloom_result = task::spawn_blocking(move || build_bloom("2011.csv")).await;
    match bloom_result {
        Ok(bloom) => {
            let end_time = current_time();
            let summary = SegmentSummary {
                segment_number,
                start_time,
                end_time,
                terms_bloom_filter: bloom,
            };
            // Store the new SegmentSummary in the shared vector
            {
                let mut summaries_lock = summaries_clone.lock().unwrap();
                summaries_lock.push(summary);
                println!(
                    "Stored SegmentSummary number: {}. Total segments: {}",
                    segment_number,
                    summaries_lock.len()
                );
                if let Some(usage) = memory_stats() {
                    println!(
                        "Current physical memory usage: {} MB",
                        usage.physical_mem / 1024 / 1024
                    );
                    println!(
                        "Current virtual memory usage: {} MB",
                        usage.virtual_mem / 1024 / 1024
                    );
                } else {
                    println!("Couldn't get the current memory usage :(");
                }
            }

            Ok(warp::reply::with_status(
                format!("Segment summary {} created", segment_number),
                warp::http::StatusCode::OK,
            ))
        }
        Err(e) => {
            println!("Failed to build Bloom filter: {:?}", e);
            Ok(warp::reply::with_status(
                "Failed to create segment summary".to_string(),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}

async fn handle_query_segment(
    params: QueryParams,
    summaries: SegmentSummaries,
) -> Result<impl warp::Reply, Infallible> {
    let id = params.id;
    let term = params.term;
    let summaries_lock = summaries.lock().unwrap();
    println!("Query for term {} in segment {}", term, id);
    if let Some(segment) = summaries_lock.iter().find(|s| s.segment_number == id) {
        let exists = segment.terms_bloom_filter.check(term.as_bytes());
        let response = QueryResponse {
            exists,
            segment_number: id,
        };
        let json_response = serde_json::to_string(&response).unwrap();
        println!("Query for term {} in segment {}: {}", term, id, exists);
        Ok(warp::reply::with_status(
            json_response,
            warp::http::StatusCode::OK,
        ))
    } else {
        // Segment not found
        Ok(warp::reply::with_status(
            format!("Segment {} not found", id),
            warp::http::StatusCode::NOT_FOUND,
        ))
    }
}

fn build_bloom(file_path: &str) -> Bloom<[u8]> {
    let num_items = 16_918_463; // Number of expected items
    let fp_rate = 0.001;
    // Initialize the Bloom filter
    let mut bloom = Bloom::new_for_fp_rate(num_items, fp_rate);

    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);
    let start_time = std::time::Instant::now();
    let mut count = 0;
    // Insert each line into the Bloom filter
    for line_result in reader.lines() {
        let line = line_result.unwrap();
        bloom.set(line.as_bytes());
        count += 1;
        // // Optional: Log progress every 1 million lines
        // if count % 1_000_000 == 0 {
        //     println!("Inserted {} items into Bloom filter", count);
        // }
    }
    println!(
        "Built Bloom filter with {} items in {:?} seconds",
        count,
        start_time.elapsed()
    );
    bloom
}

// Function to get the current Unix timestamp in seconds
fn current_time() -> u64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    now.as_secs()
}

// fn build_fst() {
//     let mut wtr = io::BufWriter::new(File::create("map.fst").unwrap());
//     let mut build = MapBuilder::new(wtr).unwrap();

//     let f = File::open("2011.csv").expect("Unable to open file");
//     let f = BufReader::new(f);

//     let time = std::time::Instant::now();
//     let mut err_count = 0;
//     for line in f.lines() {
//         let line = line.expect("Unable to read line");
//         let res = build.insert(line, 1);
//         if res.is_err() {
//             err_count += 1;
//         }
//     }

//     println!("Time taken: {:?}", time.elapsed());
//     println!("Error count: {}", err_count);

//     build.finish().unwrap();
// }
