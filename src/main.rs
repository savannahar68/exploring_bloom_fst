use bloomfilter::Bloom;
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use memory_stats::memory_stats;
use std::io::BufRead;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_http::{Response, Server};
// Define the SegmentSummary structure
struct SegmentSummary {
    segment_number: u32,
    start_time: u64,
    end_time: u64,
    terms_bloom_filter: Arc<Mutex<Bloom<[u8]>>>,
}
fn main() {
    let server = Server::http("127.0.0.1:7878").expect("Failed to start server");
    let segment_summaries = Arc::new(Mutex::new(Vec::new()));
    let segment_counter = Arc::new(Mutex::new(0u32));
    println!("Server running on http://127.0.0.1:7878");

    for request in server.incoming_requests() {
        let summaries = Arc::clone(&segment_summaries);
        let counter = Arc::clone(&segment_counter);

        thread::spawn(move || {
            let mut cnt = counter.lock().unwrap();
            *cnt += 1;
            let segment_number = *cnt;
            drop(cnt); // Release the lock

            let start_time = current_time();
            let bloom = build_bloom().expect("Failed to build Bloom filter");
            let end_time = current_time();
            let summary = SegmentSummary {
                segment_number,
                start_time,
                end_time,
                terms_bloom_filter: Arc::new(Mutex::new(bloom)),
            };

            summaries.lock().unwrap().push(summary);
            if let Some(usage) = memory_stats() {
                println!(
                    "Segments Created: {}, Physical Memory: {} MB, Virtual Memory: {} MB",
                    segment_number,
                    usage.physical_mem / 1024 / 1024,
                    usage.virtual_mem / 1024 / 1024
                );
            } else {
                println!("Couldn't get the current memory usage :(");
            }

            let response = Response::from_string("Segment summary created").with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
            );
            request.respond(response).unwrap();
        });
    }
}

// Function to build the Bloom filter from the CSV file
fn build_bloom() -> Result<Bloom<[u8]>, Box<dyn std::error::Error>> {
    let num_items = 16_918_463;
    let fp_rate = 0.001;
    let mut bloom = Bloom::new_for_fp_rate(num_items, fp_rate);
    let file = std::fs::File::open("2011.csv")?;
    let reader = std::io::BufReader::new(file);
    for line_result in reader.lines() {
        let line = line_result?;
        bloom.set(line.as_bytes());
    }
    Ok(bloom)
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
