use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

mod analyze;
mod delta;
mod storage;
mod util;

use analyze::Analyzer;
use storage::StorageConfig;

#[tokio::main(flavor = "multi_thread", worker_threads = 40)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting Lake Health");

    let container = "";
    let tenant_id = "";

    let account_name = "";
    let client_id = "";
    let client_secret = "";
    let path = "";

    let general_start_time = SystemTime::now();

    let storage_config = StorageConfig::azure()
        .with_option("container", container)
        .with_option("tenant_id", tenant_id)
        .with_option("account_name", account_name)
        .with_option("client_id", client_id)
        .with_option("client_secret", client_secret);
    let storage_config_new_dur = general_start_time.elapsed()?;

    let analyzer_new_start = SystemTime::now();
    let analyzer = Analyzer::builder(storage_config)
        .with_parallelism(20)
        .build()
        .await?;
    let analyzer_new_dur = analyzer_new_start.elapsed()?;

    let analyze_func_start = SystemTime::now();
    let mut report = analyzer.analyze(path).await?;
    let analyze_func_dur = analyze_func_start.elapsed()?;

    report.timed_metrics.duration_collection.push_front((
        "analyzer_new_dur".to_string(),
        analyzer_new_start.duration_since(UNIX_EPOCH)?.as_millis(),
        analyzer_new_dur.as_millis(),
    ));
    report.timed_metrics.duration_collection.push_front((
        "storage_config_new_dur".to_string(),
        general_start_time.duration_since(UNIX_EPOCH)?.as_millis(),
        storage_config_new_dur.as_millis(),
    ));
    report.timed_metrics.duration_collection.push_back((
        "analyze_total_dur".to_string(),
        analyze_func_start.duration_since(UNIX_EPOCH)?.as_millis(),
        analyze_func_dur.as_millis(),
    ));
    report.timed_metrics.duration_collection.push_back((
        "total_dur".to_string(),
        general_start_time.duration_since(UNIX_EPOCH)?.as_millis(),
        general_start_time.elapsed()?.as_millis(),
    ));

    let mut times_human_readable: String = "".to_string();
    report
        .clone()
        .timed_metrics
        .duration_collection
        .iter()
        .for_each(|(k, _, dur)| {
            times_human_readable = format!("{}\n{}: {}ms", times_human_readable, k, dur);
        });
    
    // Write JSON to file
    let out_file_name = format!("{}.json", path.replace("/", "_"));
    let mut f = File::create(out_file_name)?;
    f.write_all(report.to_json(false)?.as_bytes())?;

    println!("{}", report.clone());
    // println!();
    // println!("{}", times_human_readable);
    // println!();
    // println!("{}", report.timed_metrics.to_chrome_tracing()?);
    println!(
        "{}",
        report.timed_metrics.duration_collection_as_gantt(None)?
    );

    Ok(())
}
