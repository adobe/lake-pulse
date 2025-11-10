use lake_pulse::{Analyzer, StorageConfig};

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let storage_config = StorageConfig::local().with_option("path", "./examples/data");
    let analyzer = Analyzer::builder(storage_config).build().await.unwrap();

    // Generate report
    let report = analyzer.analyze("delta_dataset").await.unwrap();

    // Print as JSON
    println!("{}", report.to_json(false).unwrap());
    // Print execution timeline as Gantt chart
    println!(
        "{}",
        report
            .timed_metrics
            .duration_collection_as_gantt(None)
            .unwrap()
    );
    // Print as Chrome tracing
    println!("{}", report.timed_metrics.to_chrome_tracing().unwrap());
}
