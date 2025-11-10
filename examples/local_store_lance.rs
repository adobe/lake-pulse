use lake_pulse::{Analyzer, StorageConfig};

#[tokio::main]
async fn main() {
    let storage_config = StorageConfig::local().with_option("path", "./examples/data");
    let analyzer = Analyzer::builder(storage_config).build().await.unwrap();

    // Generate report for Lance table (dataset3 has deletions, appends, and multiple versions)
    // Run `cargo run --example create_dataset3` first to create the dataset
    let report = analyzer.analyze("lance_dataset.lance").await.unwrap();

    // Print pretty report
    println!("{}", report);
}
