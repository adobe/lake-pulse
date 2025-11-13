use lake_pulse::{Analyzer, StorageConfig};

#[tokio::main]
async fn main() {
    let storage_config = StorageConfig::local().with_option("path", "./examples/data");
    let analyzer = Analyzer::builder(storage_config).build().await.unwrap();

    // Generate report for Hudi table
    let report = analyzer.analyze("hudi_dataset").await.unwrap();

    // Print pretty report
    println!("{}", report);
}
