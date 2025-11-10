use lake_health::{Analyzer, StorageConfig};

#[tokio::main]
async fn main() {
    let storage_config = StorageConfig::local().with_option("path", "./examples/data");
    let analyzer = Analyzer::builder(storage_config).build().await.unwrap();

    // Generate report
    let report = analyzer.analyze("delta_dataset").await.unwrap();

    // Print pretty report
    println!("{}", report);
}
