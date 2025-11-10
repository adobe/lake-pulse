use lake_pulse::{Analyzer, StorageConfig};

#[tokio::main]
async fn main() {
    let storage_config = StorageConfig::aws()
        .with_option("bucket", "my-bucket-1234")
        .with_option("region", "us-east-1")
        .with_option("access_key_id", "the_access_key_id")
        .with_option("secret_access_key", "the_secret_access_key")
        .with_option("session_token", "session_token_if_needed");
    let analyzer = Analyzer::builder(storage_config).build().await.unwrap();

    // Generate report
    let report = analyzer.analyze("my/table/path").await.unwrap();

    // Print pretty report
    println!("{}", report);
}
