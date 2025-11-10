use lake_pulse::{Analyzer, StorageConfig};

#[tokio::main]
async fn main() {
    let storage_config = StorageConfig::azure()
        .with_option("container", "my_container")
        .with_option("tenant_id", "the_tenant_id")
        .with_option("account_name", "my_account_name")
        .with_option("client_id", "client_id")
        .with_option("client_secret", "client_secret");
    let analyzer = Analyzer::builder(storage_config).build().await.unwrap();

    // Generate report
    let report = analyzer.analyze("my/table/path").await.unwrap();

    // Print pretty report
    println!("{}", report);
}
