// Copyright 2022 Adobe. All rights reserved.
// This file is licensed to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
// OF ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

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
    let analyzer = Analyzer::builder(storage_config)
        .with_parallelism(5)
        .build()
        .await
        .unwrap();

    // Generate report
    let report = analyzer
        .analyze("delta_dataset_with_features")
        .await
        .unwrap();

    // Print as JSON
    println!("{}", report);
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
