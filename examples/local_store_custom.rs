// Copyright 2025 Adobe. All rights reserved.
// This file is licensed to you under the Apache License,
// Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
// or the MIT license (http://opensource.org/licenses/MIT),
// at your option.

// Unless required by applicable law or agreed to in writing,
// this software is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR REPRESENTATIONS OF ANY KIND, either express or
// implied. See the LICENSE-MIT and LICENSE-APACHE files for the
// specific language governing permissions and limitations under
// each license.

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
