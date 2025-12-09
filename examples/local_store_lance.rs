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
    let storage_config = StorageConfig::local().with_option("path", "./examples/data");
    let analyzer = Analyzer::builder(storage_config).build().await.unwrap();

    // Generate report for Lance table (dataset3 has deletions, appends, and multiple versions)
    // Run `cargo run --example create_dataset3` first to create the dataset
    let report = analyzer.analyze("lance_dataset.lance").await.unwrap();

    // Print pretty report
    println!("{}", report);
}
