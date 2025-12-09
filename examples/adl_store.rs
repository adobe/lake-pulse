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
