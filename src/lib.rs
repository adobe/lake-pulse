// Copyright 2025 Adobe. All rights reserved.
// This file is licensed to you under the Apache License,
// Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
// or the MIT license (http://opensource.org/licenses/MIT),
// at your option.
//
// Unless required by applicable law or agreed to in writing,
// this software is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR REPRESENTATIONS OF ANY KIND, either express or
// implied. See the LICENSE-MIT and LICENSE-APACHE files for the
// specific language governing permissions and limitations under
// each license.

//! # Lake Pulse
//!
//! A Rust library for analyzing data lake table health across multiple formats and storage providers.
//!
//! Lake Pulse provides comprehensive health analysis for data lake tables including Delta Lake,
//! Apache Iceberg, Apache Hudi, Lance, and Apache Paimon. It supports multiple cloud storage providers
//! (AWS S3, Azure Data Lake, GCS) and local filesystems.
//!
//! ## Features
//!
//! - **Multi-format support**: Delta Lake, Apache Iceberg, Apache Hudi, Lance, Apache Paimon
//! - **Cloud storage**: AWS S3, Azure Data Lake Storage, Google Cloud Storage, HDFS, HTTP/WebDAV, Local filesystem
//! - **Health metrics**: File size distribution, partition analysis, data skew detection
//! - **Advanced analysis**: Schema evolution, time travel metrics, deletion vectors, compaction opportunities
//! - **Performance tracking**: Built-in timing metrics with Gantt chart visualization
//!
//! ## Quick Start
//!
//! ### Local Filesystem Example
//!
//! ```rust,no_run
//! use lake_pulse::{Analyzer, StorageConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! // Configure storage for local filesystem
//! let config = StorageConfig::local()
//!     .with_option("path", "./examples/data");
//!
//! // Create analyzer
//! let analyzer = Analyzer::builder(config)
//!     .build()
//!     .await?;
//!
//! // Analyze a table (auto-detects format: Delta, Iceberg, Hudi, Lance, or Paimon)
//! let report = analyzer.analyze("delta_dataset").await?;
//!
//! // Print the health report
//! println!("{}", report);
//! # Ok(())
//! # }
//! ```
//!
//! ### AWS S3 Example
//!
//! ```rust,no_run
//! use lake_pulse::{Analyzer, StorageConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let config = StorageConfig::aws()
//!     .with_option("bucket", "my-bucket")
//!     .with_option("region", "us-east-1")
//!     .with_option("access_key_id", "ACCESS_KEY")
//!     .with_option("secret_access_key", "SECRET_KEY");
//!
//! let analyzer = Analyzer::builder(config).build().await?;
//! let report = analyzer.analyze("my/table/path").await?;
//! println!("{}", report);
//! # Ok(())
//! # }
//! ```
//!
//! ### Azure Data Lake Example
//!
//! ```rust,no_run
//! use lake_pulse::{Analyzer, StorageConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let config = StorageConfig::azure()
//!     .with_option("container", "my-container")
//!     .with_option("account_name", "my-account")
//!     .with_option("tenant_id", "TENANT_ID")
//!     .with_option("client_id", "CLIENT_ID")
//!     .with_option("client_secret", "CLIENT_SECRET");
//!
//! let analyzer = Analyzer::builder(config).build().await?;
//! let report = analyzer.analyze("my/table/path").await?;
//! println!("{}", report);
//! # Ok(())
//! # }
//! ```
//!
//! For more examples, see the [`examples/`](https://github.com/adobe/lake-pulse/tree/main/examples) directory.
//!
//! ## Modules
//!
//! - [`analyze`] - Core analysis functionality and table analyzers
//! - [`storage`] - Cloud storage abstraction layer
//! - [`reader`] - Table format readers (Delta, Iceberg, Hudi, Lance)
//! - [`util`] - Utility functions and helpers

pub mod analyze;
pub mod reader;
pub mod storage;
pub mod util;

// Re-export commonly used types
pub use analyze::metrics::{HealthMetrics, HealthReport};
pub use analyze::Analyzer;
pub use storage::StorageConfig;
