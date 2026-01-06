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

//! Apache Hudi table reader.
//!
//! This module provides a reader for extracting metrics from Apache Hudi tables.
//! It parses the Hudi timeline and metadata to provide comprehensive table statistics.
//!
//! ## Usage
//!
//! ```no_run
//! use lake_pulse::reader::hudi::reader::HudiReader;
//! use std::collections::HashMap;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let storage_options = HashMap::new();
//! let reader = HudiReader::open(
//!     "file:///path/to/hudi/table",
//!     &storage_options
//! ).await?;
//!
//! let metrics = reader.extract_metrics().await?;
//! println!("Table: {}", metrics.table_name);
//! println!("Commits: {}", metrics.timeline_info.total_commits);
//! # Ok(())
//! # }
//! ```
//!
//! ## Metrics
//!
//! The reader extracts:
//!
//! - **Table Metadata**: Table name, type (CoW/MoR), version, partition fields
//! - **Timeline Metrics**: Commit counts by type (commit, deltacommit, clean, etc.)
//! - **File Statistics**: Total files, size, average file size
//! - **Partition Metrics**: Partition count and distribution
//!
//! ## Feature Flag
//!
//! Requires the `hudi` feature:
//!
//! ```toml
//! [dependencies]
//! lake-pulse = { version = "0.1", features = ["hudi"] }
//! ```

pub mod metrics;
pub mod reader;
