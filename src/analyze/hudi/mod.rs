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

//! Apache Hudi table analyzer.
//!
//! This module provides health analysis for Apache Hudi tables, supporting both
//! Copy-on-Write (CoW) and Merge-on-Read (MoR) table types.
//!
//! ## Hudi Table Structure
//!
//! Hudi tables store metadata in a `.hoodie/` directory containing:
//!
//! - **`hoodie.properties`**: Table configuration (table type, record key, partition fields, etc.)
//! - **Timeline files**: Commit history with format `[timestamp].[action_type]`
//!   - `.commit` / `.deltacommit`: Completed write operations
//!   - `.clean`: Cleanup operations
//!   - `.compaction`: Compaction operations (MoR tables)
//!   - `.rollback`: Rollback operations
//!   - `.savepoint`: Savepoint markers
//!
//! ## Metrics Extracted
//!
//! The Hudi analyzer extracts the following metrics:
//!
//! - **Schema Evolution**: Tracks schema changes across commits, stability score
//! - **Time Travel**: Snapshot count, age, retention efficiency
//! - **File Compaction**: Opportunity score, small file count, priority
//! - **Deletion Metrics**: Deleted rows count from update operations
//! - **Table Constraints**: Default constraint metrics for Hudi tables
//! - **Clustering**: File group information and clustering recommendations
//!
//! ## Table Types
//!
//! - **Copy-on-Write (CoW)**: Data files are rewritten on every update
//! - **Merge-on-Read (MoR)**: Updates are written to delta log files, merged on read
//!
//! ## Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use lake_pulse::storage::StorageProvider;
//! use lake_pulse::analyze::hudi::HudiAnalyzer;
//!
//! # async fn example(storage: Arc<dyn StorageProvider>) {
//! let analyzer = HudiAnalyzer::new(storage, 4);
//! // Use with the Analyzer for automatic format detection
//! # }
//! ```
//!
//! ## Feature Flag
//!
//! This module requires the `hudi` feature flag (part of the `experimental` feature set):
//!
//! ```toml
//! [dependencies]
//! lake-pulse = { version = "0.1", features = ["experimental"] }
//! ```

pub mod analyze;

pub use analyze::HudiAnalyzer;
