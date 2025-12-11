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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Apache Hudi specific metrics extracted from table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HudiMetrics {
    /// Table type (COPY_ON_WRITE or MERGE_ON_READ)
    pub table_type: String,

    /// Table name
    pub table_name: String,

    /// Table metadata
    pub metadata: TableMetadata,

    /// Table configuration/properties
    pub table_properties: HashMap<String, String>,

    /// File statistics
    pub file_stats: FileStatistics,

    /// Partition information
    pub partition_info: PartitionMetrics,

    /// Timeline information
    pub timeline_info: TimelineMetrics,
}

/// Hudi table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table name
    pub name: String,

    /// Table base path
    pub base_path: String,

    /// Schema as JSON string
    pub schema_string: String,

    /// Number of fields in the schema
    pub field_count: usize,

    /// Partition columns
    pub partition_columns: Vec<String>,

    /// Table creation time (milliseconds since epoch)
    pub created_time: Option<i64>,

    /// Table format provider (always "parquet" for Hudi)
    pub format_provider: String,

    /// Table format options
    pub format_options: HashMap<String, String>,
}

/// File-level statistics from Hudi table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatistics {
    /// Total number of active files
    pub num_files: usize,

    /// Total size of active files in bytes
    pub total_size_bytes: u64,

    /// Average file size in bytes
    pub avg_file_size_bytes: f64,

    /// Minimum file size in bytes
    pub min_file_size_bytes: u64,

    /// Maximum file size in bytes
    pub max_file_size_bytes: u64,

    /// Number of log files (for MOR tables)
    pub num_log_files: usize,

    /// Total size of log files in bytes
    pub total_log_size_bytes: u64,
}

/// Partition-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetrics {
    /// Number of partition columns
    pub num_partition_columns: usize,

    /// Number of partitions
    pub num_partitions: usize,

    /// Partition paths
    pub partition_paths: Vec<String>,

    /// Largest partition size in bytes
    pub largest_partition_size_bytes: u64,

    /// Smallest partition size in bytes
    pub smallest_partition_size_bytes: u64,

    /// Average partition size in bytes
    pub avg_partition_size_bytes: f64,
}

/// Timeline metrics for Hudi table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineMetrics {
    /// Total number of commits
    pub total_commits: usize,

    /// Total number of delta commits (for MOR tables)
    pub total_delta_commits: usize,

    /// Total number of compactions
    pub total_compactions: usize,

    /// Total number of cleans
    pub total_cleans: usize,

    /// Total number of rollbacks
    pub total_rollbacks: usize,

    /// Total number of savepoints
    pub total_savepoints: usize,

    /// Latest commit timestamp
    pub latest_commit_timestamp: Option<String>,

    /// Earliest commit timestamp
    pub earliest_commit_timestamp: Option<String>,

    /// Number of pending compactions
    pub pending_compactions: usize,
}
