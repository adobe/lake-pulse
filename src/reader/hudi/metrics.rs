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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};

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

impl Default for HudiMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl HudiMetrics {
    /// Create a new HudiMetrics with default values
    pub fn new() -> Self {
        Self {
            table_type: String::new(),
            table_name: String::new(),
            metadata: TableMetadata::new(),
            table_properties: HashMap::new(),
            file_stats: FileStatistics::new(),
            partition_info: PartitionMetrics::new(),
            timeline_info: TimelineMetrics::new(),
        }
    }
}

impl Default for TableMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl TableMetadata {
    /// Create a new TableMetadata with default values
    pub fn new() -> Self {
        Self {
            name: String::new(),
            base_path: String::new(),
            schema_string: String::new(),
            field_count: 0,
            partition_columns: Vec::new(),
            created_time: None,
            format_provider: String::new(),
            format_options: HashMap::new(),
        }
    }
}

impl Default for FileStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl FileStatistics {
    /// Create a new FileStatistics with default values
    pub fn new() -> Self {
        Self {
            num_files: 0,
            total_size_bytes: 0,
            avg_file_size_bytes: 0.0,
            min_file_size_bytes: 0,
            max_file_size_bytes: 0,
            num_log_files: 0,
            total_log_size_bytes: 0,
        }
    }
}

impl Default for PartitionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionMetrics {
    /// Create a new PartitionMetrics with default values
    pub fn new() -> Self {
        Self {
            num_partition_columns: 0,
            num_partitions: 0,
            partition_paths: Vec::new(),
            largest_partition_size_bytes: 0,
            smallest_partition_size_bytes: 0,
            avg_partition_size_bytes: 0.0,
        }
    }
}

impl Default for TimelineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TimelineMetrics {
    /// Create a new TimelineMetrics with default values
    pub fn new() -> Self {
        Self {
            total_commits: 0,
            total_delta_commits: 0,
            total_compactions: 0,
            total_cleans: 0,
            total_rollbacks: 0,
            total_savepoints: 0,
            latest_commit_timestamp: None,
            earliest_commit_timestamp: None,
            pending_compactions: 0,
        }
    }
}

impl Display for HudiMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        writeln!(f)?;
        writeln!(f, " Hudi Specific Metrics")?;
        writeln!(f, "{}", "━".repeat(80))?;

        // Basic info
        writeln!(f, " {:<40} {:>32}", "Table Type", &self.table_type)?;
        if !self.table_name.is_empty() {
            writeln!(f, " {:<40} {:>32}", "Table Name", &self.table_name)?;
        }

        // Metadata
        writeln!(
            f,
            " {:<40} {:>32}",
            "Field Count", self.metadata.field_count
        )?;
        if !self.metadata.partition_columns.is_empty() {
            writeln!(
                f,
                " {:<40} {:>32}",
                "Partition Columns",
                self.metadata.partition_columns.join(", ")
            )?;
        }
        writeln!(
            f,
            " {:<40} {:>32}",
            "Format Provider", &self.metadata.format_provider
        )?;

        // Table Properties
        if !self.table_properties.is_empty() {
            self.table_properties
                .iter()
                .enumerate()
                .try_for_each(|(i, (k, v))| {
                    if i == 0 {
                        writeln!(
                            f,
                            " {:<20} {:>52}",
                            "Table Properties",
                            format!("{}: {}", k, v)
                        )
                    } else {
                        writeln!(f, " {:<20} {:>52}", "", format!("{}: {}", k, v))
                    }
                })?;
        }

        // File Statistics
        let file_stats = &self.file_stats;
        let total_size_mb = file_stats.total_size_bytes as f64 / (1024.0 * 1024.0);
        let avg_size_mb = file_stats.avg_file_size_bytes / (1024.0 * 1024.0);
        let min_size_kb = file_stats.min_file_size_bytes as f64 / 1024.0;
        let max_size_mb = file_stats.max_file_size_bytes as f64 / (1024.0 * 1024.0);

        writeln!(f, " File Statistics")?;
        writeln!(f, "   {:<38} {:>32}", "Files", file_stats.num_files)?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Size",
            format!("{:.2} MB", total_size_mb)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Avg Size",
            format!("{:.2} MB", avg_size_mb)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Min Size",
            format!("{:.2} KB", min_size_kb)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Max Size",
            format!("{:.2} MB", max_size_mb)
        )?;
        if file_stats.num_log_files > 0 {
            let log_size_mb = file_stats.total_log_size_bytes as f64 / (1024.0 * 1024.0);
            writeln!(f, "   {:<38} {:>32}", "Log Files", file_stats.num_log_files)?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Total Log Size",
                format!("{:.2} MB", log_size_mb)
            )?;
        }

        // Partition Info
        let partition_info = &self.partition_info;
        writeln!(f, " Partition Info")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Partition Columns", partition_info.num_partition_columns
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Number of Partitions", partition_info.num_partitions
        )?;
        if partition_info.num_partitions > 0 {
            let largest_mb = partition_info.largest_partition_size_bytes as f64 / (1024.0 * 1024.0);
            let smallest_kb = partition_info.smallest_partition_size_bytes as f64 / 1024.0;
            let avg_mb = partition_info.avg_partition_size_bytes / (1024.0 * 1024.0);
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Largest Partition",
                format!("{:.2} MB", largest_mb)
            )?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Smallest Partition",
                format!("{:.2} KB", smallest_kb)
            )?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Avg Partition Size",
                format!("{:.2} MB", avg_mb)
            )?;
        }

        // Timeline Info
        let timeline = &self.timeline_info;
        writeln!(f, " Timeline Info")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Commits", timeline.total_commits
        )?;
        if timeline.total_delta_commits > 0 {
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Delta Commits", timeline.total_delta_commits
            )?;
        }
        if timeline.total_compactions > 0 {
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Compactions", timeline.total_compactions
            )?;
        }
        if timeline.total_cleans > 0 {
            writeln!(f, "   {:<38} {:>32}", "Cleans", timeline.total_cleans)?;
        }
        if timeline.total_rollbacks > 0 {
            writeln!(f, "   {:<38} {:>32}", "Rollbacks", timeline.total_rollbacks)?;
        }
        if timeline.total_savepoints > 0 {
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Savepoints", timeline.total_savepoints
            )?;
        }
        if let Some(ref ts) = timeline.latest_commit_timestamp {
            writeln!(f, "   {:<38} {:>32}", "Latest Commit", ts)?;
        }
        if let Some(ref ts) = timeline.earliest_commit_timestamp {
            writeln!(f, "   {:<38} {:>32}", "Earliest Commit", ts)?;
        }
        if timeline.pending_compactions > 0 {
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Pending Compactions", timeline.pending_compactions
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== HudiMetrics Tests ====================

    #[test]
    fn test_hudi_metrics_new() {
        let metrics = HudiMetrics::new();

        assert_eq!(metrics.table_type, "");
        assert_eq!(metrics.table_name, "");
        assert_eq!(metrics.metadata.name, "");
        assert_eq!(metrics.metadata.base_path, "");
        assert_eq!(metrics.metadata.schema_string, "");
        assert_eq!(metrics.metadata.field_count, 0);
        assert!(metrics.metadata.partition_columns.is_empty());
        assert!(metrics.metadata.created_time.is_none());
        assert_eq!(metrics.metadata.format_provider, "");
        assert!(metrics.metadata.format_options.is_empty());
        assert!(metrics.table_properties.is_empty());
        assert_eq!(metrics.file_stats.num_files, 0);
        assert_eq!(metrics.file_stats.total_size_bytes, 0);
        assert_eq!(metrics.partition_info.num_partition_columns, 0);
        assert_eq!(metrics.timeline_info.total_commits, 0);
    }

    #[test]
    fn test_hudi_metrics_default() {
        let metrics = HudiMetrics::default();
        let new_metrics = HudiMetrics::new();

        assert_eq!(metrics.table_type, new_metrics.table_type);
        assert_eq!(metrics.table_name, new_metrics.table_name);
        assert_eq!(metrics.metadata.name, new_metrics.metadata.name);
    }

    #[test]
    fn test_hudi_metrics_clone() {
        let mut metrics = HudiMetrics::new();
        metrics.table_type = "COPY_ON_WRITE".to_string();
        metrics.table_name = "test_table".to_string();
        metrics.timeline_info.total_commits = 42;

        let cloned = metrics.clone();

        assert_eq!(cloned.table_type, "COPY_ON_WRITE");
        assert_eq!(cloned.table_name, "test_table");
        assert_eq!(cloned.timeline_info.total_commits, 42);
    }

    #[test]
    fn test_hudi_metrics_debug() {
        let metrics = HudiMetrics::new();
        let debug_str = format!("{:?}", metrics);

        assert!(debug_str.contains("HudiMetrics"));
        assert!(debug_str.contains("table_type"));
        assert!(debug_str.contains("table_name"));
    }

    #[test]
    fn test_hudi_metrics_with_data() {
        let mut metrics = HudiMetrics::new();
        metrics.table_type = "MERGE_ON_READ".to_string();
        metrics.table_name = "events".to_string();
        metrics.metadata.name = "events".to_string();
        metrics.metadata.base_path = "/data/events".to_string();
        metrics.metadata.field_count = 10;
        metrics.metadata.partition_columns = vec!["date".to_string(), "hour".to_string()];
        metrics.file_stats.num_files = 100;
        metrics.file_stats.total_size_bytes = 1_000_000;
        metrics.timeline_info.total_commits = 50;
        metrics.timeline_info.total_delta_commits = 25;

        assert_eq!(metrics.table_type, "MERGE_ON_READ");
        assert_eq!(metrics.metadata.partition_columns.len(), 2);
        assert_eq!(metrics.file_stats.num_files, 100);
        assert_eq!(metrics.timeline_info.total_commits, 50);
    }

    // ==================== TableMetadata Tests ====================

    #[test]
    fn test_table_metadata_new() {
        let metadata = TableMetadata::new();

        assert_eq!(metadata.name, "");
        assert_eq!(metadata.base_path, "");
        assert_eq!(metadata.schema_string, "");
        assert_eq!(metadata.field_count, 0);
        assert!(metadata.partition_columns.is_empty());
        assert!(metadata.created_time.is_none());
        assert_eq!(metadata.format_provider, "");
        assert!(metadata.format_options.is_empty());
    }

    #[test]
    fn test_table_metadata_default() {
        let metadata = TableMetadata::default();
        let new_metadata = TableMetadata::new();

        assert_eq!(metadata.name, new_metadata.name);
        assert_eq!(metadata.field_count, new_metadata.field_count);
    }

    #[test]
    fn test_table_metadata_complete() {
        let mut format_options = HashMap::new();
        format_options.insert("compression".to_string(), "snappy".to_string());

        let metadata = TableMetadata {
            name: "my_table".to_string(),
            base_path: "/data/my_table".to_string(),
            schema_string: r#"{"type":"struct","fields":[]}"#.to_string(),
            field_count: 5,
            partition_columns: vec!["year".to_string(), "month".to_string()],
            created_time: Some(1609459200000),
            format_provider: "parquet".to_string(),
            format_options,
        };

        assert_eq!(metadata.name, "my_table");
        assert_eq!(metadata.base_path, "/data/my_table");
        assert_eq!(metadata.field_count, 5);
        assert_eq!(metadata.partition_columns.len(), 2);
        assert_eq!(metadata.created_time, Some(1609459200000));
        assert_eq!(metadata.format_provider, "parquet");
        assert!(metadata.format_options.contains_key("compression"));
    }

    #[test]
    fn test_table_metadata_clone() {
        let metadata = TableMetadata {
            name: "test".to_string(),
            base_path: "/test".to_string(),
            schema_string: "{}".to_string(),
            field_count: 3,
            partition_columns: vec!["col1".to_string()],
            created_time: Some(1000),
            format_provider: "parquet".to_string(),
            format_options: HashMap::new(),
        };

        let cloned = metadata.clone();

        assert_eq!(cloned.name, "test");
        assert_eq!(cloned.field_count, 3);
        assert_eq!(cloned.partition_columns.len(), 1);
    }

    #[test]
    fn test_table_metadata_debug() {
        let metadata = TableMetadata::new();
        let debug_str = format!("{:?}", metadata);

        assert!(debug_str.contains("TableMetadata"));
        assert!(debug_str.contains("name"));
        assert!(debug_str.contains("base_path"));
    }

    // ==================== FileStatistics Tests ====================

    #[test]
    fn test_file_statistics_new() {
        let stats = FileStatistics::new();

        assert_eq!(stats.num_files, 0);
        assert_eq!(stats.total_size_bytes, 0);
        assert_eq!(stats.avg_file_size_bytes, 0.0);
        assert_eq!(stats.min_file_size_bytes, 0);
        assert_eq!(stats.max_file_size_bytes, 0);
        assert_eq!(stats.num_log_files, 0);
        assert_eq!(stats.total_log_size_bytes, 0);
    }

    #[test]
    fn test_file_statistics_default() {
        let stats = FileStatistics::default();
        let new_stats = FileStatistics::new();

        assert_eq!(stats.num_files, new_stats.num_files);
        assert_eq!(stats.total_size_bytes, new_stats.total_size_bytes);
    }

    #[test]
    fn test_file_statistics_with_data() {
        let stats = FileStatistics {
            num_files: 100,
            total_size_bytes: 10_000_000,
            avg_file_size_bytes: 100_000.0,
            min_file_size_bytes: 50_000,
            max_file_size_bytes: 200_000,
            num_log_files: 20,
            total_log_size_bytes: 500_000,
        };

        assert_eq!(stats.num_files, 100);
        assert_eq!(stats.total_size_bytes, 10_000_000);
        assert_eq!(stats.avg_file_size_bytes, 100_000.0);
        assert_eq!(stats.min_file_size_bytes, 50_000);
        assert_eq!(stats.max_file_size_bytes, 200_000);
        assert_eq!(stats.num_log_files, 20);
        assert_eq!(stats.total_log_size_bytes, 500_000);
    }

    #[test]
    fn test_file_statistics_clone() {
        let stats = FileStatistics {
            num_files: 50,
            total_size_bytes: 5_000_000,
            avg_file_size_bytes: 100_000.0,
            min_file_size_bytes: 10_000,
            max_file_size_bytes: 500_000,
            num_log_files: 10,
            total_log_size_bytes: 100_000,
        };

        let cloned = stats.clone();

        assert_eq!(cloned.num_files, 50);
        assert_eq!(cloned.total_size_bytes, 5_000_000);
        assert_eq!(cloned.num_log_files, 10);
    }

    #[test]
    fn test_file_statistics_debug() {
        let stats = FileStatistics::new();
        let debug_str = format!("{:?}", stats);

        assert!(debug_str.contains("FileStatistics"));
        assert!(debug_str.contains("num_files"));
        assert!(debug_str.contains("total_size_bytes"));
    }

    #[test]
    fn test_file_statistics_with_large_sizes() {
        let stats = FileStatistics {
            num_files: usize::MAX,
            total_size_bytes: u64::MAX,
            avg_file_size_bytes: f64::MAX,
            min_file_size_bytes: 0,
            max_file_size_bytes: u64::MAX,
            num_log_files: usize::MAX,
            total_log_size_bytes: u64::MAX,
        };

        assert_eq!(stats.num_files, usize::MAX);
        assert_eq!(stats.total_size_bytes, u64::MAX);
    }

    // ==================== PartitionMetrics Tests ====================

    #[test]
    fn test_partition_metrics_new() {
        let metrics = PartitionMetrics::new();

        assert_eq!(metrics.num_partition_columns, 0);
        assert_eq!(metrics.num_partitions, 0);
        assert!(metrics.partition_paths.is_empty());
        assert_eq!(metrics.largest_partition_size_bytes, 0);
        assert_eq!(metrics.smallest_partition_size_bytes, 0);
        assert_eq!(metrics.avg_partition_size_bytes, 0.0);
    }

    #[test]
    fn test_partition_metrics_default() {
        let metrics = PartitionMetrics::default();
        let new_metrics = PartitionMetrics::new();

        assert_eq!(
            metrics.num_partition_columns,
            new_metrics.num_partition_columns
        );
        assert_eq!(metrics.num_partitions, new_metrics.num_partitions);
    }

    #[test]
    fn test_partition_metrics_no_partitions() {
        let metrics = PartitionMetrics {
            num_partition_columns: 0,
            num_partitions: 0,
            partition_paths: vec![],
            largest_partition_size_bytes: 0,
            smallest_partition_size_bytes: 0,
            avg_partition_size_bytes: 0.0,
        };

        assert_eq!(metrics.num_partition_columns, 0);
        assert!(metrics.partition_paths.is_empty());
    }

    #[test]
    fn test_partition_metrics_with_partitions() {
        let metrics = PartitionMetrics {
            num_partition_columns: 2,
            num_partitions: 100,
            partition_paths: vec![
                "year=2024/month=01".to_string(),
                "year=2024/month=02".to_string(),
            ],
            largest_partition_size_bytes: 1_000_000,
            smallest_partition_size_bytes: 100_000,
            avg_partition_size_bytes: 500_000.0,
        };

        assert_eq!(metrics.num_partition_columns, 2);
        assert_eq!(metrics.num_partitions, 100);
        assert_eq!(metrics.partition_paths.len(), 2);
        assert_eq!(metrics.largest_partition_size_bytes, 1_000_000);
    }

    #[test]
    fn test_partition_metrics_clone() {
        let metrics = PartitionMetrics {
            num_partition_columns: 3,
            num_partitions: 50,
            partition_paths: vec!["p1".to_string(), "p2".to_string()],
            largest_partition_size_bytes: 500_000,
            smallest_partition_size_bytes: 50_000,
            avg_partition_size_bytes: 250_000.0,
        };

        let cloned = metrics.clone();

        assert_eq!(cloned.num_partition_columns, 3);
        assert_eq!(cloned.num_partitions, 50);
        assert_eq!(cloned.partition_paths.len(), 2);
    }

    #[test]
    fn test_partition_metrics_debug() {
        let metrics = PartitionMetrics::new();
        let debug_str = format!("{:?}", metrics);

        assert!(debug_str.contains("PartitionMetrics"));
        assert!(debug_str.contains("num_partition_columns"));
    }

    // ==================== TimelineMetrics Tests ====================

    #[test]
    fn test_timeline_metrics_new() {
        let metrics = TimelineMetrics::new();

        assert_eq!(metrics.total_commits, 0);
        assert_eq!(metrics.total_delta_commits, 0);
        assert_eq!(metrics.total_compactions, 0);
        assert_eq!(metrics.total_cleans, 0);
        assert_eq!(metrics.total_rollbacks, 0);
        assert_eq!(metrics.total_savepoints, 0);
        assert!(metrics.latest_commit_timestamp.is_none());
        assert!(metrics.earliest_commit_timestamp.is_none());
        assert_eq!(metrics.pending_compactions, 0);
    }

    #[test]
    fn test_timeline_metrics_default() {
        let metrics = TimelineMetrics::default();
        let new_metrics = TimelineMetrics::new();

        assert_eq!(metrics.total_commits, new_metrics.total_commits);
        assert_eq!(metrics.total_delta_commits, new_metrics.total_delta_commits);
    }

    #[test]
    fn test_timeline_metrics_cow_table() {
        let metrics = TimelineMetrics {
            total_commits: 100,
            total_delta_commits: 0, // CoW tables don't have delta commits
            total_compactions: 0,
            total_cleans: 10,
            total_rollbacks: 2,
            total_savepoints: 5,
            latest_commit_timestamp: Some("20251229120000000".to_string()),
            earliest_commit_timestamp: Some("20251201000000000".to_string()),
            pending_compactions: 0,
        };

        assert_eq!(metrics.total_commits, 100);
        assert_eq!(metrics.total_delta_commits, 0);
        assert!(metrics.latest_commit_timestamp.is_some());
    }

    #[test]
    fn test_timeline_metrics_mor_table() {
        let metrics = TimelineMetrics {
            total_commits: 50,
            total_delta_commits: 200, // MoR tables have delta commits
            total_compactions: 10,
            total_cleans: 5,
            total_rollbacks: 1,
            total_savepoints: 2,
            latest_commit_timestamp: Some("20251229120000000".to_string()),
            earliest_commit_timestamp: Some("20251101000000000".to_string()),
            pending_compactions: 3,
        };

        assert_eq!(metrics.total_commits, 50);
        assert_eq!(metrics.total_delta_commits, 200);
        assert_eq!(metrics.total_compactions, 10);
        assert_eq!(metrics.pending_compactions, 3);
    }

    #[test]
    fn test_timeline_metrics_clone() {
        let metrics = TimelineMetrics {
            total_commits: 25,
            total_delta_commits: 50,
            total_compactions: 5,
            total_cleans: 3,
            total_rollbacks: 1,
            total_savepoints: 2,
            latest_commit_timestamp: Some("20251229".to_string()),
            earliest_commit_timestamp: Some("20251201".to_string()),
            pending_compactions: 1,
        };

        let cloned = metrics.clone();

        assert_eq!(cloned.total_commits, 25);
        assert_eq!(cloned.total_delta_commits, 50);
        assert_eq!(cloned.latest_commit_timestamp, Some("20251229".to_string()));
    }

    #[test]
    fn test_timeline_metrics_debug() {
        let metrics = TimelineMetrics::new();
        let debug_str = format!("{:?}", metrics);

        assert!(debug_str.contains("TimelineMetrics"));
        assert!(debug_str.contains("total_commits"));
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn test_hudi_metrics_serialization() {
        let mut metrics = HudiMetrics::new();
        metrics.table_type = "COPY_ON_WRITE".to_string();
        metrics.table_name = "test_table".to_string();
        metrics.timeline_info.total_commits = 10;

        let json = serde_json::to_string(&metrics).unwrap();

        assert!(json.contains("COPY_ON_WRITE"));
        assert!(json.contains("test_table"));
        assert!(json.contains("\"total_commits\":10"));
    }

    #[test]
    fn test_hudi_metrics_deserialization() {
        let json = r#"{
            "table_type": "MERGE_ON_READ",
            "table_name": "events",
            "metadata": {
                "name": "events",
                "base_path": "/data/events",
                "schema_string": "{}",
                "field_count": 5,
                "partition_columns": ["date"],
                "created_time": 1609459200000,
                "format_provider": "parquet",
                "format_options": {}
            },
            "table_properties": {"key": "value"},
            "file_stats": {
                "num_files": 100,
                "total_size_bytes": 1000000,
                "avg_file_size_bytes": 10000.0,
                "min_file_size_bytes": 5000,
                "max_file_size_bytes": 20000,
                "num_log_files": 10,
                "total_log_size_bytes": 50000
            },
            "partition_info": {
                "num_partition_columns": 1,
                "num_partitions": 30,
                "partition_paths": [],
                "largest_partition_size_bytes": 50000,
                "smallest_partition_size_bytes": 10000,
                "avg_partition_size_bytes": 30000.0
            },
            "timeline_info": {
                "total_commits": 50,
                "total_delta_commits": 100,
                "total_compactions": 5,
                "total_cleans": 3,
                "total_rollbacks": 1,
                "total_savepoints": 2,
                "latest_commit_timestamp": "20251229",
                "earliest_commit_timestamp": "20251201",
                "pending_compactions": 2
            }
        }"#;

        let metrics: HudiMetrics = serde_json::from_str(json).unwrap();

        assert_eq!(metrics.table_type, "MERGE_ON_READ");
        assert_eq!(metrics.table_name, "events");
        assert_eq!(metrics.metadata.field_count, 5);
        assert_eq!(metrics.file_stats.num_files, 100);
        assert_eq!(metrics.timeline_info.total_commits, 50);
        assert_eq!(metrics.timeline_info.total_delta_commits, 100);
    }

    #[test]
    fn test_table_metadata_serialization() {
        let mut format_options = HashMap::new();
        format_options.insert("compression".to_string(), "snappy".to_string());

        let metadata = TableMetadata {
            name: "test".to_string(),
            base_path: "/data/test".to_string(),
            schema_string: "{}".to_string(),
            field_count: 3,
            partition_columns: vec!["col1".to_string()],
            created_time: Some(1000),
            format_provider: "parquet".to_string(),
            format_options,
        };

        let json = serde_json::to_string(&metadata).unwrap();

        assert!(json.contains("\"name\":\"test\""));
        assert!(json.contains("\"field_count\":3"));
        assert!(json.contains("snappy"));
    }

    #[test]
    fn test_file_statistics_serialization() {
        let stats = FileStatistics {
            num_files: 25,
            total_size_bytes: 1_000_000,
            avg_file_size_bytes: 40_000.0,
            min_file_size_bytes: 10_000,
            max_file_size_bytes: 100_000,
            num_log_files: 5,
            total_log_size_bytes: 50_000,
        };

        let json = serde_json::to_string(&stats).unwrap();

        assert!(json.contains("\"num_files\":25"));
        assert!(json.contains("\"total_size_bytes\":1000000"));
        assert!(json.contains("\"num_log_files\":5"));
    }

    #[test]
    fn test_partition_metrics_serialization() {
        let metrics = PartitionMetrics {
            num_partition_columns: 2,
            num_partitions: 50,
            partition_paths: vec!["p1".to_string()],
            largest_partition_size_bytes: 100_000,
            smallest_partition_size_bytes: 10_000,
            avg_partition_size_bytes: 50_000.0,
        };

        let json = serde_json::to_string(&metrics).unwrap();

        assert!(json.contains("\"num_partition_columns\":2"));
        assert!(json.contains("\"num_partitions\":50"));
    }

    #[test]
    fn test_timeline_metrics_serialization() {
        let metrics = TimelineMetrics {
            total_commits: 100,
            total_delta_commits: 50,
            total_compactions: 10,
            total_cleans: 5,
            total_rollbacks: 2,
            total_savepoints: 3,
            latest_commit_timestamp: Some("20251229".to_string()),
            earliest_commit_timestamp: Some("20251201".to_string()),
            pending_compactions: 1,
        };

        let json = serde_json::to_string(&metrics).unwrap();

        assert!(json.contains("\"total_commits\":100"));
        assert!(json.contains("\"total_delta_commits\":50"));
        assert!(json.contains("\"pending_compactions\":1"));
    }

    #[test]
    fn test_hudi_metrics_round_trip_serialization() {
        let mut original = HudiMetrics::new();
        original.table_type = "COPY_ON_WRITE".to_string();
        original.table_name = "round_trip_test".to_string();
        original.metadata.name = "round_trip_test".to_string();
        original.metadata.field_count = 10;
        original.file_stats.num_files = 50;
        original.timeline_info.total_commits = 25;
        original
            .table_properties
            .insert("key".to_string(), "value".to_string());

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: HudiMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.table_type, original.table_type);
        assert_eq!(deserialized.table_name, original.table_name);
        assert_eq!(
            deserialized.metadata.field_count,
            original.metadata.field_count
        );
        assert_eq!(
            deserialized.file_stats.num_files,
            original.file_stats.num_files
        );
        assert_eq!(
            deserialized.timeline_info.total_commits,
            original.timeline_info.total_commits
        );
        assert_eq!(
            deserialized.table_properties.get("key"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn test_hudi_metrics_with_table_properties() {
        let mut metrics = HudiMetrics::new();
        metrics
            .table_properties
            .insert("hoodie.table.name".to_string(), "test".to_string());
        metrics
            .table_properties
            .insert("hoodie.table.type".to_string(), "COPY_ON_WRITE".to_string());
        metrics.table_properties.insert(
            "hoodie.archivelog.folder".to_string(),
            "archived".to_string(),
        );

        assert_eq!(metrics.table_properties.len(), 3);
        assert_eq!(
            metrics.table_properties.get("hoodie.table.name"),
            Some(&"test".to_string())
        );
        assert_eq!(
            metrics.table_properties.get("hoodie.table.type"),
            Some(&"COPY_ON_WRITE".to_string())
        );
    }

    #[test]
    fn test_hudi_metrics_display() {
        let mut metrics = HudiMetrics::new();
        metrics.table_type = "COPY_ON_WRITE".to_string();
        metrics.table_name = "test_hudi_table".to_string();
        metrics.metadata.name = "test_hudi_table".to_string();
        metrics.metadata.base_path = "/path/to/table".to_string();
        metrics.metadata.field_count = 10;
        metrics.metadata.partition_columns = vec!["date".to_string()];
        metrics.metadata.created_time = Some(1700000000000);
        metrics
            .table_properties
            .insert("hoodie.table.name".to_string(), "test".to_string());
        metrics.file_stats = FileStatistics {
            num_files: 100,
            total_size_bytes: 1024 * 1024 * 500,
            avg_file_size_bytes: 1024.0 * 1024.0 * 5.0,
            min_file_size_bytes: 1024,
            max_file_size_bytes: 1024 * 1024 * 10,
            num_log_files: 20,
            total_log_size_bytes: 1024 * 1024 * 50,
        };
        metrics.partition_info = PartitionMetrics {
            num_partition_columns: 1,
            num_partitions: 30,
            partition_paths: vec!["date=2023-11-01".to_string()],
            largest_partition_size_bytes: 1024 * 1024 * 100,
            smallest_partition_size_bytes: 1024 * 1024,
            avg_partition_size_bytes: 1024.0 * 1024.0 * 50.0,
        };
        metrics.timeline_info = TimelineMetrics {
            total_commits: 100,
            total_delta_commits: 50,
            total_compactions: 10,
            total_cleans: 5,
            total_rollbacks: 2,
            total_savepoints: 1,
            latest_commit_timestamp: Some("20231114221320".to_string()),
            earliest_commit_timestamp: Some("20231101000000".to_string()),
            pending_compactions: 0,
        };

        let display = format!("{}", metrics);

        assert!(display.contains("Hudi Specific Metrics"));
        assert!(display.contains("Table Type"));
        assert!(display.contains("COPY_ON_WRITE"));
        assert!(display.contains("test_hudi_table"));
        assert!(display.contains("File Statistics"));
        assert!(display.contains("100"));
        assert!(display.contains("Log Files"));
        assert!(display.contains("Partition Info"));
        assert!(display.contains("Timeline Info"));
        assert!(display.contains("Total Commits"));
        assert!(display.contains("Table Properties"));
    }

    #[test]
    fn test_hudi_metrics_display_minimal() {
        let metrics = HudiMetrics::new();
        let display = format!("{}", metrics);

        assert!(display.contains("Hudi Specific Metrics"));
        assert!(display.contains("Table Type"));
        assert!(display.contains("File Statistics"));
    }

    #[test]
    fn test_hudi_metrics_display_mor_table() {
        let mut metrics = HudiMetrics::new();
        metrics.table_type = "MERGE_ON_READ".to_string();
        metrics.file_stats.num_log_files = 50;
        metrics.file_stats.total_log_size_bytes = 1024 * 1024 * 100;

        let display = format!("{}", metrics);

        assert!(display.contains("MERGE_ON_READ"));
        assert!(display.contains("Log Files"));
        assert!(display.contains("Total Log Size"));
    }

    #[test]
    fn test_hudi_metrics_display_timeline_details() {
        let mut metrics = HudiMetrics::new();
        metrics.timeline_info.total_commits = 100;
        metrics.timeline_info.total_delta_commits = 50;
        metrics.timeline_info.total_compactions = 10;
        metrics.timeline_info.total_cleans = 5;
        metrics.timeline_info.total_rollbacks = 2;
        metrics.timeline_info.total_savepoints = 1;

        let display = format!("{}", metrics);

        assert!(display.contains("Total Commits"));
        assert!(display.contains("Delta Commits"));
        assert!(display.contains("Compactions"));
        assert!(display.contains("Cleans"));
        assert!(display.contains("Rollbacks"));
        assert!(display.contains("Savepoints"));
    }
}
