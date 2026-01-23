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
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Apache Paimon specific metrics extracted from table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaimonMetrics {
    /// Table name
    pub table_name: String,

    /// Table metadata
    pub metadata: TableMetadata,

    /// Snapshot information
    pub snapshot_info: SnapshotMetrics,

    /// Schema information
    pub schema_info: SchemaMetrics,

    /// File statistics
    pub file_stats: FileStatistics,

    /// Partition information
    pub partition_info: PartitionMetrics,
}

/// Paimon table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table UUID
    pub uuid: String,

    /// Table format version
    pub format_version: i32,

    /// Number of fields in the schema
    pub field_count: usize,

    /// Primary key columns
    pub primary_keys: Vec<String>,

    /// Partition columns
    pub partition_keys: Vec<String>,

    /// Bucket count (-1 for dynamic bucketing)
    pub bucket_count: i32,

    /// Table comment
    pub comment: Option<String>,
}

/// Snapshot-level metrics for Paimon table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetrics {
    /// Current snapshot ID
    pub current_snapshot_id: i64,

    /// Total number of snapshots
    pub total_snapshots: usize,

    /// Earliest snapshot ID
    pub earliest_snapshot_id: Option<i64>,

    /// Latest snapshot commit timestamp (milliseconds since epoch)
    pub latest_commit_time: Option<i64>,

    /// Earliest snapshot commit timestamp (milliseconds since epoch)
    pub earliest_commit_time: Option<i64>,

    /// Commit kind of latest snapshot (APPEND, COMPACT, OVERWRITE)
    pub latest_commit_kind: Option<String>,

    /// Total number of records in current snapshot
    pub total_record_count: i64,
}

/// Schema-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetrics {
    /// Current schema ID
    pub current_schema_id: i64,

    /// Total number of schema versions
    pub total_schema_versions: usize,

    /// Schema as JSON string
    pub schema_string: String,
}

/// File-level statistics from Paimon table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatistics {
    /// Total number of data files
    pub num_data_files: usize,

    /// Total size of data files in bytes
    pub total_data_size_bytes: u64,

    /// Average data file size in bytes
    pub avg_data_file_size_bytes: f64,

    /// Minimum data file size in bytes
    pub min_data_file_size_bytes: u64,

    /// Maximum data file size in bytes
    pub max_data_file_size_bytes: u64,

    /// Number of manifest files
    pub num_manifest_files: usize,

    /// Total size of manifest files in bytes
    pub total_manifest_size_bytes: u64,
}

/// Partition-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetrics {
    /// Number of partition columns
    pub num_partition_keys: usize,

    /// Number of partitions
    pub num_partitions: usize,

    /// Number of buckets
    pub num_buckets: usize,

    /// Largest partition size in bytes
    pub largest_partition_size_bytes: u64,

    /// Smallest partition size in bytes
    pub smallest_partition_size_bytes: u64,

    /// Average partition size in bytes
    pub avg_partition_size_bytes: f64,
}

impl Default for PaimonMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PaimonMetrics {
    /// Create a new PaimonMetrics with default values
    pub fn new() -> Self {
        Self {
            table_name: String::new(),
            metadata: TableMetadata::new(),
            snapshot_info: SnapshotMetrics::new(),
            schema_info: SchemaMetrics::new(),
            file_stats: FileStatistics::new(),
            partition_info: PartitionMetrics::new(),
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
            uuid: String::new(),
            format_version: 0,
            field_count: 0,
            primary_keys: Vec::new(),
            partition_keys: Vec::new(),
            bucket_count: -1,
            comment: None,
        }
    }
}

impl Default for SnapshotMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotMetrics {
    /// Create a new SnapshotMetrics with default values
    pub fn new() -> Self {
        Self {
            current_snapshot_id: 0,
            total_snapshots: 0,
            earliest_snapshot_id: None,
            latest_commit_time: None,
            earliest_commit_time: None,
            latest_commit_kind: None,
            total_record_count: 0,
        }
    }
}

impl Default for SchemaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaMetrics {
    /// Create a new SchemaMetrics with default values
    pub fn new() -> Self {
        Self {
            current_schema_id: 0,
            total_schema_versions: 0,
            schema_string: String::new(),
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
            num_data_files: 0,
            total_data_size_bytes: 0,
            avg_data_file_size_bytes: 0.0,
            min_data_file_size_bytes: 0,
            max_data_file_size_bytes: 0,
            num_manifest_files: 0,
            total_manifest_size_bytes: 0,
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
            num_partition_keys: 0,
            num_partitions: 0,
            num_buckets: 0,
            largest_partition_size_bytes: 0,
            smallest_partition_size_bytes: 0,
            avg_partition_size_bytes: 0.0,
        }
    }
}

impl Display for PaimonMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        writeln!(f)?;
        writeln!(f, " Paimon Specific Metrics")?;
        writeln!(f, "{}", "━".repeat(80))?;

        // Basic info
        if !self.table_name.is_empty() {
            writeln!(f, " {:<40} {:>32}", "Table Name", &self.table_name)?;
        }

        // Metadata
        if !self.metadata.uuid.is_empty() {
            writeln!(f, " {:<40} {:>32}", "Table UUID", &self.metadata.uuid)?;
        }
        writeln!(
            f,
            " {:<40} {:>32}",
            "Format Version", self.metadata.format_version
        )?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Field Count", self.metadata.field_count
        )?;
        if !self.metadata.primary_keys.is_empty() {
            writeln!(
                f,
                " {:<40} {:>32}",
                "Primary Keys",
                self.metadata.primary_keys.join(", ")
            )?;
        }
        if !self.metadata.partition_keys.is_empty() {
            writeln!(
                f,
                " {:<40} {:>32}",
                "Partition Keys",
                self.metadata.partition_keys.join(", ")
            )?;
        }
        writeln!(
            f,
            " {:<40} {:>32}",
            "Bucket Count", self.metadata.bucket_count
        )?;

        // Snapshot Info
        writeln!(f, " Snapshot Info")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Current Snapshot ID", self.snapshot_info.current_snapshot_id
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Snapshots", self.snapshot_info.total_snapshots
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Records", self.snapshot_info.total_record_count
        )?;
        if let Some(ref kind) = self.snapshot_info.latest_commit_kind {
            writeln!(f, "   {:<38} {:>32}", "Latest Commit Kind", kind)?;
        }

        // Schema Info
        writeln!(f, " Schema Info")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Current Schema ID", self.schema_info.current_schema_id
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Schema Versions", self.schema_info.total_schema_versions
        )?;

        // File Statistics
        let file_stats = &self.file_stats;
        let total_size_mb = file_stats.total_data_size_bytes as f64 / (1024.0 * 1024.0);
        let avg_size_mb = file_stats.avg_data_file_size_bytes / (1024.0 * 1024.0);

        writeln!(f, " File Statistics")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Data Files", file_stats.num_data_files
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Data Size",
            format!("{:.2} MB", total_size_mb)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Avg Data File Size",
            format!("{:.2} MB", avg_size_mb)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Manifest Files", file_stats.num_manifest_files
        )?;

        // Partition Info
        let partition_info = &self.partition_info;
        writeln!(f, " Partition Info")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Partition Keys", partition_info.num_partition_keys
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Number of Partitions", partition_info.num_partitions
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Number of Buckets", partition_info.num_buckets
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Default trait tests ==========

    #[test]
    fn test_paimon_metrics_default() {
        let metrics = PaimonMetrics::default();
        assert!(metrics.table_name.is_empty());
        assert_eq!(metrics.metadata.format_version, 0);
    }

    #[test]
    fn test_table_metadata_default() {
        let metadata = TableMetadata::default();
        assert!(metadata.uuid.is_empty());
        assert_eq!(metadata.bucket_count, -1);
    }

    #[test]
    fn test_snapshot_metrics_default() {
        let metrics = SnapshotMetrics::default();
        assert_eq!(metrics.current_snapshot_id, 0);
        assert!(metrics.earliest_snapshot_id.is_none());
    }

    #[test]
    fn test_schema_metrics_default() {
        let metrics = SchemaMetrics::default();
        assert_eq!(metrics.current_schema_id, 0);
        assert!(metrics.schema_string.is_empty());
    }

    #[test]
    fn test_file_statistics_default() {
        let stats = FileStatistics::default();
        assert_eq!(stats.num_data_files, 0);
        assert_eq!(stats.total_manifest_size_bytes, 0);
    }

    #[test]
    fn test_partition_metrics_default() {
        let metrics = PartitionMetrics::default();
        assert_eq!(metrics.num_partition_keys, 0);
        assert_eq!(metrics.avg_partition_size_bytes, 0.0);
    }

    // ========== new() tests ==========

    #[test]
    fn test_paimon_metrics_new() {
        let metrics = PaimonMetrics::new();
        assert!(metrics.table_name.is_empty());
        assert_eq!(metrics.metadata.format_version, 0);
        assert_eq!(metrics.snapshot_info.current_snapshot_id, 0);
        assert_eq!(metrics.schema_info.current_schema_id, 0);
        assert_eq!(metrics.file_stats.num_data_files, 0);
        assert_eq!(metrics.partition_info.num_partitions, 0);
    }

    #[test]
    fn test_table_metadata_new() {
        let metadata = TableMetadata::new();
        assert!(metadata.uuid.is_empty());
        assert_eq!(metadata.format_version, 0);
        assert_eq!(metadata.field_count, 0);
        assert!(metadata.primary_keys.is_empty());
        assert!(metadata.partition_keys.is_empty());
        assert_eq!(metadata.bucket_count, -1);
        assert!(metadata.comment.is_none());
    }

    #[test]
    fn test_snapshot_metrics_new() {
        let metrics = SnapshotMetrics::new();
        assert_eq!(metrics.current_snapshot_id, 0);
        assert_eq!(metrics.total_snapshots, 0);
        assert!(metrics.earliest_snapshot_id.is_none());
        assert!(metrics.latest_commit_time.is_none());
        assert!(metrics.earliest_commit_time.is_none());
        assert!(metrics.latest_commit_kind.is_none());
        assert_eq!(metrics.total_record_count, 0);
    }

    #[test]
    fn test_schema_metrics_new() {
        let metrics = SchemaMetrics::new();
        assert_eq!(metrics.current_schema_id, 0);
        assert_eq!(metrics.total_schema_versions, 0);
        assert!(metrics.schema_string.is_empty());
    }

    #[test]
    fn test_file_statistics_new() {
        let stats = FileStatistics::new();
        assert_eq!(stats.num_data_files, 0);
        assert_eq!(stats.total_data_size_bytes, 0);
        assert_eq!(stats.avg_data_file_size_bytes, 0.0);
        assert_eq!(stats.min_data_file_size_bytes, 0);
        assert_eq!(stats.max_data_file_size_bytes, 0);
        assert_eq!(stats.num_manifest_files, 0);
        assert_eq!(stats.total_manifest_size_bytes, 0);
    }

    #[test]
    fn test_partition_metrics_new() {
        let metrics = PartitionMetrics::new();
        assert_eq!(metrics.num_partition_keys, 0);
        assert_eq!(metrics.num_partitions, 0);
        assert_eq!(metrics.num_buckets, 0);
        assert_eq!(metrics.largest_partition_size_bytes, 0);
        assert_eq!(metrics.smallest_partition_size_bytes, 0);
        assert_eq!(metrics.avg_partition_size_bytes, 0.0);
    }

    // ========== Display tests ==========

    #[test]
    fn test_paimon_metrics_display() {
        let mut metrics = PaimonMetrics::new();
        metrics.table_name = "test_table".to_string();
        metrics.metadata.format_version = 3;
        metrics.metadata.field_count = 5;
        metrics.metadata.primary_keys = vec!["id".to_string()];
        metrics.metadata.partition_keys = vec!["dt".to_string()];
        metrics.metadata.bucket_count = 4;
        metrics.snapshot_info.current_snapshot_id = 10;
        metrics.snapshot_info.total_snapshots = 10;
        metrics.snapshot_info.total_record_count = 1000;
        metrics.schema_info.current_schema_id = 0;
        metrics.schema_info.total_schema_versions = 1;
        metrics.file_stats.num_data_files = 20;
        metrics.file_stats.total_data_size_bytes = 1024 * 1024 * 100;
        metrics.partition_info.num_partition_keys = 1;
        metrics.partition_info.num_partitions = 5;
        metrics.partition_info.num_buckets = 4;

        let display = format!("{}", metrics);
        assert!(display.contains("test_table"));
        assert!(display.contains("Format Version"));
        assert!(display.contains("Field Count"));
        assert!(display.contains("Primary Keys"));
        assert!(display.contains("Partition Keys"));
        assert!(display.contains("Bucket Count"));
        assert!(display.contains("Current Snapshot ID"));
        assert!(display.contains("Total Snapshots"));
        assert!(display.contains("Data Files"));
    }

    #[test]
    fn test_paimon_metrics_display_with_uuid() {
        let mut metrics = PaimonMetrics::new();
        metrics.metadata.uuid = "550e8400-e29b-41d4-a716-446655440000".to_string();

        let display = format!("{}", metrics);
        assert!(display.contains("Table UUID"));
        assert!(display.contains("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn test_paimon_metrics_display_with_commit_kind() {
        let mut metrics = PaimonMetrics::new();
        metrics.snapshot_info.latest_commit_kind = Some("COMPACT".to_string());

        let display = format!("{}", metrics);
        assert!(display.contains("Latest Commit Kind"));
        assert!(display.contains("COMPACT"));
    }

    #[test]
    fn test_paimon_metrics_display_empty() {
        // Test display with all empty/default values
        let metrics = PaimonMetrics::new();
        let display = format!("{}", metrics);

        // Should still contain section headers
        assert!(display.contains("Paimon Specific Metrics"));
        assert!(display.contains("Snapshot Info"));
        assert!(display.contains("Schema Info"));
        assert!(display.contains("File Statistics"));
        assert!(display.contains("Partition Info"));
        // Should NOT contain optional fields when empty
        assert!(!display.contains("Table Name"));
        assert!(!display.contains("Table UUID"));
        // Note: "Primary Keys" and "Partition Keys" labels are only shown when non-empty
        // but "Partition Keys" appears in Partition Info section as a count label
        assert!(!display.contains("Latest Commit Kind"));
    }

    #[test]
    fn test_paimon_metrics_display_file_stats_formatting() {
        let mut metrics = PaimonMetrics::new();
        metrics.file_stats.num_data_files = 100;
        metrics.file_stats.total_data_size_bytes = 1024 * 1024 * 512; // 512 MB
        metrics.file_stats.avg_data_file_size_bytes = 1024.0 * 1024.0 * 5.12; // 5.12 MB
        metrics.file_stats.num_manifest_files = 10;

        let display = format!("{}", metrics);
        assert!(display.contains("512.00 MB"));
        assert!(display.contains("5.12 MB"));
        assert!(display.contains("Manifest Files"));
    }

    // ========== Serialization tests ==========

    #[test]
    fn test_paimon_metrics_serialization() {
        let metrics = PaimonMetrics::new();
        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: PaimonMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(metrics.table_name, deserialized.table_name);
        assert_eq!(
            metrics.metadata.format_version,
            deserialized.metadata.format_version
        );
    }

    #[test]
    fn test_table_metadata_serialization() {
        let mut metadata = TableMetadata::new();
        metadata.uuid = "test-uuid".to_string();
        metadata.format_version = 2;
        metadata.field_count = 10;
        metadata.primary_keys = vec!["id".to_string(), "ts".to_string()];
        metadata.partition_keys = vec!["dt".to_string()];
        metadata.bucket_count = 8;
        metadata.comment = Some("Test table".to_string());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: TableMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(metadata.uuid, deserialized.uuid);
        assert_eq!(metadata.format_version, deserialized.format_version);
        assert_eq!(metadata.field_count, deserialized.field_count);
        assert_eq!(metadata.primary_keys, deserialized.primary_keys);
        assert_eq!(metadata.partition_keys, deserialized.partition_keys);
        assert_eq!(metadata.bucket_count, deserialized.bucket_count);
        assert_eq!(metadata.comment, deserialized.comment);
    }

    #[test]
    fn test_snapshot_metrics_serialization() {
        let mut metrics = SnapshotMetrics::new();
        metrics.current_snapshot_id = 42;
        metrics.total_snapshots = 100;
        metrics.earliest_snapshot_id = Some(1);
        metrics.latest_commit_time = Some(1705000000000);
        metrics.earliest_commit_time = Some(1700000000000);
        metrics.latest_commit_kind = Some("APPEND".to_string());
        metrics.total_record_count = 1_000_000;

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: SnapshotMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(
            metrics.current_snapshot_id,
            deserialized.current_snapshot_id
        );
        assert_eq!(metrics.total_snapshots, deserialized.total_snapshots);
        assert_eq!(
            metrics.earliest_snapshot_id,
            deserialized.earliest_snapshot_id
        );
        assert_eq!(metrics.latest_commit_time, deserialized.latest_commit_time);
        assert_eq!(
            metrics.earliest_commit_time,
            deserialized.earliest_commit_time
        );
        assert_eq!(metrics.latest_commit_kind, deserialized.latest_commit_kind);
        assert_eq!(metrics.total_record_count, deserialized.total_record_count);
    }

    #[test]
    fn test_schema_metrics_serialization() {
        let mut metrics = SchemaMetrics::new();
        metrics.current_schema_id = 5;
        metrics.total_schema_versions = 6;
        metrics.schema_string = r#"{"fields": []}"#.to_string();

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: SchemaMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(metrics.current_schema_id, deserialized.current_schema_id);
        assert_eq!(
            metrics.total_schema_versions,
            deserialized.total_schema_versions
        );
        assert_eq!(metrics.schema_string, deserialized.schema_string);
    }

    #[test]
    fn test_file_statistics_serialization() {
        let mut stats = FileStatistics::new();
        stats.num_data_files = 500;
        stats.total_data_size_bytes = 1024 * 1024 * 1024; // 1 GB
        stats.avg_data_file_size_bytes = 2.0 * 1024.0 * 1024.0; // 2 MB
        stats.min_data_file_size_bytes = 1024;
        stats.max_data_file_size_bytes = 10 * 1024 * 1024;
        stats.num_manifest_files = 20;
        stats.total_manifest_size_bytes = 1024 * 100;

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: FileStatistics = serde_json::from_str(&json).unwrap();

        assert_eq!(stats.num_data_files, deserialized.num_data_files);
        assert_eq!(
            stats.total_data_size_bytes,
            deserialized.total_data_size_bytes
        );
        assert_eq!(
            stats.avg_data_file_size_bytes,
            deserialized.avg_data_file_size_bytes
        );
        assert_eq!(
            stats.min_data_file_size_bytes,
            deserialized.min_data_file_size_bytes
        );
        assert_eq!(
            stats.max_data_file_size_bytes,
            deserialized.max_data_file_size_bytes
        );
        assert_eq!(stats.num_manifest_files, deserialized.num_manifest_files);
        assert_eq!(
            stats.total_manifest_size_bytes,
            deserialized.total_manifest_size_bytes
        );
    }

    #[test]
    fn test_partition_metrics_serialization() {
        let mut metrics = PartitionMetrics::new();
        metrics.num_partition_keys = 2;
        metrics.num_partitions = 365;
        metrics.num_buckets = 16;
        metrics.largest_partition_size_bytes = 100 * 1024 * 1024;
        metrics.smallest_partition_size_bytes = 1024;
        metrics.avg_partition_size_bytes = 10.0 * 1024.0 * 1024.0;

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: PartitionMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(metrics.num_partition_keys, deserialized.num_partition_keys);
        assert_eq!(metrics.num_partitions, deserialized.num_partitions);
        assert_eq!(metrics.num_buckets, deserialized.num_buckets);
        assert_eq!(
            metrics.largest_partition_size_bytes,
            deserialized.largest_partition_size_bytes
        );
        assert_eq!(
            metrics.smallest_partition_size_bytes,
            deserialized.smallest_partition_size_bytes
        );
        assert_eq!(
            metrics.avg_partition_size_bytes,
            deserialized.avg_partition_size_bytes
        );
    }

    // ========== Clone tests ==========

    #[test]
    fn test_paimon_metrics_clone() {
        let mut metrics = PaimonMetrics::new();
        metrics.table_name = "cloned_table".to_string();
        metrics.metadata.field_count = 15;

        let cloned = metrics.clone();
        assert_eq!(cloned.table_name, "cloned_table");
        assert_eq!(cloned.metadata.field_count, 15);
    }

    #[test]
    fn test_table_metadata_clone() {
        let mut metadata = TableMetadata::new();
        metadata.primary_keys = vec!["a".to_string(), "b".to_string()];

        let cloned = metadata.clone();
        assert_eq!(cloned.primary_keys, vec!["a", "b"]);
    }

    #[test]
    fn test_snapshot_metrics_clone() {
        let mut metrics = SnapshotMetrics::new();
        metrics.current_snapshot_id = 99;

        let cloned = metrics.clone();
        assert_eq!(cloned.current_snapshot_id, 99);
    }

    #[test]
    fn test_schema_metrics_clone() {
        let mut metrics = SchemaMetrics::new();
        metrics.schema_string = "test schema".to_string();

        let cloned = metrics.clone();
        assert_eq!(cloned.schema_string, "test schema");
    }

    #[test]
    fn test_file_statistics_clone() {
        let mut stats = FileStatistics::new();
        stats.num_data_files = 42;

        let cloned = stats.clone();
        assert_eq!(cloned.num_data_files, 42);
    }

    #[test]
    fn test_partition_metrics_clone() {
        let mut metrics = PartitionMetrics::new();
        metrics.num_partitions = 100;

        let cloned = metrics.clone();
        assert_eq!(cloned.num_partitions, 100);
    }

    // ========== Debug tests ==========

    #[test]
    fn test_paimon_metrics_debug() {
        let metrics = PaimonMetrics::new();
        let debug = format!("{:?}", metrics);
        assert!(debug.contains("PaimonMetrics"));
    }

    #[test]
    fn test_table_metadata_debug() {
        let metadata = TableMetadata::new();
        let debug = format!("{:?}", metadata);
        assert!(debug.contains("TableMetadata"));
    }

    #[test]
    fn test_snapshot_metrics_debug() {
        let metrics = SnapshotMetrics::new();
        let debug = format!("{:?}", metrics);
        assert!(debug.contains("SnapshotMetrics"));
    }

    #[test]
    fn test_schema_metrics_debug() {
        let metrics = SchemaMetrics::new();
        let debug = format!("{:?}", metrics);
        assert!(debug.contains("SchemaMetrics"));
    }

    #[test]
    fn test_file_statistics_debug() {
        let stats = FileStatistics::new();
        let debug = format!("{:?}", stats);
        assert!(debug.contains("FileStatistics"));
    }

    #[test]
    fn test_partition_metrics_debug() {
        let metrics = PartitionMetrics::new();
        let debug = format!("{:?}", metrics);
        assert!(debug.contains("PartitionMetrics"));
    }
}
