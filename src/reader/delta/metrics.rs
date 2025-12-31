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

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Delta Lake specific metrics extracted from table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaMetrics {
    /// Table version
    pub version: i64,

    /// Protocol version information
    pub protocol: ProtocolInfo,

    /// Table metadata
    pub metadata: TableMetadata,

    /// Table configuration/properties
    pub table_properties: HashMap<String, String>,

    /// File statistics
    pub file_stats: FileStatistics,

    /// Partition information
    pub partition_info: PartitionMetrics,
}

/// Delta Lake protocol information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolInfo {
    /// Minimum reader version required to read the table
    pub min_reader_version: i32,

    /// Minimum writer version required to write to the table
    pub min_writer_version: i32,

    /// Reader features (for protocol version 3+)
    pub reader_features: Option<Vec<String>>,

    /// Writer features (for protocol version 7+)
    pub writer_features: Option<Vec<String>>,
}

/// Delta Lake table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table ID
    pub id: String,

    /// Table name (if available)
    pub name: Option<String>,

    /// Table description (if available)
    pub description: Option<String>,

    /// Schema as JSON string
    pub schema_string: String,

    /// Number of fields in the schema
    pub field_count: usize,

    /// Partition columns
    pub partition_columns: Vec<String>,

    /// Table creation time (milliseconds since epoch)
    pub created_time: Option<i64>,

    /// Table format provider
    pub format_provider: String,

    /// Table format options
    pub format_options: HashMap<String, String>,
}

/// File-level statistics from Delta table
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

    /// Number of files with deletion vectors
    pub files_with_deletion_vectors: usize,
}

/// Partition-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetrics {
    /// Number of partition columns
    pub num_partition_columns: usize,

    /// Partition column names
    pub partition_columns: Vec<String>,

    /// Estimated number of unique partitions (if available)
    pub estimated_partition_count: Option<usize>,
}

impl DeltaMetrics {
    /// Create a new DeltaMetrics instance with default values
    pub fn new() -> Self {
        Self {
            version: 0,
            protocol: ProtocolInfo {
                min_reader_version: 0,
                min_writer_version: 0,
                reader_features: None,
                writer_features: None,
            },
            metadata: TableMetadata {
                id: String::new(),
                name: None,
                description: None,
                schema_string: String::new(),
                field_count: 0,
                partition_columns: Vec::new(),
                created_time: None,
                format_provider: String::new(),
                format_options: HashMap::new(),
            },
            table_properties: HashMap::new(),
            file_stats: FileStatistics {
                num_files: 0,
                total_size_bytes: 0,
                avg_file_size_bytes: 0.0,
                min_file_size_bytes: 0,
                max_file_size_bytes: 0,
                files_with_deletion_vectors: 0,
            },
            partition_info: PartitionMetrics {
                num_partition_columns: 0,
                partition_columns: Vec::new(),
                estimated_partition_count: None,
            },
        }
    }
}

impl Default for DeltaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for DeltaMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        writeln!(f)?;
        writeln!(f, " Delta Specific Metrics")?;
        writeln!(f, "{}", "━".repeat(80))?;
        writeln!(f, " {:<40} {:>32}", "Version", self.version)?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Min Reader Version", self.protocol.min_reader_version
        )?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Min Writer Version", self.protocol.min_writer_version
        )?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Reader Features",
            format!("{:?}", self.protocol.reader_features)
        )?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Writer Features",
            format!("{:?}", self.protocol.writer_features)
        )?;
        writeln!(f, " {:<31}  {:>40}", "Table ID", &self.metadata.id)?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Table Name",
            format!("{:?}", self.metadata.name)
        )?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Table Description",
            format!("{:?}", self.metadata.description)
        )?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Field Count", self.metadata.field_count
        )?;
        writeln!(
            f,
            " {:<40} {:>32}",
            "Partition Columns",
            format!("{:?}", self.metadata.partition_columns)
        )?;
        if let Some(created_time) = self.metadata.created_time {
            if let Some(created_datetime) = DateTime::from_timestamp(created_time / 1000, 0) {
                writeln!(
                    f,
                    " {:<40} {:>32}",
                    "Created Time",
                    created_datetime.to_rfc3339()
                )?;
            }
        }
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
        } else {
            writeln!(f, " {:<20} {:>52}", "Table Properties", "None")?;
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
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Files with Deletion Vectors", file_stats.files_with_deletion_vectors
        )?;

        // Partition Info
        let partition_info = &self.partition_info;
        writeln!(f, " Partition Info")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Partition Columns", partition_info.num_partition_columns
        )?;
        if !partition_info.partition_columns.is_empty() {
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Column Names",
                partition_info.partition_columns.join(", ")
            )?;
        }
        if let Some(count) = partition_info.estimated_partition_count {
            writeln!(f, "   {:<38} {:>32}", "Estimated Partitions", count)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_metrics_new() {
        let metrics = DeltaMetrics::new();

        assert_eq!(metrics.version, 0);
        assert_eq!(metrics.protocol.min_reader_version, 0);
        assert_eq!(metrics.protocol.min_writer_version, 0);
        assert!(metrics.protocol.reader_features.is_none());
        assert!(metrics.protocol.writer_features.is_none());
        assert_eq!(metrics.metadata.id, "");
        assert!(metrics.metadata.name.is_none());
        assert!(metrics.metadata.description.is_none());
        assert_eq!(metrics.metadata.schema_string, "");
        assert_eq!(metrics.metadata.field_count, 0);
        assert_eq!(metrics.metadata.partition_columns.len(), 0);
        assert!(metrics.metadata.created_time.is_none());
        assert_eq!(metrics.metadata.format_provider, "");
        assert_eq!(metrics.metadata.format_options.len(), 0);
        assert_eq!(metrics.table_properties.len(), 0);
        assert_eq!(metrics.file_stats.num_files, 0);
        assert_eq!(metrics.file_stats.total_size_bytes, 0);
        assert_eq!(metrics.file_stats.avg_file_size_bytes, 0.0);
        assert_eq!(metrics.file_stats.min_file_size_bytes, 0);
        assert_eq!(metrics.file_stats.max_file_size_bytes, 0);
        assert_eq!(metrics.file_stats.files_with_deletion_vectors, 0);
        assert_eq!(metrics.partition_info.num_partition_columns, 0);
        assert_eq!(metrics.partition_info.partition_columns.len(), 0);
        assert!(metrics.partition_info.estimated_partition_count.is_none());
    }

    #[test]
    fn test_delta_metrics_default() {
        let metrics = DeltaMetrics::default();
        let new_metrics = DeltaMetrics::new();

        assert_eq!(metrics.version, new_metrics.version);
        assert_eq!(
            metrics.protocol.min_reader_version,
            new_metrics.protocol.min_reader_version
        );
    }

    #[test]
    fn test_delta_metrics_clone() {
        let mut metrics = DeltaMetrics::new();
        metrics.version = 42;
        metrics.metadata.id = "test-id".to_string();

        let cloned = metrics.clone();

        assert_eq!(cloned.version, 42);
        assert_eq!(cloned.metadata.id, "test-id");
    }

    #[test]
    fn test_delta_metrics_debug() {
        let metrics = DeltaMetrics::new();
        let debug_str = format!("{:?}", metrics);

        assert!(debug_str.contains("DeltaMetrics"));
        assert!(debug_str.contains("version"));
    }

    #[test]
    fn test_protocol_info_with_features() {
        let protocol = ProtocolInfo {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![
                "columnMapping".to_string(),
                "deletionVectors".to_string(),
            ]),
            writer_features: Some(vec!["appendOnly".to_string(), "invariants".to_string()]),
        };

        assert_eq!(protocol.min_reader_version, 3);
        assert_eq!(protocol.min_writer_version, 7);
        assert_eq!(protocol.reader_features.as_ref().unwrap().len(), 2);
        assert_eq!(protocol.writer_features.as_ref().unwrap().len(), 2);
        assert!(protocol
            .reader_features
            .as_ref()
            .unwrap()
            .contains(&"columnMapping".to_string()));
    }

    #[test]
    fn test_protocol_info_without_features() {
        let protocol = ProtocolInfo {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        };

        assert_eq!(protocol.min_reader_version, 1);
        assert_eq!(protocol.min_writer_version, 2);
        assert!(protocol.reader_features.is_none());
        assert!(protocol.writer_features.is_none());
    }

    #[test]
    fn test_table_metadata_complete() {
        let mut format_options = HashMap::new();
        format_options.insert("compression".to_string(), "snappy".to_string());

        let metadata = TableMetadata {
            id: "table-123".to_string(),
            name: Some("my_table".to_string()),
            description: Some("Test table".to_string()),
            schema_string: r#"{"type":"struct","fields":[]}"#.to_string(),
            field_count: 5,
            partition_columns: vec!["year".to_string(), "month".to_string()],
            created_time: Some(1609459200000), // 2021-01-01
            format_provider: "parquet".to_string(),
            format_options,
        };

        assert_eq!(metadata.id, "table-123");
        assert_eq!(metadata.name.as_ref().unwrap(), "my_table");
        assert_eq!(metadata.description.as_ref().unwrap(), "Test table");
        assert_eq!(metadata.field_count, 5);
        assert_eq!(metadata.partition_columns.len(), 2);
        assert_eq!(metadata.created_time.unwrap(), 1609459200000);
        assert_eq!(metadata.format_provider, "parquet");
        assert_eq!(metadata.format_options.len(), 1);
    }

    #[test]
    fn test_table_metadata_minimal() {
        let metadata = TableMetadata {
            id: "table-456".to_string(),
            name: None,
            description: None,
            schema_string: "".to_string(),
            field_count: 0,
            partition_columns: vec![],
            created_time: None,
            format_provider: "".to_string(),
            format_options: HashMap::new(),
        };

        assert_eq!(metadata.id, "table-456");
        assert!(metadata.name.is_none());
        assert!(metadata.description.is_none());
        assert_eq!(metadata.partition_columns.len(), 0);
        assert!(metadata.created_time.is_none());
    }

    #[test]
    fn test_file_statistics_empty() {
        let stats = FileStatistics {
            num_files: 0,
            total_size_bytes: 0,
            avg_file_size_bytes: 0.0,
            min_file_size_bytes: 0,
            max_file_size_bytes: 0,
            files_with_deletion_vectors: 0,
        };

        assert_eq!(stats.num_files, 0);
        assert_eq!(stats.total_size_bytes, 0);
        assert_eq!(stats.avg_file_size_bytes, 0.0);
        assert_eq!(stats.min_file_size_bytes, 0);
        assert_eq!(stats.max_file_size_bytes, 0);
        assert_eq!(stats.files_with_deletion_vectors, 0);
    }

    #[test]
    fn test_file_statistics_with_data() {
        let stats = FileStatistics {
            num_files: 100,
            total_size_bytes: 10_000_000_000,   // 10GB
            avg_file_size_bytes: 100_000_000.0, // 100MB
            min_file_size_bytes: 50_000_000,    // 50MB
            max_file_size_bytes: 200_000_000,   // 200MB
            files_with_deletion_vectors: 5,
        };

        assert_eq!(stats.num_files, 100);
        assert_eq!(stats.total_size_bytes, 10_000_000_000);
        assert_eq!(stats.avg_file_size_bytes, 100_000_000.0);
        assert_eq!(stats.min_file_size_bytes, 50_000_000);
        assert_eq!(stats.max_file_size_bytes, 200_000_000);
        assert_eq!(stats.files_with_deletion_vectors, 5);
    }

    #[test]
    fn test_file_statistics_clone() {
        let stats = FileStatistics {
            num_files: 50,
            total_size_bytes: 5_000_000,
            avg_file_size_bytes: 100_000.0,
            min_file_size_bytes: 50_000,
            max_file_size_bytes: 150_000,
            files_with_deletion_vectors: 2,
        };

        let cloned = stats.clone();

        assert_eq!(cloned.num_files, 50);
        assert_eq!(cloned.total_size_bytes, 5_000_000);
        assert_eq!(cloned.files_with_deletion_vectors, 2);
    }

    #[test]
    fn test_partition_metrics_no_partitions() {
        let metrics = PartitionMetrics {
            num_partition_columns: 0,
            partition_columns: vec![],
            estimated_partition_count: None,
        };

        assert_eq!(metrics.num_partition_columns, 0);
        assert_eq!(metrics.partition_columns.len(), 0);
        assert!(metrics.estimated_partition_count.is_none());
    }

    #[test]
    fn test_partition_metrics_with_partitions() {
        let metrics = PartitionMetrics {
            num_partition_columns: 3,
            partition_columns: vec!["year".to_string(), "month".to_string(), "day".to_string()],
            estimated_partition_count: Some(365),
        };

        assert_eq!(metrics.num_partition_columns, 3);
        assert_eq!(metrics.partition_columns.len(), 3);
        assert_eq!(metrics.estimated_partition_count.unwrap(), 365);
        assert!(metrics.partition_columns.contains(&"year".to_string()));
        assert!(metrics.partition_columns.contains(&"month".to_string()));
        assert!(metrics.partition_columns.contains(&"day".to_string()));
    }

    #[test]
    fn test_partition_metrics_clone() {
        let metrics = PartitionMetrics {
            num_partition_columns: 2,
            partition_columns: vec!["region".to_string(), "country".to_string()],
            estimated_partition_count: Some(100),
        };

        let cloned = metrics.clone();

        assert_eq!(cloned.num_partition_columns, 2);
        assert_eq!(cloned.partition_columns.len(), 2);
        assert_eq!(cloned.estimated_partition_count.unwrap(), 100);
    }

    #[test]
    fn test_delta_metrics_serialization() {
        let mut metrics = DeltaMetrics::new();
        metrics.version = 10;
        metrics.protocol.min_reader_version = 2;
        metrics.protocol.min_writer_version = 5;
        metrics.metadata.id = "test-table".to_string();
        metrics.metadata.name = Some("my_delta_table".to_string());

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"version\":10"));
        assert!(json.contains("\"min_reader_version\":2"));
        assert!(json.contains("\"min_writer_version\":5"));
        assert!(json.contains("test-table"));
    }

    #[test]
    fn test_delta_metrics_deserialization() {
        let json = r#"{
            "version": 5,
            "protocol": {
                "min_reader_version": 1,
                "min_writer_version": 2,
                "reader_features": null,
                "writer_features": null
            },
            "metadata": {
                "id": "abc-123",
                "name": "test_table",
                "description": null,
                "schema_string": "",
                "field_count": 3,
                "partition_columns": ["date"],
                "created_time": 1234567890,
                "format_provider": "parquet",
                "format_options": {}
            },
            "table_properties": {},
            "file_stats": {
                "num_files": 10,
                "total_size_bytes": 1000000,
                "avg_file_size_bytes": 100000.0,
                "min_file_size_bytes": 50000,
                "max_file_size_bytes": 150000,
                "files_with_deletion_vectors": 0
            },
            "partition_info": {
                "num_partition_columns": 1,
                "partition_columns": ["date"],
                "estimated_partition_count": null
            }
        }"#;

        let metrics: DeltaMetrics = serde_json::from_str(json).unwrap();

        assert_eq!(metrics.version, 5);
        assert_eq!(metrics.protocol.min_reader_version, 1);
        assert_eq!(metrics.protocol.min_writer_version, 2);
        assert_eq!(metrics.metadata.id, "abc-123");
        assert_eq!(metrics.metadata.name.as_ref().unwrap(), "test_table");
        assert_eq!(metrics.metadata.field_count, 3);
        assert_eq!(metrics.file_stats.num_files, 10);
        assert_eq!(metrics.partition_info.num_partition_columns, 1);
    }

    #[test]
    fn test_protocol_info_serialization() {
        let protocol = ProtocolInfo {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".to_string()]),
            writer_features: Some(vec!["appendOnly".to_string()]),
        };

        let json = serde_json::to_string(&protocol).unwrap();
        assert!(json.contains("\"min_reader_version\":3"));
        assert!(json.contains("\"min_writer_version\":7"));
        assert!(json.contains("deletionVectors"));
        assert!(json.contains("appendOnly"));
    }

    #[test]
    fn test_table_metadata_serialization() {
        let mut format_options = HashMap::new();
        format_options.insert("compression".to_string(), "snappy".to_string());

        let metadata = TableMetadata {
            id: "table-789".to_string(),
            name: Some("sales".to_string()),
            description: Some("Sales data".to_string()),
            schema_string: r#"{"type":"struct"}"#.to_string(),
            field_count: 10,
            partition_columns: vec!["year".to_string()],
            created_time: Some(1609459200000),
            format_provider: "parquet".to_string(),
            format_options,
        };

        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("table-789"));
        assert!(json.contains("sales"));
        assert!(json.contains("Sales data"));
        assert!(json.contains("\"field_count\":10"));
    }

    #[test]
    fn test_file_statistics_serialization() {
        let stats = FileStatistics {
            num_files: 25,
            total_size_bytes: 2_500_000,
            avg_file_size_bytes: 100_000.0,
            min_file_size_bytes: 50_000,
            max_file_size_bytes: 200_000,
            files_with_deletion_vectors: 3,
        };

        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"num_files\":25"));
        assert!(json.contains("\"total_size_bytes\":2500000"));
        assert!(json.contains("\"files_with_deletion_vectors\":3"));
    }

    #[test]
    fn test_partition_metrics_serialization() {
        let metrics = PartitionMetrics {
            num_partition_columns: 2,
            partition_columns: vec!["year".to_string(), "month".to_string()],
            estimated_partition_count: Some(24),
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"num_partition_columns\":2"));
        assert!(json.contains("year"));
        assert!(json.contains("month"));
        assert!(json.contains("\"estimated_partition_count\":24"));
    }

    #[test]
    fn test_delta_metrics_with_table_properties() {
        let mut metrics = DeltaMetrics::new();
        metrics
            .table_properties
            .insert("delta.appendOnly".to_string(), "true".to_string());
        metrics.table_properties.insert(
            "delta.enableChangeDataFeed".to_string(),
            "false".to_string(),
        );

        assert_eq!(metrics.table_properties.len(), 2);
        assert_eq!(
            metrics.table_properties.get("delta.appendOnly").unwrap(),
            "true"
        );
        assert_eq!(
            metrics
                .table_properties
                .get("delta.enableChangeDataFeed")
                .unwrap(),
            "false"
        );
    }

    #[test]
    fn test_delta_metrics_round_trip_serialization() {
        let mut original = DeltaMetrics::new();
        original.version = 100;
        original.protocol.min_reader_version = 3;
        original.protocol.reader_features = Some(vec!["columnMapping".to_string()]);
        original.metadata.id = "round-trip-test".to_string();
        original.metadata.field_count = 15;
        original.file_stats.num_files = 50;
        original.partition_info.num_partition_columns = 2;

        // Serialize to JSON
        let json = serde_json::to_string(&original).unwrap();

        // Deserialize back
        let deserialized: DeltaMetrics = serde_json::from_str(&json).unwrap();

        // Verify all fields match
        assert_eq!(deserialized.version, 100);
        assert_eq!(deserialized.protocol.min_reader_version, 3);
        assert_eq!(
            deserialized.protocol.reader_features.as_ref().unwrap()[0],
            "columnMapping"
        );
        assert_eq!(deserialized.metadata.id, "round-trip-test");
        assert_eq!(deserialized.metadata.field_count, 15);
        assert_eq!(deserialized.file_stats.num_files, 50);
        assert_eq!(deserialized.partition_info.num_partition_columns, 2);
    }

    #[test]
    fn test_protocol_info_debug() {
        let protocol = ProtocolInfo {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        };

        let debug_str = format!("{:?}", protocol);
        assert!(debug_str.contains("ProtocolInfo"));
        assert!(debug_str.contains("min_reader_version"));
    }

    #[test]
    fn test_file_statistics_debug() {
        let stats = FileStatistics {
            num_files: 10,
            total_size_bytes: 1000,
            avg_file_size_bytes: 100.0,
            min_file_size_bytes: 50,
            max_file_size_bytes: 150,
            files_with_deletion_vectors: 1,
        };

        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("FileStatistics"));
        assert!(debug_str.contains("num_files"));
    }

    #[test]
    fn test_large_version_number() {
        let mut metrics = DeltaMetrics::new();
        metrics.version = i64::MAX;

        assert_eq!(metrics.version, i64::MAX);

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: DeltaMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.version, i64::MAX);
    }

    #[test]
    fn test_negative_version_number() {
        let mut metrics = DeltaMetrics::new();
        metrics.version = -1;

        assert_eq!(metrics.version, -1);
    }

    #[test]
    fn test_file_statistics_with_large_sizes() {
        let stats = FileStatistics {
            num_files: usize::MAX,
            total_size_bytes: u64::MAX,
            avg_file_size_bytes: f64::MAX,
            min_file_size_bytes: u64::MAX,
            max_file_size_bytes: u64::MAX,
            files_with_deletion_vectors: usize::MAX,
        };

        assert_eq!(stats.num_files, usize::MAX);
        assert_eq!(stats.total_size_bytes, u64::MAX);
        assert_eq!(stats.max_file_size_bytes, u64::MAX);
    }

    #[test]
    fn test_delta_metrics_display() {
        let mut metrics = DeltaMetrics::new();
        metrics.version = 5;
        metrics.protocol.min_reader_version = 1;
        metrics.protocol.min_writer_version = 2;
        metrics.protocol.reader_features = Some(vec!["deletionVectors".to_string()]);
        metrics.protocol.writer_features = Some(vec!["deletionVectors".to_string()]);
        metrics.metadata.id = "test-table-id".to_string();
        metrics.metadata.name = Some("test_table".to_string());
        metrics.metadata.description = Some("A test table".to_string());
        metrics.metadata.field_count = 10;
        metrics.metadata.partition_columns = vec!["date".to_string()];
        metrics.metadata.created_time = Some(1700000000000);
        metrics
            .table_properties
            .insert("delta.minReaderVersion".to_string(), "1".to_string());
        metrics.file_stats = FileStatistics {
            num_files: 100,
            total_size_bytes: 1024 * 1024 * 500,
            avg_file_size_bytes: 1024.0 * 1024.0 * 5.0,
            min_file_size_bytes: 1024,
            max_file_size_bytes: 1024 * 1024 * 10,
            files_with_deletion_vectors: 5,
        };
        metrics.partition_info = PartitionMetrics {
            num_partition_columns: 1,
            partition_columns: vec!["date".to_string()],
            estimated_partition_count: Some(30),
        };

        let display = format!("{}", metrics);

        assert!(display.contains("Delta Specific Metrics"));
        assert!(display.contains("Version"));
        assert!(display.contains("5"));
        assert!(display.contains("Min Reader Version"));
        assert!(display.contains("Min Writer Version"));
        assert!(display.contains("test-table-id"));
        assert!(display.contains("test_table"));
        assert!(display.contains("File Statistics"));
        assert!(display.contains("100"));
        assert!(display.contains("Partition Info"));
        assert!(display.contains("Table Properties"));
    }

    #[test]
    fn test_delta_metrics_display_minimal() {
        let metrics = DeltaMetrics::new();
        let display = format!("{}", metrics);

        assert!(display.contains("Delta Specific Metrics"));
        assert!(display.contains("Version"));
        assert!(display.contains("File Statistics"));
    }

    #[test]
    fn test_delta_metrics_display_no_table_properties() {
        let mut metrics = DeltaMetrics::new();
        metrics.version = 1;

        let display = format!("{}", metrics);

        assert!(display.contains("Table Properties"));
        assert!(display.contains("None"));
    }
}
