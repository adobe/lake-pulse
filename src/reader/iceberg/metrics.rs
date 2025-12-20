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

/// Iceberg table specific metrics extracted from table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergMetrics {
    /// Current snapshot ID
    pub current_snapshot_id: Option<i64>,

    /// Table format version
    pub format_version: i32,

    /// Table UUID
    pub table_uuid: String,

    /// Table metadata
    pub metadata: TableMetadata,

    /// Table properties
    pub table_properties: HashMap<String, String>,

    /// Snapshot information
    pub snapshot_info: SnapshotMetrics,

    /// Schema information
    pub schema_info: SchemaMetrics,

    /// Partition spec information
    pub partition_spec: PartitionSpecMetrics,

    /// Sort order information
    pub sort_order: SortOrderMetrics,

    /// File statistics
    pub file_stats: FileStatistics,

    /// Manifest statistics
    pub manifest_stats: ManifestStatistics,
}

/// Iceberg table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table location (base path)
    pub location: String,

    /// Last updated timestamp (milliseconds since epoch)
    pub last_updated_ms: Option<i64>,

    /// Last column ID assigned
    pub last_column_id: i32,

    /// Current schema ID
    pub current_schema_id: i32,

    /// Number of schemas
    pub schema_count: usize,

    /// Default spec ID
    pub default_spec_id: i32,

    /// Number of partition specs
    pub partition_spec_count: usize,

    /// Default sort order ID
    pub default_sort_order_id: i32,

    /// Number of sort orders
    pub sort_order_count: usize,

    /// Last sequence number
    pub last_sequence_number: Option<i64>,
}

/// Snapshot-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetrics {
    /// Total number of snapshots
    pub total_snapshots: usize,

    /// Current snapshot ID
    pub current_snapshot_id: Option<i64>,

    /// Current snapshot timestamp (milliseconds since epoch)
    pub current_snapshot_timestamp_ms: Option<i64>,

    /// Parent snapshot ID (if available)
    pub parent_snapshot_id: Option<i64>,

    /// Snapshot operation (append, replace, overwrite, delete)
    pub operation: Option<String>,

    /// Snapshot summary statistics
    pub summary: HashMap<String, String>,

    /// Manifest list location
    pub manifest_list: Option<String>,

    /// Schema ID at snapshot time
    pub schema_id: Option<i32>,

    /// Sequence number
    pub sequence_number: Option<i64>,
}

/// Schema-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetrics {
    /// Current schema ID
    pub schema_id: i32,

    /// Number of fields in current schema
    pub field_count: usize,

    /// Schema as JSON string
    pub schema_string: String,

    /// List of field names
    pub field_names: Vec<String>,

    /// Number of nested fields (structs, lists, maps)
    pub nested_field_count: usize,

    /// Number of required fields
    pub required_field_count: usize,

    /// Number of optional fields
    pub optional_field_count: usize,
}

/// Partition spec metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpecMetrics {
    /// Partition spec ID
    pub spec_id: i32,

    /// Number of partition fields
    pub partition_field_count: usize,

    /// Partition field names
    pub partition_fields: Vec<String>,

    /// Partition transforms (identity, bucket, truncate, year, month, day, hour)
    pub partition_transforms: Vec<String>,

    /// Whether the table is partitioned
    pub is_partitioned: bool,

    /// Estimated number of unique partitions (if available)
    pub estimated_partition_count: Option<usize>,
}

/// Sort order metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrderMetrics {
    /// Sort order ID
    pub order_id: i32,

    /// Number of sort fields
    pub sort_field_count: usize,

    /// Sort field names
    pub sort_fields: Vec<String>,

    /// Sort directions (asc, desc)
    pub sort_directions: Vec<String>,

    /// Null orders (nulls-first, nulls-last)
    pub null_orders: Vec<String>,

    /// Whether the table has a sort order defined
    pub is_sorted: bool,
}

/// File-level statistics from Iceberg table
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

    /// Total number of records across all data files
    pub total_records: Option<i64>,

    /// Number of delete files (position deletes and equality deletes)
    pub num_delete_files: usize,

    /// Total size of delete files in bytes
    pub total_delete_size_bytes: u64,

    /// Number of position delete files
    pub num_position_delete_files: usize,

    /// Number of equality delete files
    pub num_equality_delete_files: usize,
}

/// Manifest file statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestStatistics {
    /// Total number of manifest files
    pub num_manifest_files: usize,

    /// Total size of manifest files in bytes
    pub total_manifest_size_bytes: u64,

    /// Average manifest file size in bytes
    pub avg_manifest_file_size_bytes: f64,

    /// Number of data manifests
    pub num_data_manifests: usize,

    /// Number of delete manifests
    pub num_delete_manifests: usize,

    /// Total number of manifest lists
    pub num_manifest_lists: usize,

    /// Total size of manifest lists in bytes
    pub total_manifest_list_size_bytes: u64,
}

impl IcebergMetrics {
    /// Create a new IcebergMetrics instance with default values
    pub fn new() -> Self {
        Self {
            current_snapshot_id: None,
            format_version: 1,
            table_uuid: String::new(),
            metadata: TableMetadata {
                location: String::new(),
                last_updated_ms: None,
                last_column_id: 0,
                current_schema_id: 0,
                schema_count: 0,
                default_spec_id: 0,
                partition_spec_count: 0,
                default_sort_order_id: 0,
                sort_order_count: 0,
                last_sequence_number: None,
            },
            table_properties: HashMap::new(),
            snapshot_info: SnapshotMetrics {
                total_snapshots: 0,
                current_snapshot_id: None,
                current_snapshot_timestamp_ms: None,
                parent_snapshot_id: None,
                operation: None,
                summary: HashMap::new(),
                manifest_list: None,
                schema_id: None,
                sequence_number: None,
            },
            schema_info: SchemaMetrics {
                schema_id: 0,
                field_count: 0,
                schema_string: String::new(),
                field_names: Vec::new(),
                nested_field_count: 0,
                required_field_count: 0,
                optional_field_count: 0,
            },
            partition_spec: PartitionSpecMetrics {
                spec_id: 0,
                partition_field_count: 0,
                partition_fields: Vec::new(),
                partition_transforms: Vec::new(),
                is_partitioned: false,
                estimated_partition_count: None,
            },
            sort_order: SortOrderMetrics {
                order_id: 0,
                sort_field_count: 0,
                sort_fields: Vec::new(),
                sort_directions: Vec::new(),
                null_orders: Vec::new(),
                is_sorted: false,
            },
            file_stats: FileStatistics {
                num_data_files: 0,
                total_data_size_bytes: 0,
                avg_data_file_size_bytes: 0.0,
                min_data_file_size_bytes: 0,
                max_data_file_size_bytes: 0,
                total_records: None,
                num_delete_files: 0,
                total_delete_size_bytes: 0,
                num_position_delete_files: 0,
                num_equality_delete_files: 0,
            },
            manifest_stats: ManifestStatistics {
                num_manifest_files: 0,
                total_manifest_size_bytes: 0,
                avg_manifest_file_size_bytes: 0.0,
                num_data_manifests: 0,
                num_delete_manifests: 0,
                num_manifest_lists: 0,
                total_manifest_list_size_bytes: 0,
            },
        }
    }
}

impl Default for IcebergMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_metrics_new() {
        let metrics = IcebergMetrics::new();

        assert_eq!(metrics.current_snapshot_id, None);
        assert_eq!(metrics.format_version, 1);
        assert_eq!(metrics.table_uuid, "");
        assert_eq!(metrics.metadata.location, "");
        assert_eq!(metrics.table_properties.len(), 0);
        assert_eq!(metrics.snapshot_info.total_snapshots, 0);
        assert_eq!(metrics.schema_info.field_count, 0);
        assert_eq!(metrics.partition_spec.partition_field_count, 0);
        assert_eq!(metrics.sort_order.sort_field_count, 0);
        assert_eq!(metrics.file_stats.num_data_files, 0);
        assert_eq!(metrics.manifest_stats.num_manifest_files, 0);
    }

    #[test]
    fn test_iceberg_metrics_default() {
        let metrics = IcebergMetrics::default();

        assert_eq!(metrics.current_snapshot_id, None);
        assert_eq!(metrics.format_version, 1);
        assert_eq!(metrics.table_uuid, "");
    }

    #[test]
    fn test_table_metadata_structure() {
        let metadata = TableMetadata {
            location: "/path/to/table".to_string(),
            last_updated_ms: Some(1234567890),
            last_column_id: 10,
            current_schema_id: 1,
            schema_count: 2,
            default_spec_id: 0,
            partition_spec_count: 1,
            default_sort_order_id: 0,
            sort_order_count: 1,
            last_sequence_number: Some(5),
        };

        assert_eq!(metadata.location, "/path/to/table");
        assert_eq!(metadata.last_updated_ms, Some(1234567890));
        assert_eq!(metadata.last_column_id, 10);
        assert_eq!(metadata.current_schema_id, 1);
        assert_eq!(metadata.schema_count, 2);
    }

    #[test]
    fn test_snapshot_metrics_structure() {
        let mut summary = HashMap::new();
        summary.insert("total-records".to_string(), "1000".to_string());

        let snapshot = SnapshotMetrics {
            total_snapshots: 5,
            current_snapshot_id: Some(123),
            current_snapshot_timestamp_ms: Some(1234567890),
            parent_snapshot_id: Some(122),
            operation: Some("append".to_string()),
            summary,
            manifest_list: Some("/path/to/manifest".to_string()),
            schema_id: Some(1),
            sequence_number: Some(10),
        };

        assert_eq!(snapshot.total_snapshots, 5);
        assert_eq!(snapshot.current_snapshot_id, Some(123));
        assert_eq!(snapshot.parent_snapshot_id, Some(122));
        assert_eq!(snapshot.operation, Some("append".to_string()));
        assert_eq!(
            snapshot.summary.get("total-records"),
            Some(&"1000".to_string())
        );
    }

    #[test]
    fn test_schema_metrics_structure() {
        let schema = SchemaMetrics {
            schema_id: 1,
            field_count: 5,
            schema_string: "{}".to_string(),
            field_names: vec!["id".to_string(), "name".to_string()],
            nested_field_count: 1,
            required_field_count: 3,
            optional_field_count: 2,
        };

        assert_eq!(schema.schema_id, 1);
        assert_eq!(schema.field_count, 5);
        assert_eq!(schema.field_names.len(), 2);
        assert_eq!(schema.required_field_count + schema.optional_field_count, 5);
    }

    #[test]
    fn test_partition_spec_metrics_structure() {
        let partition_spec = PartitionSpecMetrics {
            spec_id: 0,
            partition_field_count: 2,
            partition_fields: vec!["year".to_string(), "month".to_string()],
            partition_transforms: vec!["year".to_string(), "month".to_string()],
            is_partitioned: true,
            estimated_partition_count: Some(100),
        };

        assert_eq!(partition_spec.spec_id, 0);
        assert_eq!(partition_spec.partition_field_count, 2);
        assert_eq!(partition_spec.partition_fields.len(), 2);
        assert_eq!(partition_spec.partition_transforms.len(), 2);
        assert!(partition_spec.is_partitioned);
    }

    #[test]
    fn test_partition_spec_unpartitioned() {
        let partition_spec = PartitionSpecMetrics {
            spec_id: 0,
            partition_field_count: 0,
            partition_fields: vec![],
            partition_transforms: vec![],
            is_partitioned: false,
            estimated_partition_count: None,
        };

        assert_eq!(partition_spec.partition_field_count, 0);
        assert!(!partition_spec.is_partitioned);
        assert_eq!(partition_spec.partition_fields.len(), 0);
    }

    #[test]
    fn test_sort_order_metrics_structure() {
        let sort_order = SortOrderMetrics {
            order_id: 1,
            sort_field_count: 2,
            sort_fields: vec!["1".to_string(), "2".to_string()],
            sort_directions: vec!["asc".to_string(), "desc".to_string()],
            null_orders: vec!["nulls-first".to_string(), "nulls-last".to_string()],
            is_sorted: true,
        };

        assert_eq!(sort_order.order_id, 1);
        assert_eq!(sort_order.sort_field_count, 2);
        assert_eq!(sort_order.sort_fields.len(), 2);
        assert_eq!(sort_order.sort_directions.len(), 2);
        assert_eq!(sort_order.null_orders.len(), 2);
        assert!(sort_order.is_sorted);
    }

    #[test]
    fn test_sort_order_unsorted() {
        let sort_order = SortOrderMetrics {
            order_id: 0,
            sort_field_count: 0,
            sort_fields: vec![],
            sort_directions: vec![],
            null_orders: vec![],
            is_sorted: false,
        };

        assert_eq!(sort_order.sort_field_count, 0);
        assert!(!sort_order.is_sorted);
    }

    #[test]
    fn test_file_statistics_structure() {
        let file_stats = FileStatistics {
            num_data_files: 10,
            total_data_size_bytes: 1000000,
            avg_data_file_size_bytes: 100000.0,
            min_data_file_size_bytes: 50000,
            max_data_file_size_bytes: 150000,
            total_records: Some(10000),
            num_delete_files: 2,
            total_delete_size_bytes: 20000,
            num_position_delete_files: 1,
            num_equality_delete_files: 1,
        };

        assert_eq!(file_stats.num_data_files, 10);
        assert_eq!(file_stats.total_data_size_bytes, 1000000);
        assert_eq!(file_stats.avg_data_file_size_bytes, 100000.0);
        assert_eq!(file_stats.total_records, Some(10000));
        assert_eq!(
            file_stats.num_delete_files,
            file_stats.num_position_delete_files + file_stats.num_equality_delete_files
        );
    }

    #[test]
    fn test_file_statistics_empty() {
        let file_stats = FileStatistics {
            num_data_files: 0,
            total_data_size_bytes: 0,
            avg_data_file_size_bytes: 0.0,
            min_data_file_size_bytes: 0,
            max_data_file_size_bytes: 0,
            total_records: None,
            num_delete_files: 0,
            total_delete_size_bytes: 0,
            num_position_delete_files: 0,
            num_equality_delete_files: 0,
        };

        assert_eq!(file_stats.num_data_files, 0);
        assert_eq!(file_stats.total_data_size_bytes, 0);
        assert_eq!(file_stats.avg_data_file_size_bytes, 0.0);
        assert_eq!(file_stats.total_records, None);
    }

    #[test]
    fn test_manifest_statistics_structure() {
        let manifest_stats = ManifestStatistics {
            num_manifest_files: 5,
            total_manifest_size_bytes: 500000,
            avg_manifest_file_size_bytes: 100000.0,
            num_data_manifests: 4,
            num_delete_manifests: 1,
            num_manifest_lists: 1,
            total_manifest_list_size_bytes: 500000,
        };

        assert_eq!(manifest_stats.num_manifest_files, 5);
        assert_eq!(
            manifest_stats.num_manifest_files,
            manifest_stats.num_data_manifests + manifest_stats.num_delete_manifests
        );
        assert_eq!(manifest_stats.total_manifest_size_bytes, 500000);
        assert_eq!(manifest_stats.avg_manifest_file_size_bytes, 100000.0);
    }

    #[test]
    fn test_manifest_statistics_empty() {
        let manifest_stats = ManifestStatistics {
            num_manifest_files: 0,
            total_manifest_size_bytes: 0,
            avg_manifest_file_size_bytes: 0.0,
            num_data_manifests: 0,
            num_delete_manifests: 0,
            num_manifest_lists: 0,
            total_manifest_list_size_bytes: 0,
        };

        assert_eq!(manifest_stats.num_manifest_files, 0);
        assert_eq!(manifest_stats.total_manifest_size_bytes, 0);
        assert_eq!(manifest_stats.avg_manifest_file_size_bytes, 0.0);
    }

    #[test]
    fn test_iceberg_metrics_serialization() {
        let metrics = IcebergMetrics::new();

        // Test that metrics can be serialized to JSON
        let json = serde_json::to_string(&metrics);
        assert!(
            json.is_ok(),
            "Should be able to serialize IcebergMetrics to JSON"
        );

        // Test that metrics can be deserialized from JSON
        let json_str = json.unwrap();
        let deserialized: Result<IcebergMetrics, _> = serde_json::from_str(&json_str);
        assert!(
            deserialized.is_ok(),
            "Should be able to deserialize IcebergMetrics from JSON"
        );
    }

    #[test]
    fn test_table_metadata_serialization() {
        let metadata = TableMetadata {
            location: "/path/to/table".to_string(),
            last_updated_ms: Some(1234567890),
            last_column_id: 10,
            current_schema_id: 1,
            schema_count: 2,
            default_spec_id: 0,
            partition_spec_count: 1,
            default_sort_order_id: 0,
            sort_order_count: 1,
            last_sequence_number: Some(5),
        };

        let json = serde_json::to_string(&metadata);
        assert!(json.is_ok());

        let deserialized: Result<TableMetadata, _> = serde_json::from_str(&json.unwrap());
        assert!(deserialized.is_ok());
    }

    #[test]
    fn test_snapshot_metrics_clone() {
        let snapshot = SnapshotMetrics {
            total_snapshots: 5,
            current_snapshot_id: Some(123),
            current_snapshot_timestamp_ms: Some(1234567890),
            parent_snapshot_id: Some(122),
            operation: Some("append".to_string()),
            summary: HashMap::new(),
            manifest_list: Some("/path/to/manifest".to_string()),
            schema_id: Some(1),
            sequence_number: Some(10),
        };

        let cloned = snapshot.clone();
        assert_eq!(snapshot.total_snapshots, cloned.total_snapshots);
        assert_eq!(snapshot.current_snapshot_id, cloned.current_snapshot_id);
    }

    #[test]
    fn test_iceberg_metrics_debug() {
        let metrics = IcebergMetrics::new();
        let debug_str = format!("{:?}", metrics);
        assert!(!debug_str.is_empty());
        assert!(debug_str.contains("IcebergMetrics"));
    }
}
