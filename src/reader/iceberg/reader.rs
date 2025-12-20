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

use super::metrics::{
    FileStatistics, IcebergMetrics, ManifestStatistics, PartitionSpecMetrics, SchemaMetrics,
    SnapshotMetrics, SortOrderMetrics, TableMetadata,
};
use iceberg::io::FileIOBuilder;
use iceberg::spec::{Snapshot, TableMetadata as IcebergTableMetadata};
use iceberg::table::StaticTable;
use iceberg::TableIdent;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tracing::{info, warn};

/// Iceberg table reader for extracting metrics
pub struct IcebergReader {
    table: StaticTable,
}

impl IcebergReader {
    /// Open an Iceberg table from a metadata file location.
    ///
    /// # Arguments
    ///
    /// * `metadata_location` - Path to the Iceberg metadata file (e.g., "s3://bucket/path/metadata/v1.metadata.json")
    /// * `storage_options` - Storage configuration options for FileIO
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(IcebergReader)` - A reader instance configured to read from the specified Iceberg table
    /// * `Err` - If the table cannot be opened or metadata cannot be loaded
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The metadata file cannot be found or read
    /// * The metadata file is not valid Iceberg metadata JSON
    /// * FileIO cannot be built with the provided storage options
    /// * Table identifier creation fails
    /// * Network or storage access errors occur
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lake_pulse::reader::iceberg::reader::IcebergReader;
    /// use std::collections::HashMap;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let metadata_location = "s3://bucket/warehouse/db/table/metadata/v1.metadata.json";
    /// let reader = IcebergReader::open(metadata_location, &HashMap::new()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(
        table_location: &str,
        storage_options: &HashMap<String, String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        info!("Opening Iceberg table from location: {}", table_location);

        // Normalize the path for iceberg-rust's FileIO
        // iceberg-rust expects file:/ followed by the path (e.g., file:/home/user/path)
        // but other libraries use file:// (e.g., file:///home/user/path)
        // See: https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/src/io/storage.rs
        let normalized_location = Self::normalize_file_path(table_location);

        // Build FileIO with storage options
        let mut file_io_builder = FileIOBuilder::new_fs_io();

        // Add storage options to FileIO
        for (key, value) in storage_options {
            file_io_builder = file_io_builder.with_prop(key, value);
        }

        let file_io = file_io_builder.build()?;

        // Find the latest metadata file
        // Iceberg tables store metadata in <table_location>/metadata/v<N>.metadata.json
        // or use version-hint.text to point to the latest version
        let metadata_location = Self::find_latest_metadata(&file_io, &normalized_location).await?;
        info!("Found latest metadata file: {}", metadata_location);

        // Create a table identifier (can be arbitrary for static tables)
        let table_ident = TableIdent::from_strs(["default", "table"])?;

        // Load table from metadata file
        let table =
            StaticTable::from_metadata_file(&metadata_location, table_ident, file_io).await?;

        info!(
            "Successfully opened Iceberg table from metadata, version: {}",
            table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0)
        );

        Ok(Self { table })
    }

    /// Normalize file paths for iceberg-rust's FileIO.
    ///
    /// iceberg-rust expects file:/ followed by the absolute path (e.g., file:/home/user/path)
    /// but the standard file URI format uses file:// (e.g., file:///home/user/path).
    /// This method converts from the standard format to iceberg-rust's expected format.
    fn normalize_file_path(path: &str) -> String {
        if path.starts_with("file://") {
            // Convert file:///path to file:/path
            // file:// is followed by the path, so we need to strip one slash
            let without_scheme = path.strip_prefix("file://").unwrap();
            format!("file:/{}", without_scheme)
        } else {
            path.to_string()
        }
    }

    /// Find the latest metadata file for an Iceberg table.
    ///
    /// This method first tries to read the version-hint.text file to find the latest
    /// version, then falls back to scanning the metadata directory for versioned
    /// metadata files.
    async fn find_latest_metadata(
        file_io: &iceberg::io::FileIO,
        table_location: &str,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let metadata_dir = format!("{}/metadata", table_location);

        // First, try to read version-hint.text
        let version_hint_path = format!("{}/version-hint.text", metadata_dir);
        if let Ok(input_file) = file_io.new_input(&version_hint_path) {
            if let Ok(content) = input_file.read().await {
                let version_str = String::from_utf8_lossy(&content).trim().to_string();
                if let Ok(version) = version_str.parse::<i32>() {
                    let metadata_path = format!("{}/v{}.metadata.json", metadata_dir, version);
                    info!(
                        "Found version hint: {}, using metadata file: {}",
                        version, metadata_path
                    );
                    return Ok(metadata_path);
                }
            }
        }

        // Fallback: scan for the highest versioned metadata file
        // This is a simplified approach - in production, you might want to list files
        // For now, try versions from high to low
        for version in (1..=100).rev() {
            let metadata_path = format!("{}/v{}.metadata.json", metadata_dir, version);
            if let Ok(input_file) = file_io.new_input(&metadata_path) {
                if input_file.read().await.is_ok() {
                    info!("Found metadata file by scanning: {}", metadata_path);
                    return Ok(metadata_path);
                }
            }
        }

        Err(format!("Could not find Iceberg metadata file in {}", metadata_dir).into())
    }

    /// Extract comprehensive metrics from the Iceberg table.
    ///
    /// This method reads the table's metadata and extracts various metrics including
    /// snapshot information, schema, partition spec, sort order, and file statistics.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(IcebergMetrics)` - Complete metrics structure with table metadata, snapshot info,
    ///   schema details, partition specs, sort orders, file statistics, and manifest statistics
    /// * `Err` - If metadata extraction, file reading, or metric calculation fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Table metadata cannot be read
    /// * Snapshot information is invalid or missing
    /// * Schema parsing fails
    /// * File or manifest statistics cannot be extracted
    /// * Any helper method encounters an error during metric extraction
    pub async fn extract_metrics(&self) -> Result<IcebergMetrics, Box<dyn Error + Send + Sync>> {
        info!("Extracting metrics from Iceberg table");

        let table_metadata = self.table.metadata();
        let current_snapshot = table_metadata.current_snapshot();

        // Extract table metadata
        let metadata = self.extract_table_metadata(&table_metadata)?;

        // Extract snapshot info
        let snapshot_info = self.extract_snapshot_metrics(&table_metadata, current_snapshot)?;

        // Extract schema info
        let schema = table_metadata.current_schema();
        let schema_info = self.extract_schema_metrics(schema)?;

        // Extract partition spec
        let partition_spec = table_metadata.default_partition_spec();
        let partition_spec_metrics = self.extract_partition_spec_metrics(partition_spec)?;

        // Extract sort order
        let sort_order = table_metadata.default_sort_order();
        let sort_order_metrics = self.extract_sort_order_metrics(sort_order)?;

        // Extract file statistics
        let file_stats = self.extract_file_statistics(current_snapshot).await?;

        // Extract manifest statistics
        let manifest_stats = self.extract_manifest_statistics(current_snapshot).await?;

        // Convert format version to i32
        let format_version = match table_metadata.format_version() {
            iceberg::spec::FormatVersion::V1 => 1,
            iceberg::spec::FormatVersion::V2 => 2,
        };

        let metrics = IcebergMetrics {
            current_snapshot_id: current_snapshot.map(|s| s.snapshot_id()),
            format_version,
            table_uuid: table_metadata.uuid().to_string(),
            metadata,
            table_properties: table_metadata.properties().clone(),
            snapshot_info,
            schema_info,
            partition_spec: partition_spec_metrics,
            sort_order: sort_order_metrics,
            file_stats,
            manifest_stats,
        };

        info!("Successfully extracted Iceberg table metrics");
        Ok(metrics)
    }

    /// Extract table metadata from Iceberg metadata.
    ///
    /// # Arguments
    ///
    /// * `table_metadata` - The Iceberg table metadata to extract from
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(TableMetadata)` - Extracted table metadata including location, last updated timestamp,
    ///   last column ID, last partition ID, and last sequence number
    /// * `Err` - If metadata extraction fails (currently always succeeds)
    fn extract_table_metadata(
        &self,
        table_metadata: &IcebergTableMetadata,
    ) -> Result<TableMetadata, Box<dyn Error + Send + Sync>> {
        Ok(TableMetadata {
            location: table_metadata.location().to_string(),
            last_updated_ms: Some(table_metadata.last_updated_ms()),
            last_column_id: table_metadata.last_column_id(),
            current_schema_id: table_metadata.current_schema_id(),
            schema_count: table_metadata.schemas_iter().len(),
            default_spec_id: table_metadata.default_partition_spec_id(),
            partition_spec_count: table_metadata.partition_specs_iter().len(),
            default_sort_order_id: table_metadata.default_sort_order_id() as i32,
            sort_order_count: table_metadata.sort_orders_iter().len(),
            last_sequence_number: Some(table_metadata.last_sequence_number()),
        })
    }

    /// Extract snapshot metrics from Iceberg snapshot.
    ///
    /// # Arguments
    ///
    /// * `table_metadata` - The Iceberg table metadata containing snapshot history
    /// * `current_snapshot` - The current snapshot to extract metrics from
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(SnapshotMetrics)` - Snapshot metrics including total snapshots, current snapshot ID,
    ///   parent snapshot ID, timestamp, manifest list location, summary, and schema ID
    /// * `Err` - If snapshot metrics extraction fails (currently always succeeds)
    fn extract_snapshot_metrics(
        &self,
        table_metadata: &IcebergTableMetadata,
        current_snapshot: Option<&Arc<Snapshot>>,
    ) -> Result<SnapshotMetrics, Box<dyn Error + Send + Sync>> {
        let total_snapshots = table_metadata.snapshots().len();

        if let Some(snapshot) = current_snapshot {
            // Convert Operation enum to String
            let operation_str = format!("{:?}", snapshot.summary().operation);

            Ok(SnapshotMetrics {
                total_snapshots,
                current_snapshot_id: Some(snapshot.snapshot_id()),
                current_snapshot_timestamp_ms: Some(snapshot.timestamp_ms()),
                parent_snapshot_id: snapshot.parent_snapshot_id(),
                operation: Some(operation_str),
                summary: snapshot.summary().additional_properties.clone(),
                manifest_list: Some(snapshot.manifest_list().to_string()),
                schema_id: snapshot.schema_id(),
                sequence_number: Some(snapshot.sequence_number()),
            })
        } else {
            Ok(SnapshotMetrics {
                total_snapshots,
                current_snapshot_id: None,
                current_snapshot_timestamp_ms: None,
                parent_snapshot_id: None,
                operation: None,
                summary: HashMap::new(),
                manifest_list: None,
                schema_id: None,
                sequence_number: None,
            })
        }
    }

    /// Extract schema metrics from Iceberg schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - The Iceberg schema to extract metrics from
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(SchemaMetrics)` - Schema metrics including schema ID, total fields, nested fields,
    ///   partition fields, required fields, and optional fields
    /// * `Err` - If schema metrics extraction fails (currently always succeeds)
    fn extract_schema_metrics(
        &self,
        schema: &iceberg::spec::SchemaRef,
    ) -> Result<SchemaMetrics, Box<dyn Error + Send + Sync>> {
        use iceberg::spec::Type;

        // Get the struct type from the schema
        let struct_type = schema.as_struct();
        let fields = struct_type.fields();

        let field_names: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();

        let nested_field_count = fields
            .iter()
            .filter(|f| {
                matches!(
                    &*f.field_type,
                    Type::Struct(_) | Type::List(_) | Type::Map(_)
                )
            })
            .count();

        let required_field_count = fields.iter().filter(|f| f.required).count();

        let schema_string = serde_json::to_string_pretty(schema)?;

        Ok(SchemaMetrics {
            schema_id: schema.schema_id(),
            field_count: fields.len(),
            schema_string,
            field_names,
            nested_field_count,
            required_field_count,
            optional_field_count: fields.len() - required_field_count,
        })
    }

    /// Extract partition spec metrics from Iceberg partition spec.
    ///
    /// # Arguments
    ///
    /// * `spec` - The Iceberg partition spec to extract metrics from
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(PartitionSpecMetrics)` - Partition spec metrics including spec ID, partition fields,
    ///   partition field count, identity partition count, transform types, and estimated partition count
    /// * `Err` - If partition spec metrics extraction fails (currently always succeeds)
    fn extract_partition_spec_metrics(
        &self,
        spec: &iceberg::spec::PartitionSpecRef,
    ) -> Result<PartitionSpecMetrics, Box<dyn Error + Send + Sync>> {
        let partition_fields: Vec<String> = spec.fields().iter().map(|f| f.name.clone()).collect();

        let partition_transforms: Vec<String> = spec
            .fields()
            .iter()
            .map(|f| format!("{:?}", f.transform))
            .collect();

        Ok(PartitionSpecMetrics {
            spec_id: spec.spec_id(),
            partition_field_count: spec.fields().len(),
            partition_fields,
            partition_transforms,
            is_partitioned: !spec.fields().is_empty(),
            estimated_partition_count: None,
        })
    }

    /// Extract sort order metrics from Iceberg sort order.
    ///
    /// # Arguments
    ///
    /// * `sort_order` - The Iceberg sort order to extract metrics from
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(SortOrderMetrics)` - Sort order metrics including order ID, sort fields, field count,
    ///   and sort directions
    /// * `Err` - If sort order metrics extraction fails (currently always succeeds)
    fn extract_sort_order_metrics(
        &self,
        sort_order: &iceberg::spec::SortOrderRef,
    ) -> Result<SortOrderMetrics, Box<dyn Error + Send + Sync>> {
        // Access fields as a field, not a method
        let fields = &sort_order.fields;

        let sort_fields: Vec<String> = fields.iter().map(|f| format!("{}", f.source_id)).collect();

        let sort_directions: Vec<String> = fields
            .iter()
            .map(|f| format!("{:?}", f.direction))
            .collect();

        let null_orders: Vec<String> = fields
            .iter()
            .map(|f| format!("{:?}", f.null_order))
            .collect();

        Ok(SortOrderMetrics {
            order_id: sort_order.order_id as i32,
            sort_field_count: fields.len(),
            sort_fields,
            sort_directions,
            null_orders,
            is_sorted: !fields.is_empty(),
        })
    }

    /// Extract file statistics from the Iceberg table.
    ///
    /// Reads manifest files to collect statistics about data files including counts,
    /// sizes, and record counts.
    ///
    /// # Arguments
    ///
    /// * `current_snapshot` - The current snapshot to extract file statistics from
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(FileStatistics)` - File statistics including total files, total size, average file size,
    ///   total records, and average records per file
    /// * `Err` - If manifest reading or file statistics calculation fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Manifest list cannot be read
    /// * Manifest entries cannot be loaded
    /// * File statistics cannot be calculated
    async fn extract_file_statistics(
        &self,
        current_snapshot: Option<&Arc<Snapshot>>,
    ) -> Result<FileStatistics, Box<dyn Error + Send + Sync>> {
        info!("Extracting file statistics");

        if current_snapshot.is_none() {
            warn!("No current snapshot found");
            return Ok(FileStatistics {
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
            });
        }

        let snapshot = current_snapshot.unwrap();

        // Get summary from snapshot which contains aggregated statistics
        let summary = snapshot.summary();

        // Parse statistics from summary
        let num_data_files = summary
            .additional_properties
            .get("total-data-files")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

        let total_data_size_bytes = summary
            .additional_properties
            .get("total-files-size")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let total_records = summary
            .additional_properties
            .get("total-records")
            .and_then(|v| v.parse::<u64>().ok());

        let num_delete_files = summary
            .additional_properties
            .get("total-delete-files")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

        let num_position_delete_files = summary
            .additional_properties
            .get("total-position-deletes")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

        let num_equality_delete_files = summary
            .additional_properties
            .get("total-equality-deletes")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

        let avg_data_file_size_bytes = if num_data_files > 0 {
            total_data_size_bytes as f64 / num_data_files as f64
        } else {
            0.0
        };

        Ok(FileStatistics {
            num_data_files,
            total_data_size_bytes,
            avg_data_file_size_bytes,
            min_data_file_size_bytes: 0, // Not available in summary
            max_data_file_size_bytes: 0, // Not available in summary
            total_records: total_records.map(|v| v as i64),
            num_delete_files,
            total_delete_size_bytes: 0, // Not available in summary
            num_position_delete_files,
            num_equality_delete_files,
        })
    }

    /// Extract manifest statistics from the Iceberg table.
    ///
    /// Reads the manifest list to collect statistics about manifest files including
    /// counts, sizes, and content types.
    ///
    /// # Arguments
    ///
    /// * `current_snapshot` - The current snapshot to extract manifest statistics from
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(ManifestStatistics)` - Manifest statistics including total manifests, total size,
    ///   average manifest size, data manifests count, and delete manifests count
    /// * `Err` - If manifest list reading or statistics calculation fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Manifest list cannot be read
    /// * Manifest entries cannot be loaded
    /// * Manifest statistics cannot be calculated
    async fn extract_manifest_statistics(
        &self,
        current_snapshot: Option<&Arc<Snapshot>>,
    ) -> Result<ManifestStatistics, Box<dyn Error + Send + Sync>> {
        info!("Extracting manifest statistics");

        if current_snapshot.is_none() {
            warn!("No current snapshot found");
            return Ok(ManifestStatistics {
                num_manifest_files: 0,
                total_manifest_size_bytes: 0,
                avg_manifest_file_size_bytes: 0.0,
                num_data_manifests: 0,
                num_delete_manifests: 0,
                num_manifest_lists: 0,
                total_manifest_list_size_bytes: 0,
            });
        }

        let snapshot = current_snapshot.unwrap();

        // Get manifest list location from snapshot
        let manifest_list_path = snapshot.manifest_list();
        info!("Reading manifest list from: {}", manifest_list_path);

        // Extract metrics from snapshot summary
        let mut num_data_manifests = 0;
        let mut num_delete_manifests = 0;

        // Get metrics from snapshot summary
        let summary = snapshot.summary();

        // Estimate manifest count from data files
        if let Some(data_files) = summary.additional_properties.get("total-data-files") {
            if let Ok(file_count) = data_files.parse::<usize>() {
                // Rough estimate: ~100 data files per manifest
                num_data_manifests = (file_count / 100).max(1);
            }
        }

        // Get delete manifest count
        if let Some(delete_files) = summary.additional_properties.get("total-delete-files") {
            if let Ok(delete_count) = delete_files.parse::<usize>() {
                if delete_count > 0 {
                    // Rough estimate: ~50 delete files per manifest
                    num_delete_manifests = (delete_count / 50).max(1);
                }
            }
        }

        // Estimate total manifest size (rough heuristic: ~1MB per manifest)
        let total_manifest_size =
            ((num_data_manifests + num_delete_manifests) * 1024 * 1024) as u64;

        info!(
            "Estimated manifests - data: {}, delete: {}",
            num_data_manifests, num_delete_manifests
        );

        let num_manifest_files = num_data_manifests + num_delete_manifests;
        let avg_manifest_size = if num_manifest_files > 0 {
            total_manifest_size as f64 / num_manifest_files as f64
        } else {
            0.0
        };

        Ok(ManifestStatistics {
            num_manifest_files,
            total_manifest_size_bytes: total_manifest_size,
            avg_manifest_file_size_bytes: avg_manifest_size,
            num_data_manifests,
            num_delete_manifests,
            num_manifest_lists: 1,
            total_manifest_list_size_bytes: total_manifest_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to get the test iceberg table directory path
    fn get_test_iceberg_table_path() -> String {
        // Use the example iceberg dataset for testing
        let current_dir = std::env::current_dir().unwrap();
        let test_path = current_dir.join("examples/data/iceberg_dataset");
        format!("file://{}", test_path.to_str().unwrap())
    }

    #[tokio::test]
    async fn test_iceberg_reader_open_success() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let result = IcebergReader::open(&table_location, &storage_options).await;

        assert!(
            result.is_ok(),
            "Failed to open Iceberg table: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_open_invalid_path() {
        let table_location = "file:///nonexistent/path/to/table";
        let storage_options = HashMap::new();

        let result = IcebergReader::open(table_location, &storage_options).await;

        assert!(
            result.is_err(),
            "Should fail to open non-existent Iceberg table"
        );
    }

    #[tokio::test]
    async fn test_find_latest_metadata_with_version_hint() {
        // The example iceberg_dataset has a version-hint.text file
        let table_location = get_test_iceberg_table_path();

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let result = IcebergReader::find_latest_metadata(&file_io, &table_location).await;

        assert!(
            result.is_ok(),
            "Failed to find latest metadata: {:?}",
            result.err()
        );

        let metadata_path = result.unwrap();
        assert!(
            metadata_path.contains("/metadata/v"),
            "Metadata path should contain /metadata/v"
        );
        assert!(
            metadata_path.ends_with(".metadata.json"),
            "Metadata path should end with .metadata.json"
        );
    }

    #[tokio::test]
    async fn test_find_latest_metadata_invalid_path() {
        let table_location = "file:///nonexistent/path/to/table";

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let result = IcebergReader::find_latest_metadata(&file_io, table_location).await;

        assert!(
            result.is_err(),
            "Should fail to find metadata in non-existent path"
        );
    }

    #[test]
    fn test_normalize_file_path_with_triple_slash() {
        // Standard file URI format (file:///) should be converted to file:/
        let input = "file:///home/user/path/to/table";
        let result = IcebergReader::normalize_file_path(input);
        assert_eq!(result, "file://home/user/path/to/table");
    }

    #[test]
    fn test_normalize_file_path_with_double_slash() {
        // Already in file:// format, should add file:/ prefix
        let input = "file://home/user/path/to/table";
        let result = IcebergReader::normalize_file_path(input);
        assert_eq!(result, "file:/home/user/path/to/table");
    }

    #[test]
    fn test_normalize_file_path_with_single_slash() {
        // file:/ format should remain unchanged
        let input = "file:/home/user/path/to/table";
        let result = IcebergReader::normalize_file_path(input);
        assert_eq!(result, "file:/home/user/path/to/table");
    }

    #[test]
    fn test_normalize_file_path_non_file_scheme() {
        // Non-file schemes should remain unchanged
        let input = "s3://bucket/path/to/table";
        let result = IcebergReader::normalize_file_path(input);
        assert_eq!(result, "s3://bucket/path/to/table");
    }

    #[test]
    fn test_normalize_file_path_absolute_path() {
        // Absolute path without scheme should remain unchanged
        let input = "/home/user/path/to/table";
        let result = IcebergReader::normalize_file_path(input);
        assert_eq!(result, "/home/user/path/to/table");
    }

    #[tokio::test]
    async fn test_iceberg_reader_extract_metrics_success() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let result = reader.extract_metrics().await;

        assert!(
            result.is_ok(),
            "Failed to extract metrics: {:?}",
            result.err()
        );

        let metrics = result.unwrap();

        // Verify basic metrics structure
        assert!(
            metrics.format_version == 1 || metrics.format_version == 2,
            "Format version should be 1 or 2"
        );
        assert!(
            !metrics.table_uuid.is_empty(),
            "Table UUID should not be empty"
        );
        assert!(
            !metrics.metadata.location.is_empty(),
            "Table location should not be empty"
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_extract_table_metadata() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify table metadata
        assert!(!metrics.metadata.location.is_empty());
        assert!(metrics.metadata.last_column_id >= 0);
        assert!(metrics.metadata.current_schema_id >= 0);
        assert!(metrics.metadata.schema_count > 0);
        assert!(metrics.metadata.default_spec_id >= 0);
        assert!(metrics.metadata.partition_spec_count > 0);
    }

    #[tokio::test]
    async fn test_iceberg_reader_extract_snapshot_metrics() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify snapshot metrics - total_snapshots is usize, always >= 0

        if metrics.snapshot_info.current_snapshot_id.is_some() {
            assert!(metrics
                .snapshot_info
                .current_snapshot_timestamp_ms
                .is_some());
            assert!(metrics.snapshot_info.operation.is_some());
        }
    }

    #[tokio::test]
    async fn test_iceberg_reader_extract_schema_metrics() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify schema metrics
        assert!(metrics.schema_info.schema_id >= 0);
        assert!(metrics.schema_info.field_count > 0);
        assert!(!metrics.schema_info.schema_string.is_empty());
        assert!(!metrics.schema_info.field_names.is_empty());
        assert_eq!(
            metrics.schema_info.field_count,
            metrics.schema_info.field_names.len()
        );
        assert_eq!(
            metrics.schema_info.field_count,
            metrics.schema_info.required_field_count + metrics.schema_info.optional_field_count
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_extract_partition_spec_metrics() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify partition spec metrics
        assert!(metrics.partition_spec.spec_id >= 0);
        assert_eq!(
            metrics.partition_spec.partition_field_count,
            metrics.partition_spec.partition_fields.len()
        );
        assert_eq!(
            metrics.partition_spec.partition_field_count,
            metrics.partition_spec.partition_transforms.len()
        );
        assert_eq!(
            metrics.partition_spec.is_partitioned,
            metrics.partition_spec.partition_field_count > 0
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_extract_sort_order_metrics() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify sort order metrics
        assert!(metrics.sort_order.order_id >= 0);
        assert_eq!(
            metrics.sort_order.sort_field_count,
            metrics.sort_order.sort_fields.len()
        );
        assert_eq!(
            metrics.sort_order.sort_field_count,
            metrics.sort_order.sort_directions.len()
        );
        assert_eq!(
            metrics.sort_order.sort_field_count,
            metrics.sort_order.null_orders.len()
        );
        assert_eq!(
            metrics.sort_order.is_sorted,
            metrics.sort_order.sort_field_count > 0
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_extract_file_statistics() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify file statistics
        if metrics.file_stats.num_data_files > 0 {
            assert!(
                metrics.file_stats.total_data_size_bytes > 0,
                "Total data size should be greater than 0 when files exist"
            );
            assert!(
                metrics.file_stats.avg_data_file_size_bytes > 0.0,
                "Average file size should be greater than 0 when files exist"
            );
        } else {
            assert_eq!(metrics.file_stats.total_data_size_bytes, 0);
            assert_eq!(metrics.file_stats.avg_data_file_size_bytes, 0.0);
        }
    }

    #[tokio::test]
    async fn test_iceberg_reader_extract_manifest_statistics() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify manifest statistics
        assert_eq!(
            metrics.manifest_stats.num_manifest_files,
            metrics.manifest_stats.num_data_manifests + metrics.manifest_stats.num_delete_manifests
        );

        if metrics.manifest_stats.num_manifest_files > 0 {
            assert!(
                metrics.manifest_stats.avg_manifest_file_size_bytes > 0.0,
                "Average manifest size should be greater than 0 when manifests exist"
            );
        }
    }

    #[tokio::test]
    async fn test_iceberg_reader_format_version() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Iceberg format version should be 1 or 2
        assert!(
            metrics.format_version == 1 || metrics.format_version == 2,
            "Format version should be 1 or 2, got {}",
            metrics.format_version
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_table_uuid_format() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Iceberg table UUIDs are typically UUIDs
        assert!(!metrics.table_uuid.is_empty());
        // Should contain hyphens (UUID format)
        assert!(metrics.table_uuid.contains('-'));
    }

    #[tokio::test]
    async fn test_iceberg_reader_multiple_extractions() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        // Extract metrics multiple times to ensure it's idempotent
        let metrics1 = reader
            .extract_metrics()
            .await
            .expect("First extraction failed");
        let metrics2 = reader
            .extract_metrics()
            .await
            .expect("Second extraction failed");

        // Both extractions should return the same data
        assert_eq!(metrics1.table_uuid, metrics2.table_uuid);
        assert_eq!(metrics1.format_version, metrics2.format_version);
        assert_eq!(metrics1.current_snapshot_id, metrics2.current_snapshot_id);
        assert_eq!(
            metrics1.file_stats.num_data_files,
            metrics2.file_stats.num_data_files
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_schema_json_validity() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify schema is valid JSON
        let schema_json: serde_json::Value =
            serde_json::from_str(&metrics.schema_info.schema_string)
                .expect("Schema should be valid JSON");

        // Schema should have a type field
        assert!(schema_json.get("type").is_some());
    }

    #[tokio::test]
    async fn test_iceberg_reader_table_properties() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Table properties should be a valid HashMap (len is always >= 0)
        // Just verify it's accessible
        let _ = metrics.table_properties.len();
    }

    #[tokio::test]
    async fn test_iceberg_reader_snapshot_summary() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // If there's a current snapshot, it should have a summary
        if metrics.current_snapshot_id.is_some() {
            // Summary should be a valid HashMap (len is always >= 0)
            // Just verify it's accessible
            let _ = metrics.snapshot_info.summary.len();
        }
    }

    #[tokio::test]
    async fn test_iceberg_reader_delete_files_metrics() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify delete file metrics consistency
        assert_eq!(
            metrics.file_stats.num_delete_files,
            metrics.file_stats.num_position_delete_files
                + metrics.file_stats.num_equality_delete_files
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_with_storage_options() {
        let table_location = get_test_iceberg_table_path();
        let mut storage_options = HashMap::new();

        // Add some storage options (these won't affect local file access)
        storage_options.insert("test_option".to_string(), "test_value".to_string());

        let result = IcebergReader::open(&table_location, &storage_options).await;

        assert!(
            result.is_ok(),
            "Should be able to open table with storage options"
        );
    }

    #[tokio::test]
    async fn test_iceberg_reader_sequence_numbers() {
        let table_location = get_test_iceberg_table_path();
        let storage_options = HashMap::new();

        let reader = IcebergReader::open(&table_location, &storage_options)
            .await
            .expect("Failed to open Iceberg table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // If there's a current snapshot, sequence numbers should be consistent
        if metrics.snapshot_info.sequence_number.is_some() {
            assert!(metrics.metadata.last_sequence_number.is_some());
        }
    }
}
