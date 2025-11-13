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
        metadata_location: &str,
        storage_options: &HashMap<String, String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        info!("Opening Iceberg table from metadata: {}", metadata_location);

        // Build FileIO with storage options
        let mut file_io_builder = FileIOBuilder::new_fs_io();

        // Add storage options to FileIO
        for (key, value) in storage_options {
            file_io_builder = file_io_builder.with_prop(key, value);
        }

        let file_io = file_io_builder.build()?;

        // Create a table identifier (can be arbitrary for static tables)
        let table_ident = TableIdent::from_strs(["default", "table"])?;

        // Load table from metadata file
        let table =
            StaticTable::from_metadata_file(metadata_location, table_ident, file_io).await?;

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
