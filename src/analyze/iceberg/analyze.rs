use crate::analyze::metrics::{
    ClusteringInfo, DeletionVectorMetrics, FileCompactionMetrics, HealthMetrics,
    SchemaEvolutionMetrics, TableConstraintsMetrics, TimeTravelMetrics,
};
use crate::analyze::table_analyzer::TableAnalyzer;
use crate::storage::{FileMetadata, StorageProvider};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use serde_json::Value;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::info;

/// Represents a schema change event in an Iceberg table's history.
///
/// This structure captures information about schema modifications, including
/// the version, timestamp, the actual schema definition, and whether the change
/// is backward-compatible or breaking.
///
/// # Fields
///
/// * `version` - The Iceberg table version when this schema change occurred
/// * `timestamp` - Unix timestamp (in milliseconds) when the change was made
/// * `schema` - The complete schema definition as a JSON value
/// * `is_breaking` - Whether this change breaks backward compatibility (e.g., column removal, type changes)
#[derive(Debug, Clone)]
pub struct SchemaChange {
    #[allow(dead_code)]
    pub version: u64,
    pub timestamp: u64,
    pub schema: Value,
    pub is_breaking: bool,
}

/// Intermediate structure to hold aggregated results from parallel metadata processing.
///
/// This structure accumulates metrics extracted from Iceberg metadata files
/// during parallel processing. It serves as a temporary container before the final
/// metrics are computed and stored in the `HealthMetrics` structure.
///
/// # Fields
///
/// * `partition_columns` - Columns used for table partitioning
/// * `total_snapshots` - Total number of table snapshots/versions
/// * `total_historical_size` - Total size of historical data in bytes
/// * `oldest_timestamp` - Timestamp of the oldest snapshot (milliseconds)
/// * `newest_timestamp` - Timestamp of the newest snapshot (milliseconds)
/// * `total_constraints` - Total number of table constraints
/// * `check_constraints` - Number of CHECK constraints
/// * `not_null_constraints` - Number of NOT NULL constraints
/// * `unique_constraints` - Number of UNIQUE constraints
/// * `foreign_key_constraints` - Number of FOREIGN KEY constraints
/// * `sort_order_columns` - Columns used for sorting/ordering
/// * `schema_changes` - List of all schema changes detected
/// * `delete_files_count` - Number of delete files (Iceberg's merge-on-read feature)
/// * `delete_files_size` - Total size of delete files in bytes
#[derive(Debug, Default)]
pub struct MetadataProcessingResult {
    pub partition_columns: Vec<String>,
    pub total_snapshots: usize,
    pub total_historical_size: u64,
    pub oldest_timestamp: u64,
    pub newest_timestamp: u64,
    pub total_constraints: usize,
    pub check_constraints: usize,
    pub not_null_constraints: usize,
    pub unique_constraints: usize,
    pub foreign_key_constraints: usize,
    pub sort_order_columns: Vec<String>,
    pub schema_changes: Vec<SchemaChange>,
    pub delete_files_count: u64,
    pub delete_files_size: u64,
}

/// Apache Iceberg-specific analyzer for processing Iceberg metadata.
///
/// This analyzer implements the `TableAnalyzer` trait and provides functionality
/// to parse Iceberg metadata files, extract metrics, and analyze table health.
/// It supports parallel processing of metadata files for improved performance.
///
/// # Fields
///
/// * `storage_provider` - The storage backend used to read files (S3, ADLS, local, etc.)
/// * `parallelism` - Number of concurrent tasks for parallel metadata processing
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use lake_pulse::storage::StorageProvider;
/// use lake_pulse::analyze::iceberg::IcebergAnalyzer;
///
/// # async fn example(storage: Arc<dyn StorageProvider>) {
/// let analyzer = IcebergAnalyzer::new(storage, 4);
/// // Use analyzer to process Iceberg tables
/// # }
/// ```
pub struct IcebergAnalyzer {
    storage_provider: Arc<dyn StorageProvider>,
    parallelism: usize,
}

impl IcebergAnalyzer {
    /// Create a new IcebergAnalyzer.
    ///
    /// # Arguments
    ///
    /// * `storage_provider` - The storage provider to use for reading files
    /// * `parallelism` - The number of concurrent tasks to use for metadata processing
    ///
    /// # Returns
    ///
    /// A new `IcebergAnalyzer` instance configured with the specified storage provider and parallelism.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use lake_pulse::storage::StorageProvider;
    /// use lake_pulse::analyze::iceberg::IcebergAnalyzer;
    ///
    /// # async fn example(storage: Arc<dyn StorageProvider>) {
    /// let analyzer = IcebergAnalyzer::new(storage, 4);
    /// # }
    /// ```
    pub fn new(storage_provider: Arc<dyn StorageProvider>, parallelism: usize) -> Self {
        Self {
            storage_provider,
            parallelism,
        }
    }

    /// Categorize files into data files and Iceberg metadata files.
    ///
    /// Separates Parquet data files from Iceberg metadata files (metadata.json, manifest files).
    /// Data files are identified by `.parquet` extension, while metadata files include
    /// metadata.json and manifest files.
    ///
    /// # Arguments
    ///
    /// * `objects` - All files discovered in the table location
    ///
    /// # Returns
    ///
    /// A tuple of `(data_files, metadata_files)` where:
    /// * `data_files` - Vector of Parquet data files
    /// * `metadata_files` - Vector of Iceberg metadata and manifest files
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::iceberg::IcebergAnalyzer;
    /// # use lake_pulse::storage::FileMetadata;
    /// # use std::sync::Arc;
    /// # fn example(analyzer: &IcebergAnalyzer, files: Vec<FileMetadata>) {
    /// let (data_files, metadata_files) = analyzer.categorize_iceberg_files(files);
    /// println!("Found {} data files and {} metadata files",
    ///          data_files.len(), metadata_files.len());
    /// # }
    /// ```
    pub fn categorize_iceberg_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        let mut data_files = Vec::new();
        let mut metadata_files = Vec::new();

        for obj in objects {
            if obj.path.ends_with(".parquet") {
                data_files.push(obj);
            } else if obj.path.contains("metadata.json") || obj.path.contains("manifest") {
                metadata_files.push(obj);
            }
        }

        (data_files, metadata_files)
    }

    /// Find referenced files from Iceberg metadata.
    ///
    /// Parses Iceberg metadata.json and manifest files to extract all file paths
    /// referenced in the current table snapshot. This identifies which data files
    /// are currently part of the table's active state.
    ///
    /// # Arguments
    ///
    /// * `metadata_files` - The metadata files to parse
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<String>)` - Vector of file paths referenced in the Iceberg metadata
    /// * `Err` - If file reading or JSON parsing fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Any metadata file cannot be read from storage
    /// * JSON parsing of metadata or manifest files fails
    /// * File content is not valid UTF-8
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::iceberg::IcebergAnalyzer;
    /// # use lake_pulse::storage::FileMetadata;
    /// # async fn example(analyzer: &IcebergAnalyzer, metadata: &Vec<FileMetadata>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let referenced_files = analyzer.find_referenced_files(metadata).await?;
    /// println!("Found {} referenced data files", referenced_files.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_referenced_files(
        &self,
        metadata_files: &Vec<FileMetadata>,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let storage_provider = Arc::clone(&self.storage_provider);
        let metadata_files_owned = metadata_files.clone();

        // Find the current metadata.json file (usually the one with highest version or named metadata.json)
        let metadata_json_files: Vec<_> = metadata_files_owned
            .iter()
            .filter(|f| {
                f.path.ends_with("metadata.json")
                    || f.path.contains("v") && f.path.ends_with(".json")
            })
            .cloned()
            .collect();

        if metadata_json_files.is_empty() {
            info!("No Iceberg metadata.json files found");
            return Ok(Vec::new());
        }

        // Process the most recent metadata file
        let metadata_file = metadata_json_files.last().unwrap();
        info!("Processing metadata file={}", &metadata_file.path);

        let content = storage_provider
            .read_file(&metadata_file.path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let content_str = String::from_utf8_lossy(&content);
        let json: Value = serde_json::from_str(&content_str)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let mut referenced_files = Vec::new();

        // Get manifest list from current snapshot
        let manifest_list_paths = self.get_manifest_list(&json).await?;

        // For each manifest in the manifest list, extract data file paths
        for manifest_path in &manifest_list_paths {
            info!("Reading manifest file: {}", manifest_path);

            match storage_provider.read_file(manifest_path).await {
                Ok(manifest_content) => {
                    // Try to parse as JSON (some manifests might be JSON)
                    if let Ok(manifest_str) = String::from_utf8(manifest_content.clone()) {
                        if let Ok(manifest_json) = serde_json::from_str::<Value>(&manifest_str) {
                            // Extract data file paths from manifest entries
                            if let Some(entries) =
                                manifest_json.get("entries").and_then(|e| e.as_array())
                            {
                                for entry in entries {
                                    if let Some(data_file) = entry.get("data-file") {
                                        if let Some(file_path) =
                                            data_file.get("file-path").and_then(|p| p.as_str())
                                        {
                                            referenced_files.push(file_path.to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Note: Iceberg manifests are typically Avro files, not JSON
                    // A full implementation would use an Avro parser here
                    // For now, we're doing a best-effort JSON parse
                }
                Err(e) => {
                    info!("Could not read manifest file {}: {}", manifest_path, e);
                }
            }
        }

        info!(
            "Found {} referenced files from manifests",
            referenced_files.len()
        );
        Ok(referenced_files)
    }

    /// Get manifest list paths from metadata JSON
    async fn get_manifest_list(
        &self,
        metadata: &Value,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut manifest_list = Vec::new();

        // Get current snapshot ID
        if let Some(current_snapshot_id) = metadata
            .get("current-snapshot-id")
            .and_then(|id| id.as_i64())
        {
            // Find the snapshot with this ID
            if let Some(snapshots) = metadata.get("snapshots").and_then(|s| s.as_array()) {
                for snapshot in snapshots {
                    if let Some(snapshot_id) =
                        snapshot.get("snapshot-id").and_then(|id| id.as_i64())
                    {
                        if snapshot_id == current_snapshot_id {
                            // Get manifest list path
                            if let Some(manifest_list_path) =
                                snapshot.get("manifest-list").and_then(|p| p.as_str())
                            {
                                manifest_list.push(manifest_list_path.to_string());
                            }
                            break;
                        }
                    }
                }
            }
        }

        Ok(manifest_list)
    }

    /// Process a single Iceberg metadata file and extract metrics
    pub async fn process_single_metadata_file(
        &self,
        file_path: &str,
        file_index: usize,
    ) -> Result<MetadataProcessingResult, Box<dyn Error + Send + Sync>> {
        let content = self.storage_provider.read_file(file_path).await?;
        let content_str = String::from_utf8_lossy(&content);

        let mut result = MetadataProcessingResult {
            oldest_timestamp: chrono::Utc::now().timestamp() as u64,
            ..Default::default()
        };

        let json: Value = serde_json::from_str(&content_str)?;

        let current_version = file_index as u64;

        // Extract partition spec
        if let Some(partition_specs) = json.get("partition-specs").and_then(|s| s.as_array()) {
            for spec in partition_specs {
                if let Some(fields) = spec.get("fields").and_then(|f| f.as_array()) {
                    let partition_columns: Vec<String> = fields
                        .iter()
                        .filter_map(|f| {
                            f.get("name")
                                .and_then(|n| n.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect();
                    if !partition_columns.is_empty() && result.partition_columns.is_empty() {
                        result.partition_columns = partition_columns;
                    }
                }
            }
        }

        // Extract sort order
        if let Some(sort_orders) = json.get("sort-orders").and_then(|s| s.as_array()) {
            for order in sort_orders {
                if let Some(fields) = order.get("fields").and_then(|f| f.as_array()) {
                    let sort_columns: Vec<String> = fields
                        .iter()
                        .filter_map(|f| {
                            f.get("source-id")
                                .and_then(|_| f.get("transform").and_then(|t| t.as_str()))
                                .map(|s| s.to_string())
                        })
                        .collect();
                    if !sort_columns.is_empty() && result.sort_order_columns.is_empty() {
                        result.sort_order_columns = sort_columns;
                    }
                }
            }
        }

        // Extract snapshot metrics
        self.extract_snapshot_metrics(&json, &mut result);

        // Extract schema and constraints
        self.extract_schema_and_constraints(&json, &mut result, current_version);

        Ok(result)
    }

    /// Extract snapshot/time travel metrics from Iceberg metadata.
    ///
    /// Parses snapshot information from Iceberg metadata to collect timestamp data,
    /// estimate historical data size, and extract delete file metrics (for Iceberg v2+).
    ///
    /// # Arguments
    ///
    /// * `json` - The Iceberg metadata JSON
    /// * `result` - The result object to update (mutated in place)
    fn extract_snapshot_metrics(&self, json: &Value, result: &mut MetadataProcessingResult) {
        if let Some(snapshots) = json.get("snapshots").and_then(|s| s.as_array()) {
            result.total_snapshots = snapshots.len();

            for snapshot in snapshots {
                if let Some(timestamp_ms) = snapshot.get("timestamp-ms").and_then(|t| t.as_u64()) {
                    result.oldest_timestamp = result.oldest_timestamp.min(timestamp_ms);
                    result.newest_timestamp = result.newest_timestamp.max(timestamp_ms);
                }

                // Estimate snapshot size from summary
                if let Some(summary) = snapshot.get("summary").and_then(|s| s.as_object()) {
                    if let Some(total_data_files) =
                        summary.get("total-data-files").and_then(|v| v.as_str())
                    {
                        // Rough estimate based on file count
                        if let Ok(file_count) = total_data_files.parse::<u64>() {
                            result.total_historical_size += file_count * 1024 * 1024;
                            // Estimate 1MB per file
                        }
                    }

                    // Extract delete file metrics (Iceberg v2+)
                    if let Some(total_delete_files) =
                        summary.get("total-delete-files").and_then(|v| v.as_str())
                    {
                        if let Ok(delete_count) = total_delete_files.parse::<u64>() {
                            result.delete_files_count += delete_count;
                        }
                    }

                    if let Some(total_position_deletes) = summary
                        .get("total-position-deletes")
                        .and_then(|v| v.as_str())
                    {
                        if let Ok(position_deletes) = total_position_deletes.parse::<u64>() {
                            result.delete_files_count += position_deletes;
                        }
                    }

                    if let Some(total_equality_deletes) = summary
                        .get("total-equality-deletes")
                        .and_then(|v| v.as_str())
                    {
                        if let Ok(equality_deletes) = total_equality_deletes.parse::<u64>() {
                            result.delete_files_count += equality_deletes;
                        }
                    }
                }
            }
        }
    }

    /// Extract schema and constraints from Iceberg metadata.
    ///
    /// Parses schema definitions to identify and count various types of constraints
    /// and detect schema changes over time.
    ///
    /// # Arguments
    ///
    /// * `json` - The Iceberg metadata JSON
    /// * `result` - The result object to update (mutated in place)
    /// * `current_version` - The current version of the schema
    fn extract_schema_and_constraints(
        &self,
        json: &Value,
        result: &mut MetadataProcessingResult,
        current_version: u64,
    ) {
        if let Some(schemas) = json.get("schemas").and_then(|s| s.as_array()) {
            for (idx, schema) in schemas.iter().enumerate() {
                if let Some(fields) = schema.get("fields").and_then(|f| f.as_array()) {
                    let constraints = self.extract_constraints_from_schema(fields);
                    result.total_constraints += constraints.0;
                    result.check_constraints += constraints.1;
                    result.not_null_constraints += constraints.2;
                    result.unique_constraints += constraints.3;
                    result.foreign_key_constraints += constraints.4;

                    // Track schema changes
                    let is_breaking = if idx > 0 {
                        self.is_breaking_change(&result.schema_changes, schema)
                    } else {
                        false
                    };

                    result.schema_changes.push(SchemaChange {
                        version: current_version + idx as u64,
                        timestamp: 0, // Iceberg doesn't track schema change timestamps directly
                        schema: schema.clone(),
                        is_breaking,
                    });
                }
            }
        }
    }

    /// Update health metrics from Iceberg metadata files.
    ///
    /// Main entry point for extracting comprehensive health metrics from Iceberg
    /// metadata. Processes all metadata files in parallel, aggregates results, and
    /// populates the HealthMetrics structure with delete file metrics, schema evolution,
    /// time travel, constraint, partition, and compaction metrics.
    ///
    /// # Arguments
    ///
    /// * `metadata_files` - The metadata files to analyze
    /// * `data_files_total_size` - Total size of all data files in bytes
    /// * `data_files_total_files` - Total number of data files
    /// * `metrics` - The metrics object to update (mutated in place)
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the metrics extraction.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Any metadata file cannot be read or parsed
    /// * Schema metric calculation fails
    /// * Parallel processing encounters errors
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::iceberg::IcebergAnalyzer;
    /// # use lake_pulse::analyze::metrics::HealthMetrics;
    /// # use lake_pulse::storage::FileMetadata;
    /// # async fn example(analyzer: &IcebergAnalyzer, metadata: &Vec<FileMetadata>, mut metrics: HealthMetrics) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// analyzer.update_metrics_from_iceberg_metadata(
    ///     metadata,
    ///     1024 * 1024 * 1024, // 1GB total data size
    ///     100,                 // 100 data files
    ///     &mut metrics
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_metrics_from_iceberg_metadata(
        &self,
        metadata_files: &Vec<FileMetadata>,
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let parallelism = self.parallelism.max(1);

        // Filter to only process actual metadata JSON files, not manifest files or CRC files
        let metadata_json_files: Vec<_> = metadata_files
            .iter()
            .filter(|f| {
                (f.path.ends_with("metadata.json")
                    || f.path.contains("/v") && f.path.ends_with(".json"))
                    && !f.path.ends_with(".crc")
            })
            .cloned()
            .collect();

        if metadata_json_files.is_empty() {
            info!("No Iceberg metadata JSON files found to process");
            return Ok(());
        }

        let min_file_name = metadata_json_files
            .iter()
            .min_by_key(|f| f.path.clone())
            .map(|f| f.path.clone());
        let max_file_name = metadata_json_files
            .iter()
            .max_by_key(|f| f.path.clone())
            .map(|f| f.path.clone());
        metrics.metadata_health.first_file_name = min_file_name.clone();
        metrics.metadata_health.last_file_name = max_file_name.clone();

        info!(
            "Updating the metrics from Iceberg metadata files, parallelism={}, metadata_files_count={}, \
            data_files_count={}, min_file={:?}, max_file={:?}",
            parallelism,
            metadata_json_files.len(),
            data_files_total_files,
            min_file_name,
            max_file_name
        );

        let update_metrics_start = SystemTime::now();
        let metadata_files_owned: Vec<_> =
            metadata_json_files.iter().cloned().enumerate().collect();
        let results: Vec<Result<MetadataProcessingResult, Box<dyn Error + Send + Sync>>> =
            stream::iter(metadata_files_owned)
                .map(|(index, metadata_file)| {
                    info!(
                        "Starting processing Iceberg metadata file={}, index={}",
                        &metadata_file.path.clone(),
                        index
                    );
                    let path = metadata_file.path.clone();
                    let analyzer = IcebergAnalyzer {
                        storage_provider: Arc::clone(&self.storage_provider),
                        parallelism,
                    };

                    let result = async move {
                        let process_single_start = SystemTime::now();
                        let r = analyzer.process_single_metadata_file(&path, index).await;
                        info!(
                            "Processed Iceberg metadata file={} in async, took={}",
                            &path,
                            process_single_start
                                .elapsed()
                                .unwrap_or_default()
                                .as_millis(),
                        );
                        r
                    };
                    result
                })
                .buffer_unordered(parallelism)
                .collect()
                .await;

        info!(
            "Finished the metrics from Iceberg metadata files, parallelism={}, metadata_files_count={}, \
            data_files_count={}, min_file={:?}, max_file={:?}, took={}",
            parallelism,
            metadata_files.len(),
            data_files_total_files,
            min_file_name,
            max_file_name,
            update_metrics_start
                .elapsed()
                .unwrap_or_default()
                .as_millis(),
        );

        // Aggregate results from all metadata files
        let mut partition_columns: Vec<String> = vec![];
        let mut total_snapshots = 0;
        let mut total_historical_size = 0u64;
        let mut oldest_timestamp = chrono::Utc::now().timestamp() as u64;
        let mut newest_timestamp = 0u64;
        let mut total_constraints = 0;
        let mut check_constraints = 0;
        let mut not_null_constraints = 0;
        let mut unique_constraints = 0;
        let mut foreign_key_constraints = 0;
        let mut sort_order_columns: Vec<String> = vec![];
        let mut schema_changes = Vec::new();
        let mut current_version = 0u64;
        let mut delete_files_count = 0u64;

        for result in results {
            let r = result?;

            if partition_columns.is_empty() && !r.partition_columns.is_empty() {
                partition_columns = r.partition_columns;
            }

            total_snapshots += r.total_snapshots;
            total_historical_size += r.total_historical_size;
            oldest_timestamp = oldest_timestamp.min(r.oldest_timestamp);
            newest_timestamp = newest_timestamp.max(r.newest_timestamp);

            total_constraints += r.total_constraints;
            check_constraints += r.check_constraints;
            not_null_constraints += r.not_null_constraints;
            unique_constraints += r.unique_constraints;
            foreign_key_constraints += r.foreign_key_constraints;
            delete_files_count += r.delete_files_count;

            if sort_order_columns.is_empty() {
                sort_order_columns = r.sort_order_columns;
            }

            schema_changes.extend(r.schema_changes);
            current_version = current_version.max(schema_changes.len() as u64);
        }

        // Set schema evolution metrics
        if !schema_changes.is_empty() {
            metrics.schema_evolution =
                self.calculate_schema_metrics(schema_changes, current_version)?;
        }

        // Set deletion vector metrics (delete files in Iceberg)
        if delete_files_count > 0 {
            let deletion_vector_metrics = self.calculate_deletion_vector_metrics(
                delete_files_count,
                oldest_timestamp,
                newest_timestamp,
            );
            metrics.deletion_vector_metrics = Some(deletion_vector_metrics);
        }

        // Set time travel metrics
        if total_snapshots != 0 {
            let now = chrono::Utc::now().timestamp() as u64;
            let oldest_age_days = (now - oldest_timestamp / 1000) as f64 / 86400.0;
            let newest_age_days = (now - newest_timestamp / 1000) as f64 / 86400.0;
            let avg_snapshot_size = total_historical_size as f64 / total_snapshots as f64;

            let storage_cost_impact = self.calculate_storage_cost_impact(
                total_historical_size,
                total_snapshots,
                oldest_age_days,
            );
            let retention_efficiency = self.calculate_retention_efficiency(
                total_snapshots,
                oldest_age_days,
                newest_age_days,
            );
            let recommended_retention =
                self.calculate_recommended_retention(total_snapshots, oldest_age_days);

            metrics.time_travel_metrics = Some(TimeTravelMetrics {
                total_snapshots,
                oldest_snapshot_age_days: oldest_age_days,
                newest_snapshot_age_days: newest_age_days,
                total_historical_size_bytes: total_historical_size,
                avg_snapshot_size_bytes: avg_snapshot_size,
                storage_cost_impact_score: storage_cost_impact,
                retention_efficiency_score: retention_efficiency,
                recommended_retention_days: recommended_retention,
            });
        }

        // Set table constraints metrics
        if total_constraints != 0 {
            let constraint_violation_risk =
                self.calculate_constraint_violation_risk(total_constraints, check_constraints);
            let data_quality_score =
                self.calculate_data_quality_score(total_constraints, constraint_violation_risk);
            let constraint_coverage_score =
                self.calculate_constraint_coverage_score(total_constraints, check_constraints);

            metrics.table_constraints = Some(TableConstraintsMetrics {
                total_constraints,
                check_constraints,
                not_null_constraints,
                unique_constraints,
                foreign_key_constraints,
                constraint_violation_risk,
                data_quality_score,
                constraint_coverage_score,
            });
        }

        // For Iceberg, use partition columns for clustering
        let partition_count = metrics.partitions.len();
        let cluster_count = partition_count.max(1);
        let avg_files_per_cluster = if cluster_count > 0 {
            data_files_total_files as f64 / cluster_count as f64
        } else {
            0.0
        };

        let avg_cluster_size_bytes = if cluster_count > 0 {
            data_files_total_size as f64 / cluster_count as f64
        } else {
            0.0
        };

        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: partition_columns.to_vec(),
            cluster_count,
            avg_files_per_cluster,
            avg_cluster_size_bytes,
        });

        // Calculate file compaction metrics
        let file_compaction = self.calculate_file_compaction_metrics(
            data_files_total_files,
            data_files_total_size,
            &sort_order_columns,
        );
        metrics.file_compaction = Some(file_compaction);

        Ok(())
    }

    /// Calculate file compaction metrics
    fn calculate_file_compaction_metrics(
        &self,
        total_files: usize,
        total_size: u64,
        sort_order_columns: &[String],
    ) -> FileCompactionMetrics {
        // Estimate small files (< 16MB) based on average file size
        let avg_file_size = if total_files > 0 {
            total_size as f64 / total_files as f64
        } else {
            0.0
        };

        let small_file_threshold = 16 * 1024 * 1024; // 16MB
        let small_files_count = if avg_file_size < small_file_threshold as f64 {
            (total_files as f64 * 0.7).ceil() as usize // Estimate 70% are small
        } else {
            (total_files as f64 * 0.2).ceil() as usize // Estimate 20% are small
        };

        let small_files_size =
            (small_files_count as f64 * avg_file_size.min(small_file_threshold as f64)) as u64;

        // Calculate potential savings
        let target_file_size = 128 * 1024 * 1024; // 128MB target
        let estimated_savings = if small_files_count > 1 {
            let files_per_target =
                (target_file_size as f64 / avg_file_size.max(1.0)).ceil() as usize;
            let target_files = (small_files_count as f64 / files_per_target as f64).ceil() as usize;
            let estimated_target_size = target_files as u64 * target_file_size / 2;
            small_files_size.saturating_sub(estimated_target_size)
        } else {
            0
        };

        let compaction_opportunity =
            self.calculate_compaction_opportunity(small_files_count, total_files);

        let compaction_priority =
            self.calculate_compaction_priority(compaction_opportunity, small_files_count);

        FileCompactionMetrics {
            compaction_opportunity_score: compaction_opportunity,
            small_files_count,
            small_files_size_bytes: small_files_size,
            potential_compaction_files: small_files_count,
            estimated_compaction_savings_bytes: estimated_savings,
            recommended_target_file_size_bytes: target_file_size,
            compaction_priority,
            z_order_opportunity: !sort_order_columns.is_empty(),
            z_order_columns: sort_order_columns.to_vec(),
        }
    }

    /// Calculate compaction opportunity score
    fn calculate_compaction_opportunity(&self, small_files: usize, total_files: usize) -> f64 {
        if total_files == 0 {
            return 0.0;
        }

        let small_file_ratio = small_files as f64 / total_files as f64;

        if small_file_ratio > 0.8 {
            1.0
        } else if small_file_ratio > 0.6 {
            0.8
        } else if small_file_ratio > 0.4 {
            0.6
        } else if small_file_ratio > 0.2 {
            0.4
        } else {
            0.2
        }
    }

    /// Calculate compaction priority
    fn calculate_compaction_priority(&self, opportunity_score: f64, small_files: usize) -> String {
        if opportunity_score > 0.8 || small_files > 100 {
            "critical".to_string()
        } else if opportunity_score > 0.6 || small_files > 50 {
            "high".to_string()
        } else if opportunity_score > 0.4 || small_files > 20 {
            "medium".to_string()
        } else {
            "low".to_string()
        }
    }

    /// Calculate deletion vector metrics from delete files
    fn calculate_deletion_vector_metrics(
        &self,
        delete_files_count: u64,
        oldest_timestamp: u64,
        _newest_timestamp: u64,
    ) -> DeletionVectorMetrics {
        // Estimate deletion vector size (rough estimate: 1KB per delete file)
        let total_dv_size = delete_files_count * 1024;
        let avg_dv_size = if delete_files_count > 0 {
            total_dv_size as f64 / delete_files_count as f64
        } else {
            0.0
        };

        // Calculate age in days
        let now = chrono::Utc::now().timestamp() as u64;
        let oldest_age_days = if oldest_timestamp > 0 {
            (now - oldest_timestamp / 1000) as f64 / 86400.0
        } else {
            0.0
        };

        // Estimate deleted rows (rough estimate: 100 rows per delete file)
        let deleted_rows = delete_files_count * 100;

        // Calculate impact score
        let impact_score =
            self.calculate_deletion_impact_score(delete_files_count, oldest_age_days);

        DeletionVectorMetrics {
            deletion_vector_count: delete_files_count as usize,
            total_deletion_vector_size_bytes: total_dv_size,
            avg_deletion_vector_size_bytes: avg_dv_size,
            deletion_vector_age_days: oldest_age_days,
            deleted_rows_count: deleted_rows,
            deletion_vector_impact_score: impact_score,
        }
    }

    /// Calculate deletion impact score
    fn calculate_deletion_impact_score(&self, delete_count: u64, age_days: f64) -> f64 {
        let mut score: f64 = 0.0;

        // More delete files = higher impact
        if delete_count > 1000 {
            score += 0.5;
        } else if delete_count > 500 {
            score += 0.4;
        } else if delete_count > 100 {
            score += 0.3;
        } else if delete_count > 10 {
            score += 0.2;
        } else {
            score += 0.1;
        }

        // Older delete files = higher impact
        if age_days > 90.0 {
            score += 0.5;
        } else if age_days > 30.0 {
            score += 0.3;
        } else if age_days > 7.0 {
            score += 0.2;
        } else {
            score += 0.1;
        }

        score.min(1.0)
    }

    /// Calculate schema metrics from schema changes
    fn calculate_schema_metrics(
        &self,
        changes: Vec<SchemaChange>,
        current_version: u64,
    ) -> Result<Option<SchemaEvolutionMetrics>, Box<dyn Error + Send + Sync>> {
        let total_changes = changes.len();
        let breaking_changes = changes.iter().filter(|c| c.is_breaking).count();
        let non_breaking_changes = total_changes - breaking_changes;

        // Calculate time-based metrics
        let now = chrono::Utc::now().timestamp() as u64;
        let days_since_last = if let Some(last_change) = changes.last() {
            if last_change.timestamp > 0 {
                (now - last_change.timestamp / 1000) as f64 / 86400.0
            } else {
                365.0 // No timestamp available
            }
        } else {
            365.0 // No changes in a year = very stable
        };

        // Calculate change frequency (changes per day)
        let total_days = if changes.len() > 1 {
            let first_change = changes.first().unwrap().timestamp / 1000;
            let last_change = changes.last().unwrap().timestamp / 1000;
            if first_change > 0 && last_change > 0 {
                ((last_change - first_change) as f64 / 86400.0).max(1.0_f64)
            } else {
                1.0
            }
        } else {
            1.0
        };

        let change_frequency = total_changes as f64 / total_days;

        // Calculate stability score
        let stability_score = self.calculate_schema_stability_score(
            total_changes,
            breaking_changes,
            change_frequency,
            days_since_last,
        );

        Ok(Some(SchemaEvolutionMetrics {
            total_schema_changes: total_changes,
            breaking_changes,
            non_breaking_changes,
            schema_stability_score: stability_score,
            days_since_last_change: days_since_last,
            schema_change_frequency: change_frequency,
            current_schema_version: current_version,
        }))
    }

    /// Check if a schema change is breaking
    fn is_breaking_change(&self, previous_changes: &[SchemaChange], new_schema: &Value) -> bool {
        if previous_changes.is_empty() {
            return false;
        }

        let last_schema = &previous_changes.last().unwrap().schema;

        // Check for breaking changes:
        // 1. Column removal
        // 2. Column type changes
        // 3. Required field changes
        self.detect_breaking_schema_changes(last_schema, new_schema)
    }

    /// Detect breaking schema changes between two schemas
    fn detect_breaking_schema_changes(&self, old_schema: &Value, new_schema: &Value) -> bool {
        // Breaking change detection for Iceberg schemas
        if let (Some(old_fields), Some(new_fields)) =
            (old_schema.get("fields"), new_schema.get("fields"))
        {
            if let (Some(old_fields_array), Some(new_fields_array)) =
                (old_fields.as_array(), new_fields.as_array())
            {
                // Build maps of field names for easier lookup
                let old_field_names: HashSet<String> = old_fields_array
                    .iter()
                    .filter_map(|f| {
                        f.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();
                let new_field_names: HashSet<String> = new_fields_array
                    .iter()
                    .filter_map(|f| {
                        f.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();

                // If any old fields are missing, it's a breaking change
                if !old_field_names.is_subset(&new_field_names) {
                    return true;
                }

                // Check for type changes in existing fields
                for old_field in old_fields_array {
                    if let Some(field_name) = old_field.get("name").and_then(|n| n.as_str()) {
                        if let Some(new_field) = new_fields_array
                            .iter()
                            .find(|f| f.get("name").and_then(|n| n.as_str()) == Some(field_name))
                        {
                            let old_type = old_field.get("type").and_then(|t| t.as_str());
                            let new_type = new_field.get("type").and_then(|t| t.as_str());

                            // If types changed, it's a breaking change
                            if old_type != new_type {
                                return true;
                            }

                            // Check if required changed from false to true (breaking)
                            let old_required = old_field
                                .get("required")
                                .and_then(|r| r.as_bool())
                                .unwrap_or(false);
                            let new_required = new_field
                                .get("required")
                                .and_then(|r| r.as_bool())
                                .unwrap_or(false);

                            if !old_required && new_required {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        false
    }

    /// Calculate schema stability score
    fn calculate_schema_stability_score(
        &self,
        total_changes: usize,
        breaking_changes: usize,
        frequency: f64,
        days_since_last: f64,
    ) -> f64 {
        let mut score: f64 = 1.0;

        // Penalize total changes
        if total_changes > 50 {
            score -= 0.3;
        } else if total_changes > 20 {
            score -= 0.2;
        } else if total_changes > 10 {
            score -= 0.1;
        }

        // Penalize breaking changes heavily
        if breaking_changes > 10 {
            score -= 0.4;
        } else if breaking_changes > 5 {
            score -= 0.3;
        } else if breaking_changes > 0 {
            score -= 0.2;
        }

        // Penalize high frequency changes
        if frequency > 1.0 {
            // More than 1 change per day
            score -= 0.3;
        } else if frequency > 0.5 {
            // More than 1 change every 2 days
            score -= 0.2;
        } else if frequency > 0.1 {
            // More than 1 change every 10 days
            score -= 0.1;
        }

        // Reward stability (no recent changes)
        if days_since_last > 30.0 {
            score += 0.1;
        } else if days_since_last > 7.0 {
            score += 0.05;
        }

        score.clamp(0.0_f64, 1.0_f64)
    }

    /// Calculate storage cost impact
    fn calculate_storage_cost_impact(
        &self,
        total_size: u64,
        snapshot_count: usize,
        oldest_age: f64,
    ) -> f64 {
        let mut impact: f64 = 0.0;

        // Impact from total size
        let size_gb = total_size as f64 / (1024.0 * 1024.0 * 1024.0);
        if size_gb > 100.0 {
            impact += 0.4;
        } else if size_gb > 50.0 {
            impact += 0.3;
        } else if size_gb > 10.0 {
            impact += 0.2;
        } else if size_gb > 1.0 {
            impact += 0.1;
        }

        // Impact from snapshot count
        if snapshot_count > 1000 {
            impact += 0.3;
        } else if snapshot_count > 500 {
            impact += 0.2;
        } else if snapshot_count > 100 {
            impact += 0.1;
        }

        // Impact from age (older snapshots = higher cost)
        if oldest_age > 365.0 {
            impact += 0.3;
        } else if oldest_age > 90.0 {
            impact += 0.2;
        } else if oldest_age > 30.0 {
            impact += 0.1;
        }

        impact.min(1.0_f64)
    }

    /// Calculate retention efficiency
    fn calculate_retention_efficiency(
        &self,
        snapshot_count: usize,
        oldest_age: f64,
        newest_age: f64,
    ) -> f64 {
        let mut efficiency: f64 = 1.0;

        // Penalize too many snapshots
        if snapshot_count > 1000 {
            efficiency -= 0.4;
        } else if snapshot_count > 500 {
            efficiency -= 0.3;
        } else if snapshot_count > 100 {
            efficiency -= 0.2;
        } else if snapshot_count > 50 {
            efficiency -= 0.1;
        }

        // Reward appropriate retention period
        let retention_days = oldest_age - newest_age;
        if retention_days > 365.0 {
            efficiency -= 0.2; // Too long retention
        } else if retention_days < 7.0 {
            efficiency -= 0.1; // Too short retention
        }

        efficiency.clamp(0.0_f64, 1.0_f64)
    }

    /// Calculate recommended retention period
    fn calculate_recommended_retention(&self, snapshot_count: usize, oldest_age: f64) -> u64 {
        // Simple heuristic: recommend retention based on snapshot count and age
        if snapshot_count > 1000 || oldest_age > 365.0 {
            30 // 30 days for high snapshot count or very old data
        } else if snapshot_count > 500 || oldest_age > 90.0 {
            60 // 60 days for medium snapshot count or old data
        } else if snapshot_count > 100 || oldest_age > 30.0 {
            90 // 90 days for moderate snapshot count or recent data
        } else {
            180 // 180 days for low snapshot count and recent data
        }
    }

    /// Extract constraints from Iceberg schema fields
    fn extract_constraints_from_schema(
        &self,
        fields: &Vec<Value>,
    ) -> (usize, usize, usize, usize, usize) {
        let mut total = 0;
        let mut check = 0;
        let mut not_null = 0;
        let mut unique = 0;
        let mut foreign_key = 0;

        for field in fields {
            total += 1;

            // Check for NOT NULL constraint (required field in Iceberg)
            if let Some(required) = field.get("required") {
                if required.as_bool().unwrap_or(false) {
                    not_null += 1;
                }
            }

            // Check for other constraints in field metadata
            if let Some(metadata) = field.get("metadata").and_then(|m| m.as_object()) {
                for (key, _value) in metadata {
                    let key_lower = key.to_lowercase();
                    if key_lower.contains("constraint") || key_lower.contains("check") {
                        check += 1;
                    }
                    if key_lower.contains("unique") {
                        unique += 1;
                    }
                    if key_lower.contains("foreign") || key_lower.contains("reference") {
                        foreign_key += 1;
                    }
                }
            }

            // Also check field doc strings
            if let Some(doc) = field.get("doc").and_then(|d| d.as_str()) {
                let doc_lower = doc.to_lowercase();
                if doc_lower.contains("constraint") || doc_lower.contains("check") {
                    check += 1;
                }
                if doc_lower.contains("unique") {
                    unique += 1;
                }
                if doc_lower.contains("foreign") || doc_lower.contains("reference") {
                    foreign_key += 1;
                }
            }
        }

        (total, check, not_null, unique, foreign_key)
    }

    /// Calculate constraint violation risk
    fn calculate_constraint_violation_risk(
        &self,
        total_constraints: usize,
        check_constraints: usize,
    ) -> f64 {
        if total_constraints == 0 {
            return 0.0;
        }

        // Higher risk with more complex constraints
        let complexity_ratio = check_constraints as f64 / total_constraints as f64;
        if complexity_ratio > 0.5 {
            0.8
        } else if complexity_ratio > 0.3 {
            0.6
        } else if complexity_ratio > 0.1 {
            0.4
        } else {
            0.2
        }
    }

    /// Calculate data quality score
    fn calculate_data_quality_score(&self, total_constraints: usize, violation_risk: f64) -> f64 {
        let mut score = 1.0;

        // Reward having constraints
        if total_constraints > 10 {
            score += 0.2;
        } else if total_constraints > 5 {
            score += 0.1;
        }

        // Penalize violation risk
        score -= violation_risk * 0.5;

        score.clamp(0.0_f64, 1.0_f64)
    }

    /// Calculate constraint coverage score
    fn calculate_constraint_coverage_score(
        &self,
        total_constraints: usize,
        check_constraints: usize,
    ) -> f64 {
        if total_constraints == 0 {
            return 0.0;
        }

        let coverage_ratio = check_constraints as f64 / total_constraints as f64;
        if coverage_ratio > 0.5 {
            1.0
        } else if coverage_ratio > 0.3 {
            0.7
        } else if coverage_ratio > 0.1 {
            0.4
        } else {
            0.1
        }
    }
}

// Implement the TableAnalyzer trait for IcebergAnalyzer
#[async_trait]
impl TableAnalyzer for IcebergAnalyzer {
    fn categorize_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        self.categorize_iceberg_files(objects)
    }

    async fn find_referenced_files(
        &self,
        metadata_files: &Vec<FileMetadata>,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        self.find_referenced_files(metadata_files).await
    }

    async fn update_metrics_from_metadata(
        &self,
        metadata_files: &Vec<FileMetadata>,
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.update_metrics_from_iceberg_metadata(
            metadata_files,
            data_files_total_size,
            data_files_total_files,
            metrics,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::error::StorageResult;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::OnceLock;

    // Mock storage provider for testing
    struct MockStorageProvider {
        options: OnceLock<HashMap<String, String>>,
    }

    impl MockStorageProvider {
        fn new() -> Self {
            Self {
                options: OnceLock::new(),
            }
        }
    }

    #[async_trait]
    impl StorageProvider for MockStorageProvider {
        fn base_path(&self) -> &str {
            "/mock/base/path"
        }

        async fn validate_connection(&self, _path: &str) -> StorageResult<()> {
            Ok(())
        }

        async fn list_files(
            &self,
            _path: &str,
            _recursive: bool,
        ) -> StorageResult<Vec<FileMetadata>> {
            Ok(vec![])
        }

        async fn discover_partitions(
            &self,
            _path: &str,
            _exclude_prefixes: Vec<&str>,
        ) -> StorageResult<Vec<String>> {
            Ok(vec![])
        }

        async fn list_files_parallel(
            &self,
            _path: &str,
            _partitions: Vec<String>,
            _parallelism: usize,
        ) -> StorageResult<Vec<FileMetadata>> {
            Ok(vec![])
        }

        async fn read_file(&self, _path: &str) -> StorageResult<Vec<u8>> {
            Ok(vec![])
        }

        async fn exists(&self, _path: &str) -> StorageResult<bool> {
            Ok(true)
        }

        async fn get_metadata(&self, _path: &str) -> StorageResult<FileMetadata> {
            Ok(FileMetadata {
                path: "test".to_string(),
                size: 0,
                last_modified: None,
            })
        }

        fn options(&self) -> &HashMap<String, String> {
            self.options.get_or_init(|| HashMap::new())
        }

        fn clean_options(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn uri_from_path(&self, path: &str) -> String {
            path.to_string()
        }
    }

    // Helper function to create a test IcebergAnalyzer
    fn create_test_analyzer() -> IcebergAnalyzer {
        let mock_storage = Arc::new(MockStorageProvider::new());
        IcebergAnalyzer::new(mock_storage, 4)
    }

    #[test]
    fn test_iceberg_analyzer_new() {
        let mock_storage = Arc::new(MockStorageProvider::new());
        let analyzer = IcebergAnalyzer::new(mock_storage, 8);

        assert_eq!(analyzer.parallelism, 8);
    }

    #[test]
    fn test_categorize_iceberg_files_data_only() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "table/data/part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "table/data/part-00001.parquet".to_string(),
                size: 2048,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_iceberg_files(files);

        assert_eq!(data_files.len(), 2);
        assert_eq!(metadata_files.len(), 0);
    }

    #[test]
    fn test_categorize_iceberg_files_metadata_only() {
        let analyzer = create_test_analyzer();
        let files = vec![FileMetadata {
            path: "table/metadata/v1.metadata.json".to_string(),
            size: 512,
            last_modified: None,
        }];

        let (data_files, metadata_files) = analyzer.categorize_iceberg_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 1);
    }

    #[test]
    fn test_categorize_iceberg_files_mixed() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "table/data/part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "table/metadata/v1.metadata.json".to_string(),
                size: 512,
                last_modified: None,
            },
            FileMetadata {
                path: "table/data/part-00001.parquet".to_string(),
                size: 2048,
                last_modified: None,
            },
            FileMetadata {
                path: "table/metadata/manifest-list.avro".to_string(),
                size: 256,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_iceberg_files(files);

        assert_eq!(data_files.len(), 2);
        assert_eq!(metadata_files.len(), 2);
    }

    #[test]
    fn test_categorize_iceberg_files_empty() {
        let analyzer = create_test_analyzer();
        let files = vec![];

        let (data_files, metadata_files) = analyzer.categorize_iceberg_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 0);
    }

    #[test]
    fn test_categorize_iceberg_files_non_parquet() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "table/data.csv".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "table/README.md".to_string(),
                size: 512,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_iceberg_files(files);

        // Non-parquet, non-metadata files should be ignored
        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 0);
    }

    #[test]
    fn test_categorize_iceberg_files_manifest_files() {
        let analyzer = create_test_analyzer();
        let files = vec![FileMetadata {
            path: "table/metadata/manifest-list.avro".to_string(),
            size: 256,
            last_modified: None,
        }];

        let (data_files, metadata_files) = analyzer.categorize_iceberg_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 1);
        assert!(metadata_files.iter().all(|f| f.path.contains("manifest")));
    }

    #[test]
    fn test_schema_change_creation() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "required": true}
            ]
        });

        let change = SchemaChange {
            version: 1,
            timestamp: 1234567890,
            schema: schema.clone(),
            is_breaking: false,
        };

        assert_eq!(change.version, 1);
        assert_eq!(change.timestamp, 1234567890);
        assert!(!change.is_breaking);
        assert_eq!(change.schema, schema);
    }

    #[test]
    fn test_schema_change_clone() {
        let schema = json!({"type": "struct", "fields": []});
        let change = SchemaChange {
            version: 1,
            timestamp: 1234567890,
            schema: schema.clone(),
            is_breaking: true,
        };

        let cloned = change.clone();
        assert_eq!(change.version, cloned.version);
        assert_eq!(change.timestamp, cloned.timestamp);
        assert_eq!(change.is_breaking, cloned.is_breaking);
    }

    #[test]
    fn test_schema_change_debug() {
        let schema = json!({"type": "struct"});
        let change = SchemaChange {
            version: 1,
            timestamp: 1234567890,
            schema,
            is_breaking: false,
        };

        let debug_str = format!("{:?}", change);
        assert!(!debug_str.is_empty());
        assert!(debug_str.contains("SchemaChange"));
    }

    #[test]
    fn test_metadata_processing_result_default() {
        let result = MetadataProcessingResult::default();

        assert_eq!(result.partition_columns.len(), 0);
        assert_eq!(result.total_snapshots, 0);
        assert_eq!(result.total_historical_size, 0);
        assert_eq!(result.oldest_timestamp, 0);
        assert_eq!(result.newest_timestamp, 0);
        assert_eq!(result.total_constraints, 0);
        assert_eq!(result.check_constraints, 0);
        assert_eq!(result.not_null_constraints, 0);
        assert_eq!(result.unique_constraints, 0);
        assert_eq!(result.foreign_key_constraints, 0);
        assert_eq!(result.sort_order_columns.len(), 0);
        assert_eq!(result.schema_changes.len(), 0);
        assert_eq!(result.delete_files_count, 0);
        assert_eq!(result.delete_files_size, 0);
    }

    #[test]
    fn test_detect_breaking_schema_changes_field_removed() {
        let analyzer = create_test_analyzer();

        let old_schema = json!({
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        });

        let new_schema = json!({
            "fields": [
                {"name": "id", "type": "long"}
            ]
        });

        let is_breaking = analyzer.detect_breaking_schema_changes(&old_schema, &new_schema);
        assert!(is_breaking, "Removing a field should be a breaking change");
    }

    #[test]
    fn test_detect_breaking_schema_changes_type_changed() {
        let analyzer = create_test_analyzer();

        let old_schema = json!({
            "fields": [
                {"name": "id", "type": "long"}
            ]
        });

        let new_schema = json!({
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let is_breaking = analyzer.detect_breaking_schema_changes(&old_schema, &new_schema);
        assert!(
            is_breaking,
            "Changing field type should be a breaking change"
        );
    }

    #[test]
    fn test_detect_breaking_schema_changes_nullable_changed() {
        let analyzer = create_test_analyzer();

        let old_schema = json!({
            "fields": [
                {"name": "id", "type": "long", "required": false}
            ]
        });

        let new_schema = json!({
            "fields": [
                {"name": "id", "type": "long", "required": true}
            ]
        });

        let is_breaking = analyzer.detect_breaking_schema_changes(&old_schema, &new_schema);
        assert!(
            is_breaking,
            "Making a field required should be a breaking change"
        );
    }

    #[test]
    fn test_detect_breaking_schema_changes_field_added() {
        let analyzer = create_test_analyzer();

        let old_schema = json!({
            "fields": [
                {"name": "id", "type": "long"}
            ]
        });

        let new_schema = json!({
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        });

        let is_breaking = analyzer.detect_breaking_schema_changes(&old_schema, &new_schema);
        assert!(
            !is_breaking,
            "Adding a field should not be a breaking change"
        );
    }

    #[test]
    fn test_detect_breaking_schema_changes_no_change() {
        let analyzer = create_test_analyzer();

        let old_schema = json!({
            "fields": [
                {"name": "id", "type": "long"}
            ]
        });

        let new_schema = json!({
            "fields": [
                {"name": "id", "type": "long"}
            ]
        });

        let is_breaking = analyzer.detect_breaking_schema_changes(&old_schema, &new_schema);
        assert!(!is_breaking, "No change should not be breaking");
    }

    #[test]
    fn test_is_breaking_change_no_previous() {
        let analyzer = create_test_analyzer();
        let new_schema = json!({"fields": []});

        let is_breaking = analyzer.is_breaking_change(&[], &new_schema);
        assert!(!is_breaking, "First schema should not be breaking");
    }

    #[test]
    fn test_is_breaking_change_with_previous() {
        let analyzer = create_test_analyzer();

        let previous_changes = vec![SchemaChange {
            version: 1,
            timestamp: 1234567890,
            schema: json!({
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"}
                ]
            }),
            is_breaking: false,
        }];

        let new_schema = json!({
            "fields": [
                {"name": "id", "type": "long"}
            ]
        });

        let is_breaking = analyzer.is_breaking_change(&previous_changes, &new_schema);
        assert!(is_breaking, "Removing a field should be breaking");
    }

    #[test]
    fn test_calculate_schema_stability_score_perfect() {
        let analyzer = create_test_analyzer();

        let score = analyzer.calculate_schema_stability_score(0, 0, 0.0, 100.0);

        assert!(score >= 0.9, "Perfect stability should have high score");
        assert!(score <= 1.0);
    }

    #[test]
    fn test_calculate_schema_stability_score_moderate() {
        let analyzer = create_test_analyzer();

        let score = analyzer.calculate_schema_stability_score(15, 2, 0.2, 10.0);

        assert!(score >= 0.0);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_calculate_schema_stability_score_unstable() {
        let analyzer = create_test_analyzer();

        let score = analyzer.calculate_schema_stability_score(100, 20, 2.0, 1.0);

        assert!(score >= 0.0);
        assert!(score < 0.5, "Unstable schema should have low score");
    }

    #[test]
    fn test_calculate_schema_stability_score_clamped() {
        let analyzer = create_test_analyzer();

        // Extreme values that would result in negative score
        let score = analyzer.calculate_schema_stability_score(1000, 100, 10.0, 0.1);

        assert!(score >= 0.0, "Score should be clamped to 0.0");
        assert!(score <= 1.0, "Score should be clamped to 1.0");
    }

    #[test]
    fn test_calculate_storage_cost_impact_zero() {
        let analyzer = create_test_analyzer();

        let impact = analyzer.calculate_storage_cost_impact(0, 0, 0.0);

        assert_eq!(impact, 0.0);
    }

    #[test]
    fn test_calculate_storage_cost_impact_low() {
        let analyzer = create_test_analyzer();

        // 2GB, 50 snapshots, 10 days old
        let impact = analyzer.calculate_storage_cost_impact(2 * 1024 * 1024 * 1024, 50, 10.0);

        assert!(impact >= 0.1, "Should have some impact from size");
        assert!(impact < 0.5);
    }

    #[test]
    fn test_calculate_storage_cost_impact_high() {
        let analyzer = create_test_analyzer();

        // 200GB, 2000 snapshots, 400 days old
        let impact = analyzer.calculate_storage_cost_impact(200 * 1024 * 1024 * 1024, 2000, 400.0);

        assert!(impact > 0.5);
        assert!(impact <= 1.0);
    }

    #[test]
    fn test_calculate_retention_efficiency_perfect() {
        let analyzer = create_test_analyzer();

        // Low snapshot count, reasonable retention
        let efficiency = analyzer.calculate_retention_efficiency(30, 30.0, 0.0);

        assert!(efficiency >= 0.8);
        assert!(efficiency <= 1.0);
    }

    #[test]
    fn test_calculate_retention_efficiency_poor() {
        let analyzer = create_test_analyzer();

        // Too many snapshots, too long retention
        let efficiency = analyzer.calculate_retention_efficiency(2000, 500.0, 0.0);

        assert!(efficiency < 0.5);
    }

    #[test]
    fn test_calculate_recommended_retention_few_snapshots() {
        let analyzer = create_test_analyzer();

        let retention = analyzer.calculate_recommended_retention(50, 20.0);

        assert_eq!(
            retention, 180,
            "Few snapshots should recommend longer retention"
        );
    }

    #[test]
    fn test_calculate_recommended_retention_many_snapshots() {
        let analyzer = create_test_analyzer();

        let retention = analyzer.calculate_recommended_retention(1500, 400.0);

        assert_eq!(
            retention, 30,
            "Many snapshots should recommend shorter retention"
        );
    }
}
