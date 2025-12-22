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

use crate::analyze::common::{
    calculate_recommended_retention, calculate_retention_efficiency,
    calculate_schema_stability_score, is_breaking_change, SchemaChange,
};
use crate::analyze::metrics::{
    ClusteringInfo, DeletionVectorMetrics, FileCompactionMetrics, HealthMetrics,
    SchemaEvolutionMetrics, TableConstraintsMetrics, TimeTravelMetrics,
};
use crate::analyze::table_analyzer::TableAnalyzer;
use crate::storage::{FileMetadata, StorageProvider};
use crate::util::helpers::is_ndjson;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use parquet::schema::types::Type;
use serde_json::Value;
use std::error::Error;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::info;

/// Intermediate structure to hold aggregated results from parallel metadata processing.
///
/// This structure accumulates metrics extracted from Delta transaction log files
/// during parallel processing. It serves as a temporary container before the final
/// metrics are computed and stored in the `HealthMetrics` structure.
///
/// # Fields
///
/// * `clustering_columns` - Columns used for data clustering/z-ordering
/// * `deletion_vector_count` - Number of deletion vectors found
/// * `deletion_vector_total_size` - Total size of all deletion vectors in bytes
/// * `deleted_rows` - Total number of rows marked as deleted
/// * `oldest_dv_age` - Age of the oldest deletion vector in days
/// * `total_snapshots` - Total number of table snapshots/versions
/// * `total_historical_size` - Total size of historical data in bytes
/// * `oldest_timestamp` - Timestamp of the oldest transaction (milliseconds)
/// * `newest_timestamp` - Timestamp of the newest transaction (milliseconds)
/// * `total_constraints` - Total number of table constraints
/// * `check_constraints` - Number of CHECK constraints
/// * `not_null_constraints` - Number of NOT NULL constraints
/// * `unique_constraints` - Number of UNIQUE constraints
/// * `foreign_key_constraints` - Number of FOREIGN KEY constraints
/// * `z_order_columns` - Columns identified for potential Z-order optimization
/// * `z_order_opportunity` - Whether Z-order optimization is recommended
/// * `schema_changes` - List of all schema changes detected
#[derive(Debug, Default)]
pub struct MetadataProcessingResult {
    pub clustering_columns: Vec<String>,
    pub deletion_vector_count: u64,
    pub deletion_vector_total_size: u64,
    pub deleted_rows: u64,
    pub oldest_dv_age: f64,
    pub total_snapshots: usize,
    pub total_historical_size: u64,
    pub oldest_timestamp: u64,
    pub newest_timestamp: u64,
    pub total_constraints: usize,
    pub check_constraints: usize,
    pub not_null_constraints: usize,
    pub unique_constraints: usize,
    pub foreign_key_constraints: usize,
    pub z_order_columns: Vec<String>,
    pub z_order_opportunity: bool,
    pub schema_changes: Vec<SchemaChange>,
}

/// Delta Lake-specific analyzer for processing Delta transaction logs.
///
/// This analyzer implements the `TableAnalyzer` trait and provides functionality
/// to parse Delta Lake transaction logs, extract metrics, and analyze table health.
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
/// use lake_pulse::analyze::delta::DeltaAnalyzer;
///
/// # async fn example(storage: Arc<dyn StorageProvider>) {
/// let analyzer = DeltaAnalyzer::new(storage, 4);
/// // Use analyzer to process Delta tables
/// # }
/// ```
pub struct DeltaAnalyzer {
    storage_provider: Arc<dyn StorageProvider>,
    parallelism: usize,
}

impl DeltaAnalyzer {
    /// Create a new DeltaAnalyzer.
    ///
    /// # Arguments
    ///
    /// * `storage_provider` - The storage provider to use for reading files
    /// * `parallelism` - The number of concurrent tasks to use for metadata processing
    ///
    /// # Returns
    ///
    /// A new `DeltaAnalyzer` instance configured with the specified storage provider and parallelism.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use lake_pulse::storage::StorageProvider;
    /// use lake_pulse::analyze::delta::DeltaAnalyzer;
    ///
    /// # async fn example(storage: Arc<dyn StorageProvider>) {
    /// let analyzer = DeltaAnalyzer::new(storage, 4);
    /// # }
    /// ```
    pub fn new(storage_provider: Arc<dyn StorageProvider>, parallelism: usize) -> Self {
        Self {
            storage_provider,
            parallelism,
        }
    }

    /// Categorize files into data files and Delta metadata files.
    ///
    /// Separates Parquet data files from Delta transaction log JSON files.
    /// Data files are identified by `.parquet` extension, while metadata files
    /// are JSON files within the `_delta_log/` directory.
    ///
    /// # Arguments
    ///
    /// * `objects` - All files discovered in the table location
    ///
    /// # Returns
    ///
    /// A tuple of `(data_files, metadata_files)` where:
    /// * `data_files` - Vector of Parquet data files
    /// * `metadata_files` - Vector of Delta transaction log JSON files
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::delta::DeltaAnalyzer;
    /// # use lake_pulse::storage::FileMetadata;
    /// # use std::sync::Arc;
    /// # fn example(analyzer: &DeltaAnalyzer, files: Vec<FileMetadata>) {
    /// let (data_files, metadata_files) = analyzer.categorize_delta_files(files);
    /// println!("Found {} data files and {} metadata files",
    ///          data_files.len(), metadata_files.len());
    /// # }
    /// ```
    pub fn categorize_delta_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        let mut data_files = Vec::new();
        let mut metadata_files = Vec::new();

        for obj in objects {
            if obj.path.contains("_delta_log/")
                && (obj.path.ends_with(".json") || obj.path.ends_with(".checkpoint.parquet"))
            {
                metadata_files.push(obj);
            } else if obj.path.ends_with(".parquet") {
                data_files.push(obj);
            }
        }

        (data_files, metadata_files)
    }

    /// Find referenced files from Delta transaction logs.
    ///
    /// Parses Delta transaction log files to extract all file paths referenced
    /// in "add" actions. This identifies which data files are currently part of
    /// the table's active state. Processes files in parallel for performance.
    ///
    /// # Arguments
    ///
    /// * `metadata_files` - The metadata files to parse
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<String>)` - Vector of file paths referenced in the transaction logs
    /// * `Err` - If file reading or JSON parsing fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Any metadata file cannot be read from storage
    /// * JSON parsing of transaction log entries fails
    /// * File content is not valid UTF-8
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::delta::DeltaAnalyzer;
    /// # use lake_pulse::storage::FileMetadata;
    /// # async fn example(analyzer: &DeltaAnalyzer, metadata: &Vec<FileMetadata>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let referenced_files = analyzer.find_referenced_files(metadata).await?;
    /// println!("Found {} referenced data files", referenced_files.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_referenced_files(
        &self,
        metadata_files: &[FileMetadata],
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let storage_provider = Arc::clone(&self.storage_provider);
        // TODO: For now, let's check only JSON files.
        //       In the future, we should also check checkpoints.
        let metadata_files_owned = metadata_files
            .iter()
            .filter(|f| f.path.contains(".json"))
            .cloned()
            .collect::<Vec<_>>();

        let results: Vec<Result<Vec<String>, Box<dyn Error + Send + Sync>>> =
            stream::iter(metadata_files_owned)
                .map(|metadata_file| {
                    info!(
                        "Listing info from metadata file={}",
                        &metadata_file.path.clone()
                    );
                    let storage_provider = Arc::clone(&storage_provider);
                    let path = metadata_file.path.clone();

                    async move {
                        let read_file_start = SystemTime::now();
                        let content = storage_provider
                            .read_file(&path)
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
                        info!(
                            "Read file={}, took={}",
                            &path,
                            read_file_start.elapsed()?.as_millis()
                        );

                        info!("Processing content for file={}", &path);
                        let process_content_start = SystemTime::now();
                        let content_str = String::from_utf8_lossy(&content);
                        let is_ndjson = is_ndjson(&content_str);
                        let json: Vec<Value> = if is_ndjson {
                            content_str
                                .lines()
                                .filter_map(|line| serde_json::from_str(line).ok())
                                .collect()
                        } else {
                            vec![serde_json::from_str(&content_str)
                                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?]
                        };
                        info!(
                            "Processed content for file={}, count={} entries, took={}",
                            &path,
                            json.len(),
                            process_content_start.elapsed()?.as_millis()
                        );

                        info!("Extracting file references from metadata file={}", &path);
                        let extract_refs_start = SystemTime::now();
                        let mut file_refs = Vec::new();
                        for entry in json {
                            if let Some(matching_actions) = entry.get("add").or(entry.get("cdc")) {
                                if let Some(obj) = matching_actions.as_object() {
                                    if let Some(path) = obj.get("path") {
                                        if let Some(path_str) = path.as_str() {
                                            file_refs.push(path_str.to_string());
                                        }
                                    }
                                }
                            }
                        }
                        info!(
                            "Extracted file references from metadata file={}, count={}, took={}",
                            &path,
                            file_refs.len(),
                            extract_refs_start.elapsed()?.as_millis()
                        );

                        Ok(file_refs)
                    }
                })
                .buffer_unordered(self.parallelism)
                .collect()
                .await;

        // Flatten results and collect errors
        let mut referenced_files = Vec::new();
        for result in results {
            referenced_files.extend(result?);
        }

        Ok(referenced_files)
    }

    /// Process a single Delta metadata file and extract metrics.
    ///
    /// Reads and parses a Delta transaction log file to extract various metrics
    /// including clustering information, deletion vectors, snapshots, schema changes,
    /// and table constraints. This method is designed to be called in parallel for
    /// multiple metadata files.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the metadata file
    /// * `file_index` - The index of the metadata file (used as version number)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(MetadataProcessingResult)` - Aggregated metrics from this metadata file
    /// * `Err` - If file reading or parsing fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The metadata file cannot be read from storage
    /// * The file content is not valid UTF-8
    /// * JSON parsing fails for the transaction log entries
    pub async fn process_single_metadata_file(
        &self,
        file_path: &str,
        file_index: usize,
    ) -> Result<MetadataProcessingResult, Box<dyn Error + Send + Sync>> {
        // If the given file is a checkpoint (ie: `*.checkpoint.parquet`),
        // extract the schema from the checkpoint's `metaData.schemaString` column.
        if file_path.ends_with(".checkpoint.parquet") {
            return self
                .process_checkpoint_metadata(file_path, file_index)
                .await;
        }

        let content = self.storage_provider.read_file(file_path).await?;
        let content_str = String::from_utf8_lossy(&content);

        let mut result = MetadataProcessingResult {
            oldest_timestamp: Utc::now().timestamp() as u64,
            ..Default::default()
        };

        let is_ndjson = is_ndjson(&content_str);
        let json: Vec<Value> = if is_ndjson {
            content_str
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect()
        } else {
            vec![serde_json::from_str(&content_str)?]
        };

        let mut current_version = file_index as u64;

        for entry in json {
            // Extract clustering columns from clusterBy
            if let Some(cluster_by) = entry.get("clusterBy") {
                // TODO: Double check if `.as_array()` is correct. Maybe it should be `.as_object()`?
                if let Some(cluster_array) = cluster_by.as_array() {
                    let clustering_columns: Vec<String> = cluster_array
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    if !clustering_columns.is_empty() {
                        result.z_order_opportunity = true;
                        result.z_order_columns = clustering_columns.clone();
                        if result.clustering_columns.is_empty() {
                            result.clustering_columns = clustering_columns;
                        }
                    }
                }
            }

            // Extract clustering from metaData
            if let Some(metadata) = entry.get("metaData") {
                if let Some(cluster_by) = metadata.get("clusterBy") {
                    // TODO: Double check if `.as_array()` is correct. Maybe it should be `.as_object()`?
                    if let Some(cluster_array) = cluster_by.as_array() {
                        if result.clustering_columns.is_empty() {
                            result.clustering_columns = cluster_array
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                        }
                    }
                }
            }

            // Extract clustering from configuration
            if let Some(configuration) = entry.get("configuration") {
                if let Some(cluster_by) = configuration.get("delta.clustering.columns") {
                    if let Some(cluster_str) = cluster_by.as_str() {
                        if result.clustering_columns.is_empty() {
                            result.clustering_columns = cluster_str
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect();
                        }
                    }
                }
            }

            // Extract Z-Order information from OPTIMIZE operations
            if let Some(commit_info) = entry.get("commitInfo") {
                if let Some(operation) = commit_info.get("operation") {
                    if operation.as_str().map(|s| s.trim().to_lowercase())
                        == Some("OPTIMIZE".to_string())
                    {
                        if let Some(operation_params) = commit_info.get("operationParameters") {
                            if let Some(z_order_by) = operation_params.get("zOrderBy") {
                                // zOrderBy is a JSON array string like "[\"department\"]"
                                if let Some(z_order_str) = z_order_by.as_str() {
                                    // Parse the JSON array string
                                    if let Ok(z_order_array) =
                                        serde_json::from_str::<Vec<String>>(z_order_str)
                                    {
                                        if !z_order_array.is_empty() {
                                            result.z_order_opportunity = true;
                                            result.z_order_columns = z_order_array.clone();
                                            if result.clustering_columns.is_empty() {
                                                result.clustering_columns = z_order_array;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Extract deletion vector metrics
            self.extract_deletion_vectors(&entry, &mut result);

            // Extract snapshot/time travel metrics
            self.extract_snapshot_metrics(&entry, &mut result);

            // Extract schema and constraints
            self.extract_schema_and_constraints(&entry, &mut result, current_version);

            // Extract protocol version changes
            if let Some(protocol) = entry.get("protocol") {
                if let Some(reader_version) = protocol.get("minReaderVersion") {
                    let new_version = reader_version.as_u64().unwrap_or(0);
                    if new_version > current_version {
                        result.schema_changes.push(SchemaChange {
                            version: current_version,
                            timestamp: entry.get("timestamp").and_then(|t| t.as_u64()).unwrap_or(0),
                            // TODO: Is this correct?
                            schema: Value::Null,
                            is_breaking: true,
                        });
                        current_version = new_version;
                    }
                }
            }
        }

        Ok(result)
    }

    /// Process checkpoint metadata to extract schema changes and constraints.
    ///
    /// This function reads a Delta Lake checkpoint Parquet file and extracts metadata
    /// information, specifically focusing on schema changes and constraint definitions.
    /// It uses schema projection to efficiently read only the `metaData` column,
    /// avoiding the overhead of decoding all checkpoint actions for large checkpoints.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the checkpoint Parquet file to process
    /// * `file_index` - Index of the file, used as the version number for schema changes
    ///
    /// # Returns
    ///
    /// Returns a `MetadataProcessingResult` containing:
    /// * Schema changes detected in the checkpoint with version and timestamp
    /// * Constraint counts (total, check, not null, unique, foreign key)
    /// * Oldest timestamp initialized to current time (updated by caller if needed)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The file cannot be read from storage
    /// * The Parquet file is malformed or cannot be parsed
    /// * The `metaData` column is not found in the checkpoint schema
    /// * Row iteration or field extraction fails
    ///
    /// # Implementation Details
    ///
    /// The function performs the following steps:
    /// 1. Reads the checkpoint file from storage
    /// 2. Creates a schema projection containing only the `metaData` column
    /// 3. Iterates through rows looking for non-null `metaData` fields
    /// 4. Extracts `schemaString` and `createdTime` from metadata
    /// 5. Parses schema JSON and extracts constraints
    /// 6. Detects breaking schema changes by comparing with previous schemas
    /// 7. Accumulates all schema changes and constraint counts in the result
    async fn process_checkpoint_metadata(
        &self,
        file_path: &str,
        file_index: usize,
    ) -> Result<MetadataProcessingResult, Box<dyn Error + Send + Sync>> {
        let content = self.storage_provider.read_file(file_path).await?;
        let reader = SerializedFileReader::new(Bytes::copy_from_slice(content.as_slice()))
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let mut result = MetadataProcessingResult {
            oldest_timestamp: Utc::now().timestamp() as u64,
            ..Default::default()
        };

        // Project only the `metaData` column to avoid decoding all other
        // checkpoint actions (add/remove, etc.) for large checkpoints.
        let file_schema = reader.metadata().file_metadata().schema();
        let schema_name = file_schema.name();
        let fields = file_schema.get_fields();

        let selected_fields: Vec<_> = fields
            .iter()
            .filter(|&f| f.name() == "metaData")
            .cloned()
            .collect();

        if selected_fields.is_empty() {
            let err: Box<dyn Error + Send + Sync> =
                "metaData column not found in checkpoint schema".into();
            return Err(err);
        }

        let schema_projection = Type::group_type_builder(schema_name)
            .with_fields(selected_fields)
            .build()
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let row_iter = reader.get_row_iter(Some(schema_projection)).map_err(|e| {
            info!("error={:?}", e);
            Box::new(e) as Box<dyn Error + Send + Sync>
        })?;

        let version = file_index as u64;

        for row in row_iter {
            let row = row.map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

            // The checkpoint encodes actions as rows with a top-level column indicating the
            // action type. We are interested in rows where the `metaData` column is present
            // and non-null.
            if let Some((_, Field::Group(meta_group))) = row.get_column_iter().next() {
                let mut schema: Option<Value> = None;
                let mut timestamp: u64 = 0;

                for (meta_name, meta_field) in meta_group.get_column_iter() {
                    match (meta_name.as_str(), meta_field) {
                        ("schemaString", Field::Str(s)) => {
                            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                                schema = Some(parsed);
                            }
                        }
                        ("createdTime", Field::Long(v)) => {
                            if *v > 0 {
                                timestamp = *v as u64;
                            }
                        }
                        ("createdTime", Field::Int(v)) => {
                            if *v > 0 {
                                timestamp = *v as u64;
                            }
                        }
                        _ => {}
                    }
                }

                if let Some(schema) = schema {
                    let constraints = self.extract_constraints_from_schema(&schema);
                    result.total_constraints += constraints.0;
                    result.check_constraints += constraints.1;
                    result.not_null_constraints += constraints.2;
                    result.unique_constraints += constraints.3;
                    result.foreign_key_constraints += constraints.4;

                    let breaking = is_breaking_change(&result.schema_changes, &schema);

                    result
                        .schema_changes
                        .push(SchemaChange::new(version, timestamp, schema, breaking));
                }
            }
        }
        Ok(result)
    }

    /// Extract deletion vector metrics from a Delta log entry.
    ///
    /// Analyzes "add" and "remove" actions in the transaction log to identify deletion vectors,
    /// which are used in Delta Lake to mark rows as deleted without rewriting files.
    /// Extracts count, size, deleted row count, and age information.
    ///
    /// # Arguments
    ///
    /// * `entry` - The Delta log entry to process
    /// * `result` - The result object to update (mutated in place)
    fn extract_deletion_vectors(&self, entry: &Value, result: &mut MetadataProcessingResult) {
        // Deletion vectors can be stored in both 'add' and 'remove' actions
        // Check 'add' actions first
        if let Some(add_action) = entry.get("add") {
            if let Some(add_object) = add_action.as_object() {
                if let Some(deletion_vector) = add_object.get("deletionVector") {
                    result.deletion_vector_count += 1;

                    if let Some(size) = deletion_vector.get("sizeInBytes") {
                        result.deletion_vector_total_size += size.as_u64().unwrap_or(0);
                    }

                    if let Some(rows) = deletion_vector.get("cardinality") {
                        result.deleted_rows += rows.as_u64().unwrap_or(0);
                    }

                    // Calculate age based on modification time of the add action
                    if let Some(mod_time) = add_object.get("modificationTime") {
                        let creation_time = mod_time.as_u64().unwrap_or(0) as i64;
                        let age_days = (chrono::Utc::now().timestamp() - creation_time / 1000)
                            as f64
                            / 86400.0;
                        result.oldest_dv_age = result.oldest_dv_age.max(age_days);
                    }
                }
            }
        }

        // Also check 'remove' actions for deletion vectors
        if let Some(remove_action) = entry.get("remove") {
            if let Some(remove_object) = remove_action.as_object() {
                if let Some(deletion_vector) = remove_object.get("deletionVector") {
                    result.deletion_vector_count += 1;

                    if let Some(size) = deletion_vector.get("sizeInBytes") {
                        result.deletion_vector_total_size += size.as_u64().unwrap_or(0);
                    }

                    if let Some(rows) = deletion_vector.get("cardinality") {
                        result.deleted_rows += rows.as_u64().unwrap_or(0);
                    }

                    // Calculate age based on deletion timestamp or modification time
                    let timestamp = remove_object
                        .get("deletionTimestamp")
                        .or_else(|| remove_object.get("timestamp"));

                    if let Some(ts) = timestamp {
                        let creation_time = ts.as_u64().unwrap_or(0) as i64;
                        let age_days = (chrono::Utc::now().timestamp() - creation_time / 1000)
                            as f64
                            / 86400.0;
                        result.oldest_dv_age = result.oldest_dv_age.max(age_days);
                    }
                }
            }
        }
    }

    /// Extract snapshot/time travel metrics from a Delta log entry.
    ///
    /// Collects timestamp information and estimates snapshot sizes to support
    /// time travel analysis. Tracks the oldest and newest timestamps and
    /// accumulates total historical data size.
    ///
    /// # Arguments
    ///
    /// * `entry` - The Delta log entry to process
    /// * `result` - The result object to update (mutated in place)
    fn extract_snapshot_metrics(&self, entry: &Value, result: &mut MetadataProcessingResult) {
        if let Some(timestamp) = entry.get("timestamp").or(entry
            .get("commitInfo")
            .map(|c| c.get("timestamp").unwrap_or(&Value::Null)))
        {
            let ts = timestamp.as_u64().unwrap_or(0);
            if ts > 0 {
                result.total_snapshots += 1;
                result.oldest_timestamp = result.oldest_timestamp.min(ts);
                result.newest_timestamp = result.newest_timestamp.max(ts);

                let snapshot_size = self.estimate_snapshot_size(entry);
                result.total_historical_size += snapshot_size;
            }
        }
    }

    /// Extract schema and constraints from a Delta log entry.
    ///
    /// Parses metadata entries to extract schema definitions and table constraints.
    /// Identifies various constraint types (CHECK, NOT NULL, UNIQUE, FOREIGN KEY)
    /// and detects breaking vs. non-breaking schema changes.
    ///
    /// # Arguments
    ///
    /// * `entry` - The Delta log entry to process
    /// * `result` - The result object to update (mutated in place)
    /// * `current_version` - The current version of the schema
    fn extract_schema_and_constraints(
        &self,
        entry: &Value,
        result: &mut MetadataProcessingResult,
        current_version: u64,
    ) {
        if let Some(metadata) = entry.get("metaData") {
            if let Some(schema_string) = metadata.get("schemaString") {
                if let Ok(schema) =
                    serde_json::from_str::<Value>(schema_string.as_str().unwrap_or(""))
                {
                    let constraints = self.extract_constraints_from_schema(&schema);
                    result.total_constraints += constraints.0;
                    result.check_constraints += constraints.1;
                    result.not_null_constraints += constraints.2;
                    result.unique_constraints += constraints.3;
                    result.foreign_key_constraints += constraints.4;

                    let breaking = is_breaking_change(&result.schema_changes, &schema);
                    result.schema_changes.push(SchemaChange::new(
                        current_version,
                        entry.get("timestamp").and_then(|t| t.as_u64()).unwrap_or(0),
                        schema,
                        breaking,
                    ));
                }
            }
        }
    }

    /// Update health metrics from Delta metadata files.
    ///
    /// Main entry point for extracting comprehensive health metrics from Delta
    /// transaction logs. Processes all metadata files in parallel, aggregates
    /// results, and populates the HealthMetrics structure with deletion vector,
    /// schema evolution, time travel, constraint, clustering, and compaction metrics.
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
    /// # use lake_pulse::analyze::delta::DeltaAnalyzer;
    /// # use lake_pulse::analyze::metrics::HealthMetrics;
    /// # use lake_pulse::storage::FileMetadata;
    /// # async fn example(analyzer: &DeltaAnalyzer, metadata: &Vec<FileMetadata>, mut metrics: HealthMetrics) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// analyzer.update_metrics_from_delta_metadata(
    ///     metadata,
    ///     1024 * 1024 * 1024, // 1GB total data size
    ///     100,                 // 100 data files
    ///     &mut metrics
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_metrics_from_delta_metadata(
        &self,
        metadata_files: &[FileMetadata],
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let parallelism = self.parallelism.max(1);

        // TODO: `metadata_files` contains checkpoints too.
        //       Should get the last checkpoint and extract the schema from there
        let min_file_name = metadata_files
            .iter()
            .min_by_key(|f| f.path.clone())
            .map(|f| f.path.clone());
        let max_file_name = metadata_files
            .iter()
            .max_by_key(|f| f.path.clone())
            .map(|f| f.path.clone());
        metrics.metadata_health.first_file_name = min_file_name.clone();
        metrics.metadata_health.last_file_name = max_file_name.clone();

        info!(
            "Updating the metrics from Delta metadata files, parallelism={}, metadata_files_count={}, \
            data_files_count={}, min_file={:?}, max_file={:?}",
            parallelism,
            metadata_files.len(),
            data_files_total_files,
            min_file_name,
            max_file_name
        );

        let update_metrics_start = SystemTime::now();
        let metadata_files_owned: Vec<_> = metadata_files.iter().cloned().enumerate().collect();
        let results: Vec<Result<MetadataProcessingResult, Box<dyn Error + Send + Sync>>> =
            stream::iter(metadata_files_owned)
                .map(|(index, metadata_file)| {
                    info!(
                        "Starting processing Delta metadata file={}, index={}",
                        &metadata_file.path.clone(),
                        index
                    );
                    let path = metadata_file.path.clone();
                    let analyzer = DeltaAnalyzer {
                        storage_provider: Arc::clone(&self.storage_provider),
                        parallelism,
                    };

                    let result = async move {
                        let process_single_start = SystemTime::now();
                        let r = analyzer.process_single_metadata_file(&path, index).await;
                        info!(
                            "Processed Delta metadata file={} in async, took={}",
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
            "Finished the metrics from Delta metadata files, parallelism={}, metadata_files_count={}, \
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
        let mut clustering_columns: Vec<String> = vec![];
        let mut deletion_vector_count = 0u64;
        let mut total_size = 0u64;
        let mut deleted_rows = 0u64;
        let mut oldest_dv_age: f64 = 0.0;
        let mut total_snapshots = 0;
        let mut total_historical_size = 0u64;
        let mut oldest_timestamp = chrono::Utc::now().timestamp() as u64;
        let mut newest_timestamp = 0u64;
        let mut total_constraints = 0;
        let mut check_constraints = 0;
        let mut not_null_constraints = 0;
        let mut unique_constraints = 0;
        let mut foreign_key_constraints = 0;
        let mut z_order_columns: Vec<String> = vec![];
        let mut z_order_opportunity = false;
        let mut schema_changes = Vec::new();
        let mut current_version = 0u64;

        for result in results {
            let r = result?;

            if clustering_columns.is_empty() && !r.clustering_columns.is_empty() {
                clustering_columns = r.clustering_columns;
            }

            deletion_vector_count += r.deletion_vector_count;
            total_size += r.deletion_vector_total_size;
            deleted_rows += r.deleted_rows;
            oldest_dv_age = oldest_dv_age.max(r.oldest_dv_age);

            total_snapshots += r.total_snapshots;
            total_historical_size += r.total_historical_size;
            oldest_timestamp = oldest_timestamp.min(r.oldest_timestamp);
            newest_timestamp = newest_timestamp.max(r.newest_timestamp);

            total_constraints += r.total_constraints;
            check_constraints += r.check_constraints;
            not_null_constraints += r.not_null_constraints;
            unique_constraints += r.unique_constraints;
            foreign_key_constraints += r.foreign_key_constraints;

            if r.z_order_opportunity {
                z_order_opportunity = true;
                if z_order_columns.is_empty() {
                    z_order_columns = r.z_order_columns;
                }
            }

            schema_changes.extend(r.schema_changes);
            current_version = current_version.max(schema_changes.len() as u64);
        }

        // Set deletion vector metrics
        if deletion_vector_count != 0 {
            let avg_size = total_size as f64 / deletion_vector_count as f64;
            let impact_score = self.calculate_deletion_vector_impact(
                deletion_vector_count as usize,
                total_size,
                oldest_dv_age,
            );

            metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
                deletion_vector_count: deletion_vector_count as usize,
                total_deletion_vector_size_bytes: total_size,
                avg_deletion_vector_size_bytes: avg_size,
                deletion_vector_age_days: oldest_dv_age,
                deleted_rows_count: deleted_rows,
                deletion_vector_impact_score: impact_score,
            });
        }

        // Set schema evolution metrics
        if !schema_changes.is_empty() {
            metrics.schema_evolution =
                self.calculate_schema_metrics(schema_changes, current_version)?;
        }

        // Set time travel metrics
        if total_snapshots != 0 {
            let now = chrono::Utc::now().timestamp() as u64;
            let oldest_age_days = (now - oldest_timestamp / 1000) as f64 / 86400.0;
            let newest_age_days = (now - newest_timestamp / 1000) as f64 / 86400.0;
            let avg_snapshot_size = total_historical_size as f64 / total_snapshots as f64;

            let storage_cost_impact = crate::analyze::common::calculate_storage_cost_impact(
                total_historical_size,
                total_snapshots,
                oldest_age_days,
            );
            let retention_efficiency =
                calculate_retention_efficiency(total_snapshots, oldest_age_days, newest_age_days);
            let recommended_retention =
                calculate_recommended_retention(total_snapshots, oldest_age_days);

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

        // For Delta Lake clustering, we analyze the distribution of files
        // Since clustering is more about data layout than explicit clusters,
        // we use partition-like analysis but call it clustering
        let partition_count = metrics.partitions.len();

        // Calculate clustering metrics
        let cluster_count = partition_count.max(1); // Use partition count as proxy for cluster count
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
            clustering_columns: clustering_columns.to_vec(),
            cluster_count,
            avg_files_per_cluster,
            avg_cluster_size_bytes,
        });

        // TODO: Why is this all zeros?
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.0,
            small_files_count: 0,
            small_files_size_bytes: 0,
            potential_compaction_files: 0,
            estimated_compaction_savings_bytes: 0,
            recommended_target_file_size_bytes: 0,
            compaction_priority: "".to_string(),
            z_order_opportunity,
            z_order_columns,
        });

        Ok(())
    }

    /// Calculate deletion vector impact score.
    ///
    /// Computes a score (0.0 to 1.0) indicating the impact of deletion vectors
    /// on table performance. Higher scores indicate more significant impact and
    /// suggest that compaction may be beneficial.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of deletion vectors
    /// * `size` - Total size of deletion vectors in bytes
    /// * `age` - Age of the oldest deletion vector in days
    ///
    /// # Returns
    ///
    /// A score between 0.0 and 1.0, where:
    /// * 0.0 = minimal impact
    /// * 1.0 = high impact, compaction strongly recommended
    fn calculate_deletion_vector_impact(&self, count: usize, size: u64, age: f64) -> f64 {
        let mut impact: f64 = 0.0;

        // Impact from count (more DVs = higher impact)
        if count > 100 {
            impact += 0.3;
        } else if count > 50 {
            impact += 0.2;
        } else if count > 10 {
            impact += 0.1;
        }

        // Impact from size (larger DVs = higher impact)
        let size_mb = size as f64 / (1024.0 * 1024.0);
        if size_mb > 100.0 {
            impact += 0.3;
        } else if size_mb > 50.0 {
            impact += 0.2;
        } else if size_mb > 10.0 {
            impact += 0.1;
        }

        // Impact from age (older DVs = higher impact)
        if age > 30.0 {
            impact += 0.4;
        } else if age > 7.0 {
            impact += 0.2;
        }

        impact.min(1.0_f64)
    }

    /// Calculate schema metrics from schema changes.
    ///
    /// Analyzes the history of schema changes to compute evolution metrics including
    /// total changes, breaking vs. non-breaking changes, change frequency, and
    /// stability score.
    ///
    /// # Arguments
    ///
    /// * `changes` - The schema changes to analyze
    /// * `current_version` - The current version of the schema
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Some(SchemaEvolutionMetrics))` - Computed schema evolution metrics
    /// * `Ok(None)` - If no schema changes exist (though current implementation always returns Some)
    /// * `Err` - If metric calculation fails
    ///
    /// # Errors
    ///
    /// This function may return an error if timestamp calculations overflow or fail.
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
            (now - last_change.timestamp / 1000) as f64 / 86400.0
        } else {
            365.0 // No changes in a year = very stable
        };

        // Calculate change frequency (changes per day)
        let total_days = match (changes.first(), changes.last()) {
            (Some(first), Some(last)) if changes.len() > 1 => {
                let first_change = first.timestamp / 1000;
                let last_change = last.timestamp / 1000;
                ((last_change - first_change) as f64 / 86400.0).max(1.0_f64)
            }
            _ => 1.0,
        };

        let change_frequency = total_changes as f64 / total_days;

        // Calculate stability score using shared function
        let stability_score = calculate_schema_stability_score(
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
    /// Estimate snapshot size from a Delta log entry.
    ///
    /// Estimates the size of a snapshot by summing file sizes from "add" actions
    /// and adding a fixed metadata overhead.
    ///
    /// # Arguments
    ///
    /// * `json` - The Delta log entry to process
    ///
    /// # Returns
    ///
    /// Estimated snapshot size in bytes (file sizes + 1KB overhead).
    fn estimate_snapshot_size(&self, json: &Value) -> u64 {
        let mut size = 0u64;

        // Estimate size based on actions in the transaction log
        if let Some(add_actions) = json.get("add") {
            // TODO: Double check if `.as_array()` is correct. Maybe it should be `.as_object()`?
            if let Some(add_array) = add_actions.as_array() {
                for add_action in add_array {
                    if let Some(file_size) = add_action.get("sizeInBytes") {
                        size += file_size.as_u64().unwrap_or(0);
                    }
                }
            }
        }

        // Add metadata overhead (estimated)
        size + 1024 // 1KB overhead per snapshot
    }

    /// Extract constraints from Delta schema.
    ///
    /// Parses schema fields to identify and count various types of constraints
    /// including NOT NULL, CHECK, UNIQUE, and FOREIGN KEY constraints.
    ///
    /// # Arguments
    ///
    /// * `schema` - The Delta schema
    ///
    /// # Returns
    ///
    /// A tuple of `(total, check, not_null, unique, foreign_key)` where:
    /// * `total` - Total number of fields in the schema
    /// * `check` - Number of CHECK constraints
    /// * `not_null` - Number of NOT NULL constraints
    /// * `unique` - Number of UNIQUE constraints
    /// * `foreign_key` - Number of FOREIGN KEY constraints
    fn extract_constraints_from_schema(
        &self,
        schema: &Value,
    ) -> (usize, usize, usize, usize, usize) {
        let mut total = 0;
        let mut check = 0;
        let mut not_null = 0;
        let mut unique = 0;
        let mut foreign_key = 0;

        if let Some(fields) = schema.get("fields") {
            // TODO: Double check if `.as_array()` is correct. Maybe it should be `.as_object()`?
            if let Some(fields_array) = fields.as_array() {
                for field in fields_array {
                    total += 1;

                    // Check for NOT NULL constraint
                    if let Some(nullable) = field.get("nullable") {
                        if !nullable.as_bool().unwrap_or(true) {
                            not_null += 1;
                        }
                    }

                    // Check for other constraints (simplified)
                    if let Some(metadata) = field.get("metadata") {
                        if let Some(metadata_obj) = metadata.as_object() {
                            for (key, _) in metadata_obj {
                                if key.contains("constraint") || key.contains("check") {
                                    check += 1;
                                }
                                if key.contains("unique") {
                                    unique += 1;
                                }
                                if key.contains("foreign") || key.contains("reference") {
                                    foreign_key += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        (total, check, not_null, unique, foreign_key)
    }

    /// Calculate constraint violation risk.
    ///
    /// Assesses the risk of constraint violations based on the number and types
    /// of constraints defined. Fewer constraints or missing CHECK constraints
    /// indicate higher risk.
    ///
    /// # Arguments
    ///
    /// * `total_constraints` - Total number of constraints
    /// * `check_constraints` - Number of check constraints
    ///
    /// # Returns
    ///
    /// A risk score between 0.0 and 1.0, where:
    /// * 0.0 = low risk (many constraints defined)
    /// * 0.5 = medium risk (no constraints defined)
    /// * 1.0 = high risk (very few constraints)
    fn calculate_constraint_violation_risk(
        &self,
        total_constraints: usize,
        check_constraints: usize,
    ) -> f64 {
        if total_constraints == 0 {
            return 0.5; // Medium risk if no constraints
        }

        let mut risk: f64 = 0.0;

        // More constraints = lower risk
        if total_constraints < 5 {
            risk += 0.3;
        } else if total_constraints < 10 {
            risk += 0.2;
        } else if total_constraints < 20 {
            risk += 0.1;
        }

        // Check constraints are important for data quality
        if check_constraints == 0 {
            risk += 0.3;
        } else if check_constraints < 3 {
            risk += 0.2;
        }

        risk.min(1.0_f64)
    }

    /// Calculate data quality score.
    ///
    /// Computes an overall data quality score based on the number of constraints
    /// and the violation risk. More constraints and lower violation risk result
    /// in higher quality scores.
    ///
    /// # Arguments
    ///
    /// * `total_constraints` - Total number of constraints
    /// * `violation_risk` - Violation risk score
    ///
    /// # Returns
    ///
    /// A quality score between 0.0 and 1.0, where:
    /// * 1.0 = excellent data quality (many constraints, low violation risk)
    /// * 0.0 = poor data quality (few constraints, high violation risk)
    fn calculate_data_quality_score(&self, total_constraints: usize, violation_risk: f64) -> f64 {
        let mut score: f64 = 1.0;

        // Penalize based on violation risk
        score -= violation_risk * 0.5;

        // Reward more constraints
        if total_constraints > 20 {
            score += 0.2;
        } else if total_constraints > 10 {
            score += 0.1;
        }

        score.clamp(0.0_f64, 1.0_f64)
    }

    /// Calculate constraint coverage score.
    ///
    /// Measures what proportion of total constraints are CHECK constraints,
    /// which are important for enforcing business rules and data quality.
    ///
    /// # Arguments
    ///
    /// * `total_constraints` - Total number of constraints
    /// * `check_constraints` - Number of check constraints
    ///
    /// # Returns
    ///
    /// A coverage score between 0.0 and 1.0, representing the ratio of
    /// CHECK constraints to total constraints. Returns 0.0 if no constraints exist.
    fn calculate_constraint_coverage_score(
        &self,
        total_constraints: usize,
        check_constraints: usize,
    ) -> f64 {
        if total_constraints == 0 {
            return 0.0;
        }

        let coverage = check_constraints as f64 / total_constraints as f64;
        coverage.clamp(0.0_f64, 1.0_f64)
    }
}

// Implement the TableAnalyzer trait for DeltaAnalyzer
#[async_trait]
impl TableAnalyzer for DeltaAnalyzer {
    fn categorize_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        self.categorize_delta_files(objects)
    }

    async fn find_referenced_files(
        &self,
        metadata_files: &[FileMetadata],
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        self.find_referenced_files(metadata_files).await
    }

    async fn update_metrics_from_metadata(
        &self,
        metadata_files: &[FileMetadata],
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.update_metrics_from_delta_metadata(
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
    use crate::analyze::common::detect_breaking_schema_changes;
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
            self.options.get_or_init(HashMap::new)
        }

        fn clean_options(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn uri_from_path(&self, path: &str) -> String {
            path.to_string()
        }
    }

    // Helper function to create a test DeltaAnalyzer
    fn create_test_analyzer() -> DeltaAnalyzer {
        let mock_storage = Arc::new(MockStorageProvider::new());
        DeltaAnalyzer::new(mock_storage, 4)
    }

    #[test]
    fn test_delta_analyzer_new() {
        let mock_storage = Arc::new(MockStorageProvider::new());
        let analyzer = DeltaAnalyzer::new(mock_storage, 8);

        assert_eq!(analyzer.parallelism, 8);
    }

    #[test]
    fn test_categorize_delta_files_data_only() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "table/part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "table/part-00001.parquet".to_string(),
                size: 2048,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_delta_files(files);

        assert_eq!(data_files.len(), 2);
        assert_eq!(metadata_files.len(), 0);
    }

    #[test]
    fn test_categorize_delta_files_metadata_only() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "table/_delta_log/00000000000000000000.json".to_string(),
                size: 512,
                last_modified: None,
            },
            FileMetadata {
                path: "table/_delta_log/00000000000000000001.json".to_string(),
                size: 768,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_delta_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 2);
    }

    #[test]
    fn test_categorize_delta_files_mixed() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "table/part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "table/_delta_log/00000000000000000000.json".to_string(),
                size: 512,
                last_modified: None,
            },
            FileMetadata {
                path: "table/part-00001.parquet".to_string(),
                size: 2048,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_delta_files(files);

        assert_eq!(data_files.len(), 2);
        assert_eq!(metadata_files.len(), 1);
    }

    #[test]
    fn test_categorize_delta_files_checkpoint() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "table/_delta_log/00000000000000000010.checkpoint.parquet".to_string(),
                size: 4096,
                last_modified: None,
            },
            FileMetadata {
                path: "table/part-00000.parquet".to_string(),
                size: 1024,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_delta_files(files);

        assert_eq!(data_files.len(), 1);
        assert_eq!(metadata_files.len(), 1);
        assert!(metadata_files[0].path.contains("checkpoint.parquet"));
    }

    #[test]
    fn test_categorize_delta_files_empty() {
        let analyzer = create_test_analyzer();
        let files = vec![];

        let (data_files, metadata_files) = analyzer.categorize_delta_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 0);
    }

    #[test]
    fn test_categorize_delta_files_non_parquet() {
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

        let (data_files, metadata_files) = analyzer.categorize_delta_files(files);

        // Non-parquet files should be ignored
        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 0);
    }

    #[test]
    fn test_calculate_deletion_vector_impact_zero() {
        let analyzer = create_test_analyzer();
        let impact = analyzer.calculate_deletion_vector_impact(0, 0, 0.0);

        assert_eq!(impact, 0.0);
    }

    #[test]
    fn test_calculate_deletion_vector_impact_low() {
        let analyzer = create_test_analyzer();
        // Small count, small size, recent
        let impact = analyzer.calculate_deletion_vector_impact(5, 1024, 1.0);

        assert!((0.0..=1.0).contains(&impact));
        assert!(impact < 0.5); // Should be low impact
    }

    #[test]
    fn test_calculate_deletion_vector_impact_high() {
        let analyzer = create_test_analyzer();
        // Large count, large size, old
        let impact = analyzer.calculate_deletion_vector_impact(1000, 100_000_000, 90.0);

        assert!((0.0..=1.0).contains(&impact));
        assert!(impact > 0.5); // Should be high impact
    }

    #[test]
    fn test_calculate_deletion_vector_impact_clamped() {
        let analyzer = create_test_analyzer();
        // Extreme values should still be clamped to [0, 1]
        let impact = analyzer.calculate_deletion_vector_impact(10000, 1_000_000_000, 365.0);

        assert!((0.0..=1.0).contains(&impact));
    }

    #[test]
    fn test_calculate_schema_stability_score_perfect() {
        // No changes, no breaking changes, no frequency, long time since last
        let score = calculate_schema_stability_score(0, 0, 0.0, 365.0);

        assert!(score >= 0.9); // Should be very stable
        assert!(score <= 1.0);
    }

    #[test]
    fn test_calculate_schema_stability_score_unstable() {
        // Many changes, many breaking, high frequency, recent
        let score = calculate_schema_stability_score(100, 50, 2.0, 1.0);

        assert!(score >= 0.0);
        assert!(score < 0.5); // Should be unstable
    }

    #[test]
    fn test_calculate_schema_stability_score_moderate() {
        // Moderate changes, few breaking, low frequency, moderate time
        let score = calculate_schema_stability_score(15, 2, 0.05, 10.0);

        assert!((0.0..=1.0).contains(&score));
        assert!(score > 0.3 && score < 0.9); // Should be moderate
    }

    #[test]
    fn test_calculate_schema_stability_score_clamped() {
        // Extreme values should be clamped
        let score = calculate_schema_stability_score(1000, 500, 10.0, 0.1);

        assert!((0.0..=1.0).contains(&score));
    }

    #[test]
    fn test_estimate_snapshot_size_empty() {
        let analyzer = create_test_analyzer();
        let json = json!({});

        let size = analyzer.estimate_snapshot_size(&json);

        // Should return at least the overhead (1024 bytes)
        assert_eq!(size, 1024);
    }

    #[test]
    fn test_estimate_snapshot_size_with_add() {
        let analyzer = create_test_analyzer();
        // The implementation expects "add" to be an array of objects with "sizeInBytes"
        let json = json!({
            "add": [
                {
                    "sizeInBytes": 5000
                }
            ]
        });

        let size = analyzer.estimate_snapshot_size(&json);

        // Should be file size + overhead
        assert_eq!(size, 5000 + 1024);
    }

    #[test]
    fn test_estimate_snapshot_size_no_size_field() {
        let analyzer = create_test_analyzer();
        let json = json!({
            "add": {
                "path": "file.parquet"
            }
        });

        let size = analyzer.estimate_snapshot_size(&json);

        // Should return just the overhead
        assert_eq!(size, 1024);
    }

    #[test]
    fn test_extract_constraints_from_schema_empty() {
        let analyzer = create_test_analyzer();
        let schema = json!({
            "type": "struct",
            "fields": []
        });

        let (total, check, not_null, unique, foreign_key) =
            analyzer.extract_constraints_from_schema(&schema);

        assert_eq!(total, 0);
        assert_eq!(check, 0);
        assert_eq!(not_null, 0);
        assert_eq!(unique, 0);
        assert_eq!(foreign_key, 0);
    }

    #[test]
    fn test_extract_constraints_from_schema_not_null() {
        let analyzer = create_test_analyzer();
        let schema = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": false
                },
                {
                    "name": "name",
                    "type": "string",
                    "nullable": true
                }
            ]
        });

        let (total, check, not_null, unique, foreign_key) =
            analyzer.extract_constraints_from_schema(&schema);

        assert_eq!(total, 2);
        assert_eq!(not_null, 1); // Only id is not null
        assert_eq!(check, 0);
        assert_eq!(unique, 0);
        assert_eq!(foreign_key, 0);
    }

    #[test]
    fn test_extract_constraints_from_schema_with_metadata() {
        let analyzer = create_test_analyzer();
        let schema = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": false,
                    "metadata": {
                        "unique_constraint": "true"
                    }
                },
                {
                    "name": "email",
                    "type": "string",
                    "nullable": false,
                    "metadata": {
                        "check_constraint": "email LIKE '%@%'"
                    }
                }
            ]
        });

        let (total, check, not_null, unique, foreign_key) =
            analyzer.extract_constraints_from_schema(&schema);

        assert_eq!(total, 2);
        assert_eq!(not_null, 2);
        // The implementation checks for "constraint" or "check" in the key name
        // "check_constraint" contains both "constraint" AND "check", so it increments check twice
        // "unique_constraint" contains both "unique" AND "constraint", so unique=1, check=1
        assert_eq!(check, 2); // Both fields have "constraint" in metadata key
        assert_eq!(unique, 1); // Only id has "unique" in metadata key
        assert_eq!(foreign_key, 0);
    }

    #[test]
    fn test_detect_breaking_schema_changes_no_change() {
        let old_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false}
            ]
        });
        let new_schema = old_schema.clone();

        let breaking = detect_breaking_schema_changes(&old_schema, &new_schema);

        assert!(!breaking);
    }

    #[test]
    fn test_detect_breaking_schema_changes_field_added() {
        let old_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false}
            ]
        });
        let new_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false},
                {"name": "name", "type": "string", "nullable": true}
            ]
        });

        let breaking = detect_breaking_schema_changes(&old_schema, &new_schema);

        // Adding a field is not breaking
        assert!(!breaking);
    }

    #[test]
    fn test_detect_breaking_schema_changes_field_removed() {
        let old_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false},
                {"name": "name", "type": "string", "nullable": true}
            ]
        });
        let new_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false}
            ]
        });

        let breaking = detect_breaking_schema_changes(&old_schema, &new_schema);

        // Removing a field is breaking
        assert!(breaking);
    }

    #[test]
    fn test_detect_breaking_schema_changes_type_changed() {
        let old_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false}
            ]
        });
        let new_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": false}
            ]
        });

        let breaking = detect_breaking_schema_changes(&old_schema, &new_schema);

        // Changing type is breaking
        assert!(breaking);
    }

    #[test]
    fn test_detect_breaking_schema_changes_nullable_changed() {
        let old_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false}
            ]
        });
        let new_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": true}
            ]
        });

        let breaking = detect_breaking_schema_changes(&old_schema, &new_schema);

        // Changing from non-nullable to nullable is breaking
        assert!(breaking);
    }

    #[test]
    fn test_is_breaking_change_no_previous() {
        let previous_changes: Vec<SchemaChange> = vec![];
        let new_schema = json!({
            "type": "struct",
            "fields": [{"name": "id", "type": "integer"}]
        });

        let breaking = is_breaking_change(&previous_changes, &new_schema);

        // First schema is never breaking
        assert!(!breaking);
    }

    #[test]
    fn test_is_breaking_change_with_previous() {
        let previous_changes = vec![SchemaChange::new(
            0,
            1000000,
            json!({
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "name", "type": "string"}
                ]
            }),
            false,
        )];
        let new_schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer"}
            ]
        });

        let breaking = is_breaking_change(&previous_changes, &new_schema);

        // Removing "name" field is breaking
        assert!(breaking);
    }

    #[test]
    fn test_calculate_schema_metrics_no_changes() {
        let analyzer = create_test_analyzer();
        let changes: Vec<SchemaChange> = vec![];

        let result = analyzer.calculate_schema_metrics(changes, 1);

        assert!(result.is_ok());
        let metrics = result.unwrap();
        assert!(metrics.is_some());

        let metrics = metrics.unwrap();
        assert_eq!(metrics.total_schema_changes, 0);
        assert_eq!(metrics.breaking_changes, 0);
        assert_eq!(metrics.non_breaking_changes, 0);
        assert!(metrics.schema_stability_score >= 0.9); // Very stable
    }

    #[test]
    fn test_calculate_schema_metrics_with_changes() {
        let analyzer = create_test_analyzer();
        let now = chrono::Utc::now().timestamp() as u64 * 1000;
        let changes = vec![
            SchemaChange {
                version: 0,
                timestamp: now - 86400000 * 30, // 30 days ago
                schema: json!({"fields": []}),
                is_breaking: false,
            },
            SchemaChange {
                version: 1,
                timestamp: now - 86400000 * 15, // 15 days ago
                schema: json!({"fields": []}),
                is_breaking: true,
            },
            SchemaChange {
                version: 2,
                timestamp: now - 86400000 * 5, // 5 days ago
                schema: json!({"fields": []}),
                is_breaking: false,
            },
        ];

        let result = analyzer.calculate_schema_metrics(changes, 3);

        assert!(result.is_ok());
        let metrics = result.unwrap();
        assert!(metrics.is_some());

        let metrics = metrics.unwrap();
        assert_eq!(metrics.total_schema_changes, 3);
        assert_eq!(metrics.breaking_changes, 1);
        assert_eq!(metrics.non_breaking_changes, 2);
        assert_eq!(metrics.current_schema_version, 3);
        assert!(metrics.days_since_last_change >= 4.0 && metrics.days_since_last_change <= 6.0);
        assert!(metrics.schema_change_frequency > 0.0);
    }

    #[test]
    fn test_metadata_processing_result_default() {
        let result = MetadataProcessingResult::default();

        assert_eq!(result.clustering_columns.len(), 0);
        assert_eq!(result.deletion_vector_count, 0);
        assert_eq!(result.deletion_vector_total_size, 0);
        assert_eq!(result.deleted_rows, 0);
        assert_eq!(result.oldest_dv_age, 0.0);
        assert_eq!(result.total_snapshots, 0);
        assert_eq!(result.total_historical_size, 0);
        assert_eq!(result.oldest_timestamp, 0);
        assert_eq!(result.newest_timestamp, 0);
        assert_eq!(result.total_constraints, 0);
        assert_eq!(result.check_constraints, 0);
        assert_eq!(result.not_null_constraints, 0);
        assert_eq!(result.unique_constraints, 0);
        assert_eq!(result.foreign_key_constraints, 0);
        assert_eq!(result.z_order_columns.len(), 0);
        assert!(!result.z_order_opportunity);
        assert_eq!(result.schema_changes.len(), 0);
    }

    #[test]
    fn test_schema_change_creation() {
        let schema = json!({
            "type": "struct",
            "fields": [{"name": "id", "type": "integer"}]
        });

        let change = SchemaChange {
            version: 5,
            timestamp: 1234567890,
            schema: schema.clone(),
            is_breaking: true,
        };

        assert_eq!(change.version, 5);
        assert_eq!(change.timestamp, 1234567890);
        assert!(change.is_breaking);
        assert_eq!(change.schema, schema);
    }

    #[test]
    fn test_schema_change_clone() {
        let schema = json!({"fields": []});
        let change = SchemaChange {
            version: 1,
            timestamp: 1000,
            schema: schema.clone(),
            is_breaking: false,
        };

        let cloned = change.clone();

        assert_eq!(cloned.version, change.version);
        assert_eq!(cloned.timestamp, change.timestamp);
        assert_eq!(cloned.is_breaking, change.is_breaking);
        assert_eq!(cloned.schema, change.schema);
    }

    #[test]
    fn test_schema_change_debug() {
        let change = SchemaChange {
            version: 1,
            timestamp: 1000,
            schema: json!({"fields": []}),
            is_breaking: true,
        };

        let debug_str = format!("{:?}", change);

        assert!(debug_str.contains("SchemaChange"));
        assert!(debug_str.contains("version"));
        assert!(debug_str.contains("is_breaking"));
    }

    #[test]
    fn test_calculate_storage_cost_impact_zero() {
        let impact = crate::analyze::common::calculate_storage_cost_impact(0, 0, 0.0);

        assert!((0.0..=1.0).contains(&impact));
    }

    #[test]
    fn test_calculate_storage_cost_impact_low() {
        // Small size, few snapshots, recent
        let impact = crate::analyze::common::calculate_storage_cost_impact(1_000_000, 5, 1.0);

        assert!((0.0..=1.0).contains(&impact));
        assert!(impact < 0.5);
    }

    #[test]
    fn test_calculate_storage_cost_impact_high() {
        // Large size, many snapshots, old
        let impact =
            crate::analyze::common::calculate_storage_cost_impact(100_000_000_000, 1000, 365.0);

        assert!((0.0..=1.0).contains(&impact));
        assert!(impact > 0.5);
    }

    #[test]
    fn test_calculate_retention_efficiency_perfect() {
        // Few snapshots, recent data
        let efficiency = calculate_retention_efficiency(10, 7.0, 1.0);

        assert!((0.0..=1.0).contains(&efficiency));
        assert!(efficiency > 0.7); // Should be efficient
    }

    #[test]
    fn test_calculate_retention_efficiency_poor() {
        // Many snapshots (>1000), old data with long retention
        // 1000 snapshots: -0.3, retention 364 days (365-1): no penalty
        // Expected: 1.0 - 0.3 = 0.7
        let efficiency = calculate_retention_efficiency(1000, 365.0, 1.0);

        assert!((0.0..=1.0).contains(&efficiency));
        // With 1000 snapshots, efficiency is penalized by 0.3, so it's 0.7
        assert!((0.6..=0.8).contains(&efficiency));
    }

    #[test]
    fn test_calculate_recommended_retention_few_snapshots() {
        let retention = calculate_recommended_retention(5, 7.0);

        assert!(retention >= 7); // Should recommend at least current retention (in days)
        assert!(retention >= 90); // Low snapshot count should recommend longer retention
    }

    #[test]
    fn test_calculate_recommended_retention_many_snapshots() {
        // With 1000 snapshots (not > 1000), it goes to second condition (> 500)
        // and with oldest_age 365.0 (> 90.0), it returns 60 days
        let retention = calculate_recommended_retention(1000, 365.0);

        assert!(retention > 0);
        assert_eq!(retention, 60); // 1000 snapshots and old age should recommend 60 days
    }

    #[test]
    fn test_extract_deletion_vectors_no_remove() {
        let analyzer = create_test_analyzer();
        let entry = json!({
            "add": {
                "path": "file.parquet"
            }
        });
        let mut result = MetadataProcessingResult::default();

        analyzer.extract_deletion_vectors(&entry, &mut result);

        assert_eq!(result.deletion_vector_count, 0);
        assert_eq!(result.deletion_vector_total_size, 0);
        assert_eq!(result.deleted_rows, 0);
    }

    #[test]
    fn test_extract_deletion_vectors_with_dv() {
        let analyzer = create_test_analyzer();
        let entry = json!({
            "remove": {
                "path": "file.parquet",
                "deletionVector": {
                    "sizeInBytes": 1024,
                    "cardinality": 100
                },
                "timestamp": 1609459200000i64
            }
        });
        let mut result = MetadataProcessingResult::default();

        analyzer.extract_deletion_vectors(&entry, &mut result);

        assert_eq!(result.deletion_vector_count, 1);
        assert_eq!(result.deletion_vector_total_size, 1024);
        assert_eq!(result.deleted_rows, 100);
        assert!(result.oldest_dv_age > 0.0);
    }

    #[test]
    fn test_extract_snapshot_metrics_no_timestamp() {
        let analyzer = create_test_analyzer();
        let entry = json!({
            "add": {
                "path": "file.parquet"
            }
        });
        let mut result = MetadataProcessingResult::default();

        analyzer.extract_snapshot_metrics(&entry, &mut result);

        assert_eq!(result.total_snapshots, 0);
    }

    #[test]
    fn test_extract_snapshot_metrics_with_timestamp() {
        let analyzer = create_test_analyzer();
        let now = chrono::Utc::now().timestamp() as u64 * 1000;
        let entry = json!({
            "timestamp": now
        });
        let mut result = MetadataProcessingResult::default();

        analyzer.extract_snapshot_metrics(&entry, &mut result);

        assert_eq!(result.total_snapshots, 1);
        // oldest_timestamp starts at 0, so min(0, now) = 0
        assert_eq!(result.oldest_timestamp, 0);
        assert_eq!(result.newest_timestamp, now);
    }

    // Configurable mock storage provider for testing async functions
    struct ConfigurableMockStorageProvider {
        base_path: String,
        options: OnceLock<HashMap<String, String>>,
        files: Vec<FileMetadata>,
        file_contents: HashMap<String, Vec<u8>>,
    }

    impl ConfigurableMockStorageProvider {
        fn new(base_path: &str) -> Self {
            Self {
                base_path: base_path.to_string(),
                options: OnceLock::new(),
                files: vec![],
                file_contents: HashMap::new(),
            }
        }

        fn with_file_content(mut self, path: &str, content: Vec<u8>) -> Self {
            self.file_contents.insert(path.to_string(), content);
            self
        }
    }

    #[async_trait]
    impl StorageProvider for ConfigurableMockStorageProvider {
        fn base_path(&self) -> &str {
            &self.base_path
        }

        async fn validate_connection(&self, _path: &str) -> StorageResult<()> {
            Ok(())
        }

        async fn list_files(
            &self,
            _path: &str,
            _recursive: bool,
        ) -> StorageResult<Vec<FileMetadata>> {
            Ok(self.files.clone())
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
            Ok(self.files.clone())
        }

        async fn read_file(&self, path: &str) -> StorageResult<Vec<u8>> {
            if let Some(content) = self.file_contents.get(path) {
                Ok(content.clone())
            } else {
                Ok(vec![])
            }
        }

        async fn exists(&self, _path: &str) -> StorageResult<bool> {
            Ok(true)
        }

        async fn get_metadata(&self, path: &str) -> StorageResult<FileMetadata> {
            Ok(FileMetadata {
                path: path.to_string(),
                size: 0,
                last_modified: None,
            })
        }

        fn options(&self) -> &HashMap<String, String> {
            self.options.get_or_init(HashMap::new)
        }

        fn clean_options(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn uri_from_path(&self, path: &str) -> String {
            path.to_string()
        }
    }

    // ========== Tests for calculate_constraint_violation_risk ==========

    #[test]
    fn test_calculate_constraint_violation_risk_no_constraints() {
        let analyzer = create_test_analyzer();
        let risk = analyzer.calculate_constraint_violation_risk(0, 0);
        assert_eq!(risk, 0.5); // Medium risk when no constraints
    }

    #[test]
    fn test_calculate_constraint_violation_risk_few_constraints_no_check() {
        let analyzer = create_test_analyzer();
        let risk = analyzer.calculate_constraint_violation_risk(3, 0);
        // < 5 constraints = 0.3, no check constraints = 0.3
        assert_eq!(risk, 0.6);
    }

    #[test]
    fn test_calculate_constraint_violation_risk_few_constraints_with_check() {
        let analyzer = create_test_analyzer();
        let risk = analyzer.calculate_constraint_violation_risk(3, 1);
        // < 5 constraints = 0.3, < 3 check constraints = 0.2
        assert_eq!(risk, 0.5);
    }

    #[test]
    fn test_calculate_constraint_violation_risk_medium_constraints() {
        let analyzer = create_test_analyzer();
        let risk = analyzer.calculate_constraint_violation_risk(7, 3);
        // 5-10 constraints = 0.2, >= 3 check constraints = 0.0
        assert_eq!(risk, 0.2);
    }

    #[test]
    fn test_calculate_constraint_violation_risk_many_constraints() {
        let analyzer = create_test_analyzer();
        let risk = analyzer.calculate_constraint_violation_risk(15, 5);
        // 10-20 constraints = 0.1, >= 3 check constraints = 0.0
        assert_eq!(risk, 0.1);
    }

    #[test]
    fn test_calculate_constraint_violation_risk_very_many_constraints() {
        let analyzer = create_test_analyzer();
        let risk = analyzer.calculate_constraint_violation_risk(25, 10);
        // >= 20 constraints = 0.0, >= 3 check constraints = 0.0
        assert_eq!(risk, 0.0);
    }

    // ========== Tests for calculate_data_quality_score ==========

    #[test]
    fn test_calculate_data_quality_score_no_constraints_medium_risk() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_data_quality_score(0, 0.5);
        // 1.0 - (0.5 * 0.5) = 0.75
        assert_eq!(score, 0.75);
    }

    #[test]
    fn test_calculate_data_quality_score_high_risk() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_data_quality_score(0, 1.0);
        // 1.0 - (1.0 * 0.5) = 0.5
        assert_eq!(score, 0.5);
    }

    #[test]
    fn test_calculate_data_quality_score_low_risk_few_constraints() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_data_quality_score(5, 0.1);
        // 1.0 - (0.1 * 0.5) = 0.95
        assert_eq!(score, 0.95);
    }

    #[test]
    fn test_calculate_data_quality_score_low_risk_medium_constraints() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_data_quality_score(15, 0.1);
        // 1.0 - (0.1 * 0.5) + 0.1 = 1.0 (clamped)
        assert_eq!(score, 1.0);
    }

    #[test]
    fn test_calculate_data_quality_score_low_risk_many_constraints() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_data_quality_score(25, 0.0);
        // 1.0 - 0 + 0.2 = 1.0 (clamped)
        assert_eq!(score, 1.0);
    }

    // ========== Tests for calculate_constraint_coverage_score ==========

    #[test]
    fn test_calculate_constraint_coverage_score_no_constraints() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_constraint_coverage_score(0, 0);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_calculate_constraint_coverage_score_no_check_constraints() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_constraint_coverage_score(10, 0);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_calculate_constraint_coverage_score_half_check() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_constraint_coverage_score(10, 5);
        assert_eq!(score, 0.5);
    }

    #[test]
    fn test_calculate_constraint_coverage_score_all_check() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_constraint_coverage_score(10, 10);
        assert_eq!(score, 1.0);
    }

    #[test]
    fn test_calculate_constraint_coverage_score_partial() {
        let analyzer = create_test_analyzer();
        let score = analyzer.calculate_constraint_coverage_score(8, 2);
        assert_eq!(score, 0.25);
    }

    // ========== Tests for categorize_files (TableAnalyzer trait) ==========

    #[test]
    fn test_categorize_files_trait_implementation() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "_delta_log/00000000000000000000.json".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "part-00000.parquet".to_string(),
                size: 50 * 1024 * 1024,
                last_modified: None,
            },
        ];

        // Use the trait method
        let (data_files, metadata_files) = analyzer.categorize_files(files);

        assert_eq!(data_files.len(), 1);
        assert_eq!(metadata_files.len(), 1);
        assert!(data_files[0].path.contains("part-00000.parquet"));
        assert!(metadata_files[0].path.contains("_delta_log"));
    }

    #[test]
    fn test_categorize_files_checkpoint_parquet() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "_delta_log/00000000000000000010.checkpoint.parquet".to_string(),
                size: 4096,
                last_modified: None,
            },
            FileMetadata {
                path: "_delta_log/00000000000000000011.json".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "data/part-00000.parquet".to_string(),
                size: 100 * 1024 * 1024,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_files(files);

        assert_eq!(data_files.len(), 1);
        assert_eq!(metadata_files.len(), 2); // Both JSON and checkpoint
    }

    #[test]
    fn test_categorize_files_non_parquet_ignored() {
        let analyzer = create_test_analyzer();
        let files = vec![
            FileMetadata {
                path: "data/file.csv".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "data/file.json".to_string(),
                size: 2048,
                last_modified: None,
            },
            FileMetadata {
                path: "_delta_log/00000000000000000000.json".to_string(),
                size: 512,
                last_modified: None,
            },
        ];

        let (data_files, metadata_files) = analyzer.categorize_files(files);

        assert_eq!(data_files.len(), 0); // CSV and JSON not in _delta_log are ignored
        assert_eq!(metadata_files.len(), 1);
    }

    // ========== Tests for extract_schema_and_constraints ==========

    #[test]
    fn test_extract_schema_and_constraints_no_metadata() {
        let analyzer = create_test_analyzer();
        let entry = json!({
            "add": {
                "path": "file.parquet"
            }
        });
        let mut result = MetadataProcessingResult::default();

        analyzer.extract_schema_and_constraints(&entry, &mut result, 1);

        assert_eq!(result.total_constraints, 0);
        assert!(result.schema_changes.is_empty());
    }

    #[test]
    fn test_extract_schema_and_constraints_with_schema() {
        let analyzer = create_test_analyzer();
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false},
                {"name": "name", "type": "string", "nullable": true}
            ]
        });
        let entry = json!({
            "metaData": {
                "schemaString": schema.to_string(),
                "id": "test-table"
            },
            "timestamp": 1609459200000i64
        });
        let mut result = MetadataProcessingResult::default();

        analyzer.extract_schema_and_constraints(&entry, &mut result, 1);

        // Should have extracted schema and added a schema change
        assert_eq!(result.schema_changes.len(), 1);
        assert_eq!(result.schema_changes[0].version, 1);
        assert_eq!(result.schema_changes[0].timestamp, 1609459200000);
    }

    #[test]
    fn test_extract_schema_and_constraints_with_not_null() {
        let analyzer = create_test_analyzer();
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false},
                {"name": "email", "type": "string", "nullable": false},
                {"name": "name", "type": "string", "nullable": true}
            ]
        });
        let entry = json!({
            "metaData": {
                "schemaString": schema.to_string()
            }
        });
        let mut result = MetadataProcessingResult::default();

        analyzer.extract_schema_and_constraints(&entry, &mut result, 1);

        // Two NOT NULL constraints (id and email)
        assert_eq!(result.not_null_constraints, 2);
        // total_constraints counts all fields (3 fields)
        assert_eq!(result.total_constraints, 3);
    }

    // ========== Tests for find_referenced_files ==========

    #[tokio::test]
    async fn test_find_referenced_files_empty() {
        let mock_storage = Arc::new(ConfigurableMockStorageProvider::new("/test"));
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let result = analyzer.find_referenced_files(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_referenced_files_with_add_actions() {
        let json_content = r#"{"add":{"path":"part-00000.parquet","size":1024}}
{"add":{"path":"part-00001.parquet","size":2048}}"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test").with_file_content(
                "_delta_log/00000000000000000000.json",
                json_content.as_bytes().to_vec(),
            ),
        );
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let metadata_files = vec![FileMetadata {
            path: "_delta_log/00000000000000000000.json".to_string(),
            size: 100,
            last_modified: None,
        }];

        let result = analyzer
            .find_referenced_files(&metadata_files)
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"part-00000.parquet".to_string()));
        assert!(result.contains(&"part-00001.parquet".to_string()));
    }

    #[tokio::test]
    async fn test_find_referenced_files_ignores_checkpoints() {
        let mock_storage = Arc::new(ConfigurableMockStorageProvider::new("/test"));
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        // Checkpoint files should be filtered out (only .json files are processed)
        let metadata_files = vec![FileMetadata {
            path: "_delta_log/00000000000000000010.checkpoint.parquet".to_string(),
            size: 4096,
            last_modified: None,
        }];

        let result = analyzer
            .find_referenced_files(&metadata_files)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_referenced_files_multiple_json_files() {
        let json1 = r#"{"add":{"path":"file1.parquet","size":1024}}"#;
        let json2 = r#"{"add":{"path":"file2.parquet","size":2048}}"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test")
                .with_file_content(
                    "_delta_log/00000000000000000000.json",
                    json1.as_bytes().to_vec(),
                )
                .with_file_content(
                    "_delta_log/00000000000000000001.json",
                    json2.as_bytes().to_vec(),
                ),
        );
        let analyzer = DeltaAnalyzer::new(mock_storage, 2);

        let metadata_files = vec![
            FileMetadata {
                path: "_delta_log/00000000000000000000.json".to_string(),
                size: 50,
                last_modified: None,
            },
            FileMetadata {
                path: "_delta_log/00000000000000000001.json".to_string(),
                size: 50,
                last_modified: None,
            },
        ];

        let result = analyzer
            .find_referenced_files(&metadata_files)
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    // ========== Tests for process_single_metadata_file ==========

    #[tokio::test]
    async fn test_process_single_metadata_file_json() {
        let json_content = r#"{"add":{"path":"file.parquet","size":1024}}"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test").with_file_content(
                "_delta_log/00000000000000000000.json",
                json_content.as_bytes().to_vec(),
            ),
        );
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let result = analyzer
            .process_single_metadata_file("_delta_log/00000000000000000000.json", 0)
            .await
            .unwrap();

        // Basic result should be returned
        assert!(result.schema_changes.is_empty());
    }

    #[tokio::test]
    async fn test_process_single_metadata_file_with_clustering() {
        let json_content = r#"{"clusterBy":["col1","col2"]}"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test").with_file_content(
                "_delta_log/00000000000000000000.json",
                json_content.as_bytes().to_vec(),
            ),
        );
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let result = analyzer
            .process_single_metadata_file("_delta_log/00000000000000000000.json", 0)
            .await
            .unwrap();

        assert!(result.z_order_opportunity);
        assert_eq!(result.clustering_columns, vec!["col1", "col2"]);
    }

    #[tokio::test]
    async fn test_process_single_metadata_file_with_deletion_vector() {
        let json_content = r#"{"add":{"path":"file.parquet","deletionVector":{"sizeInBytes":512,"cardinality":50},"timestamp":1609459200000}}"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test").with_file_content(
                "_delta_log/00000000000000000000.json",
                json_content.as_bytes().to_vec(),
            ),
        );
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let result = analyzer
            .process_single_metadata_file("_delta_log/00000000000000000000.json", 0)
            .await
            .unwrap();

        assert_eq!(result.deletion_vector_count, 1);
        assert_eq!(result.deletion_vector_total_size, 512);
        assert_eq!(result.deleted_rows, 50);
    }

    #[tokio::test]
    async fn test_process_single_metadata_file_with_schema() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false}
            ]
        });
        let json_content = json!({
            "metaData": {
                "schemaString": schema.to_string()
            },
            "timestamp": 1609459200000i64
        })
        .to_string();

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test").with_file_content(
                "_delta_log/00000000000000000000.json",
                json_content.as_bytes().to_vec(),
            ),
        );
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let result = analyzer
            .process_single_metadata_file("_delta_log/00000000000000000000.json", 0)
            .await
            .unwrap();

        assert_eq!(result.schema_changes.len(), 1);
        assert_eq!(result.not_null_constraints, 1);
    }

    // ========== Tests for update_metrics_from_delta_metadata ==========

    #[tokio::test]
    async fn test_update_metrics_from_delta_metadata_empty() {
        let mock_storage = Arc::new(ConfigurableMockStorageProvider::new("/test"));
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let mut metrics = HealthMetrics::default();
        let result = analyzer
            .update_metrics_from_delta_metadata(&[], 0, 0, &mut metrics)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_update_metrics_from_delta_metadata_with_json() {
        let json_content = r#"{"add":{"path":"file.parquet","size":1024,"deletionVector":{"sizeInBytes":256,"cardinality":10},"timestamp":1609459200000}}"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test").with_file_content(
                "_delta_log/00000000000000000000.json",
                json_content.as_bytes().to_vec(),
            ),
        );
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let metadata_files = vec![FileMetadata {
            path: "_delta_log/00000000000000000000.json".to_string(),
            size: 100,
            last_modified: None,
        }];

        let mut metrics = HealthMetrics::default();
        let result = analyzer
            .update_metrics_from_delta_metadata(&metadata_files, 1024 * 1024, 10, &mut metrics)
            .await;

        assert!(result.is_ok());
        // Deletion vector metrics should be populated
        assert!(metrics.deletion_vector_metrics.is_some());
        let dv = metrics.deletion_vector_metrics.as_ref().unwrap();
        assert_eq!(dv.deletion_vector_count, 1);
        assert_eq!(dv.total_deletion_vector_size_bytes, 256);
    }

    #[tokio::test]
    async fn test_update_metrics_from_delta_metadata_with_schema() {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false},
                {"name": "name", "type": "string", "nullable": true}
            ]
        });
        let json_content = json!({
            "metaData": {
                "schemaString": schema.to_string()
            },
            "timestamp": 1609459200000i64
        })
        .to_string();

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test").with_file_content(
                "_delta_log/00000000000000000000.json",
                json_content.as_bytes().to_vec(),
            ),
        );
        let analyzer = DeltaAnalyzer::new(mock_storage, 1);

        let metadata_files = vec![FileMetadata {
            path: "_delta_log/00000000000000000000.json".to_string(),
            size: 200,
            last_modified: None,
        }];

        let mut metrics = HealthMetrics::default();
        let result = analyzer
            .update_metrics_from_delta_metadata(&metadata_files, 100 * 1024 * 1024, 5, &mut metrics)
            .await;

        assert!(result.is_ok());
        // Schema evolution metrics should be populated
        assert!(metrics.schema_evolution.is_some());
        // Constraints metrics should be populated
        assert!(metrics.table_constraints.is_some());
    }
}
