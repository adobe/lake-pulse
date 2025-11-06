use crate::analyze::metrics::{
    ClusteringInfo, DeletionVectorMetrics, FileCompactionMetrics, HealthMetrics,
    SchemaEvolutionMetrics, TableConstraintsMetrics, TimeTravelMetrics,
};
use crate::analyze::table_analyzer::TableAnalyzer;
use crate::storage::{FileMetadata, StorageProvider};
use crate::util::util::is_ndjson;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use serde_json::Value;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::info;

#[derive(Debug, Clone)]
pub struct SchemaChange {
    #[allow(dead_code)]
    pub version: u64,
    pub timestamp: u64,
    pub schema: Value,
    pub is_breaking: bool,
}

// Intermediate structure to hold results from parallel metadata processing
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

/// Delta Lake-specific analyzer for processing Delta transaction logs
pub struct DeltaAnalyzer {
    storage_provider: Arc<dyn StorageProvider>,
    parallelism: usize,
}

impl DeltaAnalyzer {
    /// Create a new DeltaAnalyzer
    pub fn new(storage_provider: Arc<dyn StorageProvider>, parallelism: usize) -> Self {
        Self {
            storage_provider,
            parallelism,
        }
    }

    /// Categorize files into data files and Delta metadata files
    pub fn categorize_delta_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        let mut data_files = Vec::new();
        let mut metadata_files = Vec::new();

        for obj in objects {
            if obj.path.ends_with(".parquet") {
                data_files.push(obj);
            } else if obj.path.contains("_delta_log/") && obj.path.ends_with(".json") {
                // TODO: Should it contain the checkpoint parquet files that resides in delta_log?
                // TODO: Should we consider `_change_data` files as metadata?
                metadata_files.push(obj);
            }
        }

        (data_files, metadata_files)
    }

    /// Find referenced files from Delta transaction logs
    pub async fn find_referenced_files(
        &self,
        metadata_files: &Vec<FileMetadata>,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let storage_provider = Arc::clone(&self.storage_provider);
        let metadata_files_owned = metadata_files.clone();

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
                            vec![
                                serde_json::from_str(&content_str)
                                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?,
                            ]
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
                            if let Some(add_actions) = entry.get("add") {
                                if let Some(add_array) = add_actions.as_array() {
                                    for add_action in add_array {
                                        if let Some(path) = add_action.get("path") {
                                            if let Some(path_str) = path.as_str() {
                                                file_refs.push(path_str.to_string());
                                            }
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

    /// Process a single Delta metadata file and extract metrics
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

    /// Extract deletion vector metrics from a Delta log entry
    fn extract_deletion_vectors(&self, entry: &Value, result: &mut MetadataProcessingResult) {
        if let Some(remove_actions) = entry.get("remove") {
            if let Some(remove_array) = remove_actions.as_array() {
                for remove_action in remove_array {
                    if let Some(deletion_vector) = remove_action.get("deletionVector") {
                        result.deletion_vector_count += 1;

                        if let Some(size) = deletion_vector.get("sizeInBytes") {
                            result.deletion_vector_total_size += size.as_u64().unwrap_or(0);
                        }

                        if let Some(rows) = deletion_vector.get("cardinality") {
                            result.deleted_rows += rows.as_u64().unwrap_or(0);
                        }

                        if let Some(timestamp) = remove_action.get("timestamp") {
                            let creation_time = timestamp.as_u64().unwrap_or(0) as i64;
                            let age_days = (chrono::Utc::now().timestamp() - creation_time / 1000)
                                as f64
                                / 86400.0;
                            result.oldest_dv_age = result.oldest_dv_age.max(age_days);
                        }
                    }
                }
            }
        }
    }

    /// Extract snapshot/time travel metrics from a Delta log entry
    fn extract_snapshot_metrics(&self, entry: &Value, result: &mut MetadataProcessingResult) {
        if let Some(timestamp) = entry.get("timestamp") {
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

    /// Extract schema and constraints from a Delta log entry
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

                    let is_breaking = self.is_breaking_change(&result.schema_changes, &schema);
                    result.schema_changes.push(SchemaChange {
                        version: current_version,
                        timestamp: entry.get("timestamp").and_then(|t| t.as_u64()).unwrap_or(0),
                        schema,
                        is_breaking,
                    });
                }
            }
        }
    }

    /// Update health metrics from Delta metadata files
    pub async fn update_metrics_from_delta_metadata(
        &self,
        metadata_files: &Vec<FileMetadata>,
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let parallelism = self.parallelism.max(1);

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

    /// Calculate deletion vector impact score
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
            (now - last_change.timestamp / 1000) as f64 / 86400.0
        } else {
            365.0 // No changes in a year = very stable
        };

        // Calculate change frequency (changes per day)
        let total_days = if changes.len() > 1 {
            let first_change = changes.first().unwrap().timestamp / 1000;
            let last_change = changes.last().unwrap().timestamp / 1000;
            ((last_change - first_change) as f64 / 86400.0).max(1.0_f64)
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
        // Simplified breaking change detection
        // In a real implementation, this would be more sophisticated
        if let (Some(old_fields), Some(new_fields)) =
            (old_schema.get("fields"), new_schema.get("fields"))
        {
            if let (Some(old_fields_array), Some(new_fields_array)) =
                (old_fields.as_array(), new_fields.as_array())
            {
                // Check if any fields were removed
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

                            // Check if nullable changed from false to true (breaking)
                            let old_nullable = old_field
                                .get("nullable")
                                .and_then(|n| n.as_bool())
                                .unwrap_or(true);
                            let new_nullable = new_field
                                .get("nullable")
                                .and_then(|n| n.as_bool())
                                .unwrap_or(true);

                            if !old_nullable && new_nullable {
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

    /// Estimate snapshot size from a Delta log entry
    fn estimate_snapshot_size(&self, json: &Value) -> u64 {
        let mut size = 0u64;

        // Estimate size based on actions in the transaction log
        if let Some(add_actions) = json.get("add") {
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

    /// Extract constraints from Delta schema
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

    /// Calculate constraint violation risk
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

    /// Calculate data quality score
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

    /// Calculate constraint coverage score
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
        self.update_metrics_from_delta_metadata(
            metadata_files,
            data_files_total_size,
            data_files_total_files,
            metrics,
        )
        .await
    }
}
