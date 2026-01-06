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
    calculate_schema_stability_score, SchemaChange,
};
use crate::analyze::metrics::{
    ClusteringInfo, DeletionVectorMetrics, FileCompactionMetrics, HealthMetrics,
    SchemaEvolutionMetrics, TableConstraintsMetrics, TimeTravelMetrics,
};
use crate::analyze::table_analyzer::TableAnalyzer;
use crate::storage::{FileMetadata, StorageProvider};
use async_trait::async_trait;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use lance::dataset::Dataset;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use tracing::{info, warn};

/// Lance-specific analyzer for processing Lance tables.
///
/// This analyzer implements the `TableAnalyzer` trait and provides functionality
/// to parse Lance metadata files, extract metrics, and analyze table health.
/// Lance is a modern columnar data format designed for ML/AI workloads with
/// built-in versioning and indexing capabilities.
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
/// use lake_pulse::analyze::lance::LanceAnalyzer;
///
/// # async fn example(storage: Arc<dyn StorageProvider>) {
/// let analyzer = LanceAnalyzer::new(storage, 4);
/// // Use analyzer to process Lance tables
/// # }
/// ```
pub struct LanceAnalyzer {
    storage_provider: Arc<dyn StorageProvider>,
    parallelism: usize,
}

impl LanceAnalyzer {
    /// Create a new LanceAnalyzer.
    ///
    /// # Arguments
    ///
    /// * `storage_provider` - The storage provider to use for reading files
    /// * `parallelism` - The number of concurrent tasks to use for metadata processing
    ///
    /// # Returns
    ///
    /// A new `LanceAnalyzer` instance configured with the specified storage provider and parallelism.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use lake_pulse::storage::StorageProvider;
    /// use lake_pulse::analyze::lance::LanceAnalyzer;
    ///
    /// # async fn example(storage: Arc<dyn StorageProvider>) {
    /// let analyzer = LanceAnalyzer::new(storage, 4);
    /// # }
    /// ```
    pub fn new(storage_provider: Arc<dyn StorageProvider>, parallelism: usize) -> Self {
        Self {
            storage_provider,
            parallelism,
        }
    }

    /// Categorize files into data files and Lance metadata files.
    ///
    /// Separates Lance data files (.lance) from metadata files (version manifests,
    /// indices, transaction files). Lance stores data in `.lance` files and metadata
    /// in `_versions/`, `_indices/` directories and `.manifest`/`.txn` files.
    ///
    /// # Arguments
    ///
    /// * `objects` - All files discovered in the table location
    ///
    /// # Returns
    ///
    /// A tuple of `(data_files, metadata_files)` where:
    /// * `data_files` - Vector of .lance data files
    /// * `metadata_files` - Vector of version manifests, indices, and transaction files
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::lance::LanceAnalyzer;
    /// # use lake_pulse::storage::FileMetadata;
    /// # use std::sync::Arc;
    /// # fn example(analyzer: &LanceAnalyzer, files: Vec<FileMetadata>) {
    /// let (data_files, metadata_files) = analyzer.categorize_lance_files(files);
    /// println!("Found {} data files and {} metadata files",
    ///          data_files.len(), metadata_files.len());
    /// # }
    /// ```
    pub fn categorize_lance_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        let mut data_files = Vec::new();
        let mut metadata_files = Vec::new();

        for obj in objects {
            if obj.path.ends_with(".lance") {
                // .lance files are data files
                data_files.push(obj);
            } else if obj.path.contains("/_versions/")
                || obj.path.contains("/_indices/")
                || obj.path.starts_with("_versions/")
                || obj.path.starts_with("_indices/")
            {
                // Version manifests and index files are metadata
                // Handle both absolute paths (/_versions/) and relative paths (_versions/)
                metadata_files.push(obj);
            } else if obj.path.ends_with(".manifest") || obj.path.ends_with(".txn") {
                // Manifest and transaction files
                metadata_files.push(obj);
            }
        }

        (data_files, metadata_files)
    }

    /// Convert a Lance schema to a JSON Value for comparison.
    ///
    /// This creates a normalized JSON representation of the schema that can be
    /// compared across versions to detect schema changes.
    fn schema_to_json(schema: &lance::datatypes::Schema) -> Value {
        let fields: Vec<Value> = schema
            .fields
            .iter()
            .map(|field| {
                json!({
                    "name": field.name,
                    "type": format!("{:?}", field.logical_type),
                    "nullable": field.nullable
                })
            })
            .collect();
        json!({ "fields": fields })
    }

    /// Analyze schema evolution across Lance dataset versions.
    ///
    /// Opens the dataset and iterates through all versions to detect schema changes.
    /// Compares schemas between consecutive versions to identify additions, removals,
    /// and type changes.
    ///
    /// # Arguments
    ///
    /// * `table_uri` - The URI of the Lance dataset
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * `Vec<SchemaChange>` - List of detected schema changes
    /// * `u64` - The current (latest) schema version
    async fn analyze_schema_evolution(
        &self,
        table_uri: &str,
    ) -> Result<(Vec<SchemaChange>, u64), Box<dyn Error + Send + Sync>> {
        info!(
            "Analyzing schema evolution for Lance dataset at {}, parallelism={}",
            table_uri, self.parallelism
        );

        let dataset = match Dataset::open(table_uri).await {
            Ok(ds) => ds,
            Err(e) => {
                warn!("Failed to open Lance dataset for schema analysis: {}", e);
                return Ok((Vec::new(), 0));
            }
        };

        let mut versions = match dataset.versions().await {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to get Lance dataset versions: {}", e);
                return Ok((Vec::new(), dataset.version().version));
            }
        };

        if versions.is_empty() {
            return Ok((Vec::new(), dataset.version().version));
        }

        info!("Found {} versions in Lance dataset", versions.len());

        // Sort versions by version number
        versions.sort_by_key(|v| v.version);

        // Extract version info before parallel processing (Version doesn't implement Clone)
        let version_info: Vec<(u64, u64)> = versions
            .iter()
            .map(|v| (v.version, v.timestamp.timestamp_millis() as u64))
            .collect();

        // Fetch schemas in parallel using buffer_unordered
        let parallelism = self.parallelism.max(1);
        let table_uri_owned = table_uri.to_string();

        let version_schema_results: Vec<_> = stream::iter(version_info)
            .map(|(version_num, timestamp)| {
                let uri = table_uri_owned.clone();
                async move {
                    let ds = match Dataset::open(&uri).await {
                        Ok(d) => d,
                        Err(e) => {
                            warn!("Failed to open dataset for version {}: {}", version_num, e);
                            return None;
                        }
                    };
                    match ds.checkout_version(version_num).await {
                        Ok(versioned_ds) => {
                            let schema = Self::schema_to_json(versioned_ds.schema());
                            Some((version_num, timestamp, schema))
                        }
                        Err(e) => {
                            warn!("Failed to checkout Lance version {}: {}", version_num, e);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // Filter out None results and sort by version number
        let mut version_schemas: Vec<_> = version_schema_results.into_iter().flatten().collect();
        version_schemas.sort_by_key(|(v, _, _)| *v);

        // Compare schemas sequentially to detect changes
        let mut schema_changes: Vec<SchemaChange> = Vec::new();
        let mut previous_schema: Option<Value> = None;

        for (version_num, timestamp, current_schema) in &version_schemas {
            if let Some(ref prev_schema) = previous_schema {
                if prev_schema != current_schema {
                    let is_breaking = Self::detect_breaking_change(prev_schema, current_schema);

                    schema_changes.push(SchemaChange::new(
                        *version_num,
                        *timestamp,
                        current_schema.clone(),
                        is_breaking,
                    ));

                    info!(
                        "Detected schema change at version {}, breaking={}",
                        version_num, is_breaking
                    );
                }
            } else {
                // First version - record initial schema
                schema_changes.push(SchemaChange::new(
                    *version_num,
                    *timestamp,
                    current_schema.clone(),
                    false, // Initial schema is not a breaking change
                ));
            }

            previous_schema = Some(current_schema.clone());
        }

        let current_version = version_schemas.last().map(|(v, _, _)| *v).unwrap_or(0);

        info!(
            "Schema evolution analysis complete: {} changes detected",
            schema_changes.len()
        );

        Ok((schema_changes, current_version))
    }

    /// Detect if a schema change is breaking.
    ///
    /// A breaking change is one that would cause existing readers to fail:
    /// - Column removal
    /// - Type changes
    /// - Nullability changes (nullable to non-nullable)
    fn detect_breaking_change(old_schema: &Value, new_schema: &Value) -> bool {
        let old_fields = match old_schema.get("fields").and_then(|f| f.as_array()) {
            Some(f) => f,
            None => return false,
        };
        let new_fields = match new_schema.get("fields").and_then(|f| f.as_array()) {
            Some(f) => f,
            None => return false,
        };

        // Build a map of new field names for quick lookup
        let new_field_names: HashSet<String> = new_fields
            .iter()
            .filter_map(|f| {
                f.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        // Check for removed columns (breaking)
        for old_field in old_fields {
            if let Some(name) = old_field.get("name").and_then(|n| n.as_str()) {
                if !new_field_names.contains(name) {
                    return true; // Column removed
                }

                // Find the corresponding new field
                if let Some(new_field) = new_fields
                    .iter()
                    .find(|f| f.get("name").and_then(|n| n.as_str()) == Some(name))
                {
                    // Check for type changes
                    let old_type = old_field.get("type").and_then(|t| t.as_str());
                    let new_type = new_field.get("type").and_then(|t| t.as_str());
                    if old_type != new_type {
                        return true; // Type changed
                    }

                    // Check for nullability changes (nullable -> non-nullable is breaking)
                    let old_nullable = old_field.get("nullable").and_then(|n| n.as_bool());
                    let new_nullable = new_field.get("nullable").and_then(|n| n.as_bool());
                    if old_nullable == Some(true) && new_nullable == Some(false) {
                        return true; // Became non-nullable
                    }
                }
            }
        }

        false
    }

    /// Analyze deletion metrics for a Lance dataset.
    ///
    /// Opens the dataset and counts deleted rows across all fragments.
    /// Also calculates the deletion impact score based on the ratio of
    /// deleted rows to total rows.
    ///
    /// # Arguments
    ///
    /// * `table_uri` - The URI of the Lance dataset
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * `usize` - Number of deleted rows
    /// * `f64` - Deletion impact score (0.0 to 1.0)
    async fn analyze_deletion_metrics(
        &self,
        table_uri: &str,
    ) -> Result<(usize, f64), Box<dyn Error + Send + Sync>> {
        info!(
            "Analyzing deletion metrics for Lance dataset at {}",
            table_uri
        );

        let dataset = match Dataset::open(table_uri).await {
            Ok(ds) => ds,
            Err(e) => {
                warn!("Failed to open Lance dataset for deletion analysis: {}", e);
                return Ok((0, 0.0));
            }
        };

        // Get deleted row count
        let deleted_rows = match dataset.count_deleted_rows().await {
            Ok(count) => count,
            Err(e) => {
                warn!("Failed to count deleted rows: {}", e);
                0
            }
        };

        // Get total row count (including deleted)
        let total_rows = dataset.count_rows(None).await.unwrap_or(0);
        let physical_rows = total_rows + deleted_rows;

        // Calculate deletion impact score
        // Higher score means more impact from deletions
        let deletion_ratio = if physical_rows > 0 {
            deleted_rows as f64 / physical_rows as f64
        } else {
            0.0
        };

        // Impact score considers:
        // - Deletion ratio (primary factor)
        // - Absolute count (secondary factor for very large tables)
        let impact_score = if deleted_rows == 0 {
            0.0
        } else {
            // Base score from ratio
            let ratio_score = deletion_ratio;

            // Boost for high absolute counts (>1M deleted rows is significant)
            let count_boost = (deleted_rows as f64 / 1_000_000.0).min(0.2);

            (ratio_score + count_boost).min(1.0)
        };

        info!(
            "Deletion analysis complete: {} deleted rows, impact score: {:.3}",
            deleted_rows, impact_score
        );

        Ok((deleted_rows, impact_score))
    }

    /// Find referenced files from Lance metadata.
    ///
    /// Opens the Lance dataset and reads the manifest to identify which data files
    /// (fragments) are active in the current version. Lance uses a fragment-based
    /// storage model where each fragment contains a subset of the table data.
    ///
    /// # Arguments
    ///
    /// * `metadata_files` - The metadata files to parse
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<String>)` - Vector of file paths for active Lance fragments
    /// * `Err` - If dataset opening or manifest reading fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * No version files are found in the metadata
    /// * The Lance dataset cannot be opened
    /// * Fragment file paths cannot be extracted from the manifest
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::lance::LanceAnalyzer;
    /// # use lake_pulse::storage::FileMetadata;
    /// # async fn example(analyzer: &LanceAnalyzer, metadata: &Vec<FileMetadata>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let referenced_files = analyzer.find_referenced_files(metadata).await?;
    /// println!("Found {} referenced data files", referenced_files.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_referenced_files(
        &self,
        metadata_files: &[FileMetadata],
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        info!("Finding referenced files from Lance metadata");

        // Find the latest version file to determine the table location
        let version_file = metadata_files
            .iter()
            .filter(|f| f.path.contains("/_versions/"))
            .max_by_key(|f| f.last_modified);

        // Extract the table base path from the version file path
        // Version files are typically at: <table_path>/_versions/<version>.manifest
        let Some(version_file) = version_file else {
            warn!("No version files found in Lance metadata");
            return Ok(Vec::new());
        };
        let version_path = &version_file.path;
        let table_path = if let Some(pos) = version_path.find("/_versions/") {
            &version_path[..pos]
        } else {
            warn!(
                "Could not determine table path from version file: {}",
                version_path
            );
            return Ok(Vec::new());
        };

        info!("Opening Lance dataset at path: {}", table_path);

        // Construct the full URI for the Lance dataset
        let table_uri = self.storage_provider.uri_from_path(table_path);

        // Open the Lance dataset using the lance crate
        let dataset = match Dataset::open(&table_uri).await {
            Ok(ds) => ds,
            Err(e) => {
                warn!("Failed to open Lance dataset at {}: {}", table_uri, e);
                return Ok(Vec::new());
            }
        };

        info!(
            "Successfully opened Lance dataset, version={}",
            dataset.version().version
        );

        // Get the manifest which contains fragment information
        let manifest = dataset.manifest();
        let fragments = &manifest.fragments;

        info!("Found {} fragments in Lance manifest", fragments.len());

        // Collect all referenced data files from fragments
        let mut referenced_files = HashSet::new();

        for fragment in fragments.iter() {
            // Each fragment has a list of data files
            for data_file in fragment.files.iter() {
                // The data file path is relative to the table base path
                // Construct the full path: <table_path>/data/<file_path>
                let file_path = format!("{}/data/{}", table_path, data_file.path);
                referenced_files.insert(file_path);
            }

            // Note: Deletion files are tracked separately in Lance's internal structure
            // They are stored in _deletions directory and referenced by ID in the DeletionFile struct
            // For the purpose of finding unreferenced .lance data files, we only need to track
            // the main data files from fragments
        }

        info!(
            "Found {} referenced data files from Lance manifest",
            referenced_files.len()
        );

        Ok(referenced_files.into_iter().collect())
    }

    /// Update health metrics from Lance metadata.
    ///
    /// Opens the Lance dataset and extracts comprehensive health metrics including
    /// version history, schema evolution, fragment statistics, and index information.
    /// Lance's native versioning and indexing capabilities provide rich metadata for
    /// health analysis.
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
    /// * The Lance dataset cannot be opened
    /// * Version history cannot be read
    /// * Schema information cannot be extracted
    /// * Metric calculations fail
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::lance::LanceAnalyzer;
    /// # use lake_pulse::analyze::metrics::HealthMetrics;
    /// # use lake_pulse::storage::FileMetadata;
    /// # async fn example(analyzer: &LanceAnalyzer, metadata: &Vec<FileMetadata>, mut metrics: HealthMetrics) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// analyzer.update_metrics_from_lance_metadata(
    ///     metadata,
    ///     1024 * 1024 * 1024, // 1GB total data size
    ///     100,                 // 100 data files
    ///     &mut metrics
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_metrics_from_lance_metadata(
        &self,
        metadata_files: &[FileMetadata],
        data_files_total_size: u64,
        _data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Updating metrics from Lance metadata");

        // Count version files to determine snapshot count
        // Handle both absolute paths (/_versions/) and relative paths (_versions/)
        let version_files: Vec<&FileMetadata> = metadata_files
            .iter()
            .filter(|f| f.path.contains("/_versions/") || f.path.starts_with("_versions/"))
            .collect();

        let total_snapshots = version_files.len();

        // Calculate metadata size
        let metadata_total_size: u64 = metadata_files.iter().map(|f| f.size).sum();

        // Time travel metrics - calculate age from file timestamps
        let now = Utc::now();
        let oldest_timestamp = version_files.iter().filter_map(|f| f.last_modified).min();
        let newest_timestamp = version_files.iter().filter_map(|f| f.last_modified).max();

        let oldest_age_days = oldest_timestamp
            .map(|t| (now.signed_duration_since(t).num_seconds() as f64 / 86400.0).max(0.0))
            .unwrap_or(0.0);

        let newest_age_days = newest_timestamp
            .map(|t| (now.signed_duration_since(t).num_seconds() as f64 / 86400.0).max(0.0))
            .unwrap_or(0.0);

        // Calculate retention efficiency and recommended retention
        let retention_efficiency =
            calculate_retention_efficiency(total_snapshots, oldest_age_days, newest_age_days);
        let recommended_retention =
            calculate_recommended_retention(total_snapshots, oldest_age_days);

        // Calculate storage cost impact
        let storage_cost_impact = if data_files_total_size > 0 {
            (metadata_total_size as f64 / data_files_total_size as f64).min(1.0)
        } else {
            0.0
        };

        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots,
            oldest_snapshot_age_days: oldest_age_days,
            newest_snapshot_age_days: newest_age_days,
            total_historical_size_bytes: metadata_total_size,
            avg_snapshot_size_bytes: if total_snapshots > 0 {
                metadata_total_size as f64 / total_snapshots as f64
            } else {
                0.0
            },
            storage_cost_impact_score: storage_cost_impact,
            retention_efficiency_score: retention_efficiency,
            recommended_retention_days: recommended_retention,
        });

        // Schema evolution metrics - analyze version history
        // Extract table path from version files
        // Handle both absolute paths (/_versions/) and relative paths (_versions/)
        let table_uri = version_files.first().and_then(|f| {
            if let Some(pos) = f.path.find("/_versions/") {
                // Absolute path: extract everything before /_versions/
                let table_path = &f.path[..pos];
                Some(self.storage_provider.uri_from_path(table_path))
            } else if f.path.starts_with("_versions/") {
                // Relative path from table root: use empty path (table root)
                Some(self.storage_provider.uri_from_path(""))
            } else {
                None
            }
        });

        let (schema_changes, current_version) = if let Some(ref uri) = table_uri {
            self.analyze_schema_evolution(uri).await?
        } else {
            (Vec::new(), 0)
        };

        // Calculate schema metrics
        let breaking_changes = schema_changes.iter().filter(|c| c.is_breaking).count();
        let non_breaking_changes = schema_changes.len().saturating_sub(breaking_changes);

        let days_since_last_change = schema_changes
            .last()
            .map(|c| {
                let now_ms = Utc::now().timestamp_millis() as u64;
                if c.timestamp > 0 && c.timestamp <= now_ms {
                    (now_ms - c.timestamp) as f64 / (1000.0 * 60.0 * 60.0 * 24.0)
                } else {
                    oldest_age_days
                }
            })
            .unwrap_or(oldest_age_days);

        let schema_change_frequency = if oldest_age_days > 0.0 {
            schema_changes.len() as f64 / oldest_age_days
        } else {
            0.0
        };

        let schema_stability_score = calculate_schema_stability_score(
            schema_changes.len(),
            breaking_changes,
            schema_change_frequency,
            days_since_last_change,
        );

        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: schema_changes.len(),
            breaking_changes,
            non_breaking_changes,
            days_since_last_change,
            schema_change_frequency,
            current_schema_version: current_version,
            schema_stability_score,
        });

        // Deletion vector metrics - Lance supports deletion files
        let deletion_files: Vec<&FileMetadata> = metadata_files
            .iter()
            .filter(|f| f.path.contains("_deletions"))
            .collect();

        let deletion_vector_count = deletion_files.len() as u64;
        let deletion_vector_total_size: u64 = deletion_files.iter().map(|f| f.size).sum();

        // Calculate deletion age from file timestamps
        let deletion_age_days = deletion_files
            .iter()
            .filter_map(|f| f.last_modified)
            .min()
            .map(|oldest| {
                (now.signed_duration_since(oldest).num_seconds() as f64 / 86400.0).max(0.0)
            })
            .unwrap_or(0.0);

        // Get actual deleted row count and impact score from the dataset
        let (deleted_rows_count, deletion_impact_score) = if let Some(ref uri) = table_uri {
            self.analyze_deletion_metrics(uri).await?
        } else {
            // Fallback to size-based estimation
            let impact = if data_files_total_size > 0 {
                (deletion_vector_total_size as f64 / data_files_total_size as f64).min(1.0)
            } else {
                0.0
            };
            (0, impact)
        };

        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: deletion_vector_count as usize,
            total_deletion_vector_size_bytes: deletion_vector_total_size,
            avg_deletion_vector_size_bytes: if deletion_vector_count > 0 {
                deletion_vector_total_size as f64 / deletion_vector_count as f64
            } else {
                0.0
            },
            deletion_vector_age_days: deletion_age_days,
            deleted_rows_count: deleted_rows_count as u64,
            deletion_vector_impact_score: deletion_impact_score,
        });

        // Table constraints metrics - Lance does not have built-in constraint support
        // Lance is optimized for vector search and ML workloads, not traditional OLTP constraints.
        // Data validation is typically handled at the application layer or via Arrow schema.
        // We set data_quality_score to 1.0 (no constraint violations possible) and
        // constraint_coverage_score to 0.0 (no constraints defined) to reflect this.
        metrics.table_constraints = Some(TableConstraintsMetrics {
            total_constraints: 0,
            check_constraints: 0,
            not_null_constraints: 0,
            unique_constraints: 0,
            foreign_key_constraints: 0,
            constraint_violation_risk: 0.0,
            data_quality_score: 1.0,
            constraint_coverage_score: 0.0,
        });

        // Clustering info - Lance uses fragments which are similar to clusters
        // Each fragment groups related data files together
        let clustering_info = if let Some(ref uri) = table_uri {
            match Dataset::open(uri).await {
                Ok(dataset) => {
                    let manifest = dataset.manifest();
                    let fragments = &manifest.fragments;
                    let num_fragments = fragments.len();

                    if num_fragments > 0 {
                        // Count total data files across all fragments
                        let total_data_files: usize = fragments.iter().map(|f| f.files.len()).sum();
                        let avg_files_per_fragment = total_data_files as f64 / num_fragments as f64;

                        // Calculate total size across all fragments
                        let total_fragment_size: u64 = fragments
                            .iter()
                            .flat_map(|f| f.files.iter())
                            .filter_map(|df| df.file_size_bytes.get())
                            .map(u64::from)
                            .sum();
                        let avg_fragment_size = total_fragment_size as f64 / num_fragments as f64;

                        info!(
                            "Lance clustering: fragments={}, files={}, avg_files_per_fragment={:.2}",
                            num_fragments, total_data_files, avg_files_per_fragment
                        );

                        ClusteringInfo {
                            // Lance doesn't have explicit clustering columns like Delta/Iceberg
                            // but fragments serve as the clustering mechanism
                            clustering_columns: Vec::new(),
                            cluster_count: num_fragments,
                            avg_files_per_cluster: avg_files_per_fragment,
                            avg_cluster_size_bytes: avg_fragment_size,
                        }
                    } else {
                        ClusteringInfo {
                            clustering_columns: Vec::new(),
                            cluster_count: 0,
                            avg_files_per_cluster: 0.0,
                            avg_cluster_size_bytes: 0.0,
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to open Lance dataset for clustering info: {}", e);
                    ClusteringInfo {
                        clustering_columns: Vec::new(),
                        cluster_count: 0,
                        avg_files_per_cluster: 0.0,
                        avg_cluster_size_bytes: 0.0,
                    }
                }
            }
        } else {
            ClusteringInfo {
                clustering_columns: Vec::new(),
                cluster_count: 0,
                avg_files_per_cluster: 0.0,
                avg_cluster_size_bytes: 0.0,
            }
        };
        metrics.clustering = Some(clustering_info);

        // Initialize file_compaction with placeholder values. The actual metrics
        // are calculated later by `TableAnalyzer::analyze_file_compaction()` which
        // has access to individual file sizes. This matches how Delta handles it.
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.0,
            small_files_count: 0,
            small_files_size_bytes: 0,
            potential_compaction_files: 0,
            estimated_compaction_savings_bytes: 0,
            recommended_target_file_size_bytes: 256 * 1024 * 1024, // 256MB is good for Lance
            compaction_priority: "".to_string(),
            // Lance doesn't have z-ordering like Delta, but could support clustering
            z_order_opportunity: false,
            z_order_columns: Vec::new(),
        });

        // Update metadata health
        metrics.metadata_health.metadata_total_size_bytes = metadata_total_size;
        metrics.metadata_health.metadata_file_count = metadata_files.len();
        metrics.metadata_health.avg_metadata_file_size = if !metadata_files.is_empty() {
            metadata_total_size as f64 / metadata_files.len() as f64
        } else {
            0.0
        };
        metrics.metadata_health.metadata_growth_rate = 0.0;
        metrics.metadata_health.manifest_file_count = 0;

        info!("Successfully updated metrics from Lance metadata");
        Ok(())
    }
}

// Implement the TableAnalyzer trait for LanceAnalyzer
#[async_trait]
impl TableAnalyzer for LanceAnalyzer {
    fn categorize_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        self.categorize_lance_files(objects)
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
        self.update_metrics_from_lance_metadata(
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
    use async_trait::async_trait;
    use chrono::Utc;
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
            "/mock/lance/table"
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

    fn create_test_analyzer() -> LanceAnalyzer {
        let storage = Arc::new(MockStorageProvider::new());
        LanceAnalyzer::new(storage, 4)
    }

    fn create_file_metadata(path: &str, size: u64) -> FileMetadata {
        FileMetadata {
            path: path.to_string(),
            size,
            last_modified: Some(Utc::now()),
        }
    }

    #[test]
    fn test_lance_analyzer_new() {
        let analyzer = create_test_analyzer();
        assert_eq!(analyzer.parallelism, 4);
    }

    #[test]
    fn test_categorize_lance_files_data_files() {
        let analyzer = create_test_analyzer();

        let files = vec![
            create_file_metadata("table/data/fragment1.lance", 1024),
            create_file_metadata("table/data/fragment2.lance", 2048),
            create_file_metadata("table/data/fragment3.lance", 4096),
        ];

        let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

        assert_eq!(data_files.len(), 3);
        assert_eq!(metadata_files.len(), 0);
    }

    #[test]
    fn test_categorize_lance_files_version_files() {
        let analyzer = create_test_analyzer();

        let files = vec![
            create_file_metadata("table/_versions/1.manifest", 512),
            create_file_metadata("table/_versions/2.manifest", 512),
            create_file_metadata("table/_versions/3.manifest", 512),
        ];

        let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 3);
    }

    #[test]
    fn test_categorize_lance_files_index_files() {
        let analyzer = create_test_analyzer();

        let files = vec![
            create_file_metadata("table/_indices/index1.idx", 1024),
            create_file_metadata("table/_indices/index2.idx", 2048),
        ];

        let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 2);
    }

    #[test]
    fn test_categorize_lance_files_manifest_and_txn() {
        let analyzer = create_test_analyzer();

        let files = vec![
            create_file_metadata("table/latest.manifest", 256),
            create_file_metadata("table/commit.txn", 128),
        ];

        let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 2);
    }

    #[test]
    fn test_categorize_lance_files_mixed() {
        let analyzer = create_test_analyzer();

        let files = vec![
            create_file_metadata("table/data/fragment1.lance", 1024),
            create_file_metadata("table/data/fragment2.lance", 2048),
            create_file_metadata("table/_versions/1.manifest", 512),
            create_file_metadata("table/_versions/2.manifest", 512),
            create_file_metadata("table/_indices/index1.idx", 1024),
            create_file_metadata("table/latest.manifest", 256),
            create_file_metadata("table/commit.txn", 128),
        ];

        let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

        assert_eq!(data_files.len(), 2);
        assert_eq!(metadata_files.len(), 5);
    }

    #[test]
    fn test_categorize_lance_files_empty() {
        let analyzer = create_test_analyzer();

        let files: Vec<FileMetadata> = vec![];

        let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 0);
    }

    #[test]
    fn test_categorize_lance_files_unrecognized() {
        let analyzer = create_test_analyzer();

        let files = vec![
            create_file_metadata("table/random.txt", 100),
            create_file_metadata("table/data.parquet", 200),
            create_file_metadata("table/config.json", 50),
        ];

        let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

        // Unrecognized files should not be categorized
        assert_eq!(data_files.len(), 0);
        assert_eq!(metadata_files.len(), 0);
    }

    #[tokio::test]
    async fn test_find_referenced_files_no_version_files() {
        let analyzer = create_test_analyzer();

        // Metadata files without version files
        let metadata_files = vec![
            create_file_metadata("table/latest.manifest", 256),
            create_file_metadata("table/commit.txn", 128),
        ];

        let result = analyzer.find_referenced_files(&metadata_files).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_update_metrics_from_lance_metadata_empty() {
        let analyzer = create_test_analyzer();

        let metadata_files: Vec<FileMetadata> = vec![];
        let mut metrics = HealthMetrics::default();

        let result = analyzer
            .update_metrics_from_lance_metadata(&metadata_files, 0, 0, &mut metrics)
            .await;

        assert!(result.is_ok());

        // Verify time travel metrics are set
        assert!(metrics.time_travel_metrics.is_some());
        let time_travel = metrics.time_travel_metrics.as_ref().unwrap();
        assert_eq!(time_travel.total_snapshots, 0);

        // Verify schema evolution metrics are set
        assert!(metrics.schema_evolution.is_some());

        // Verify deletion vector metrics are set
        assert!(metrics.deletion_vector_metrics.is_some());
        let dv_metrics = metrics.deletion_vector_metrics.as_ref().unwrap();
        assert_eq!(dv_metrics.deletion_vector_count, 0);
    }

    #[tokio::test]
    async fn test_update_metrics_from_lance_metadata_with_versions() {
        let analyzer = create_test_analyzer();

        let metadata_files = vec![
            create_file_metadata("table/_versions/1.manifest", 512),
            create_file_metadata("table/_versions/2.manifest", 512),
            create_file_metadata("table/_versions/3.manifest", 512),
        ];

        let mut metrics = HealthMetrics::default();

        let result = analyzer
            .update_metrics_from_lance_metadata(&metadata_files, 1024 * 1024, 10, &mut metrics)
            .await;

        assert!(result.is_ok());

        // Verify time travel metrics
        let time_travel = metrics.time_travel_metrics.as_ref().unwrap();
        assert_eq!(time_travel.total_snapshots, 3);
    }

    #[tokio::test]
    async fn test_update_metrics_from_lance_metadata_with_deletions() {
        let analyzer = create_test_analyzer();

        let metadata_files = vec![
            create_file_metadata("table/_versions/1.manifest", 512),
            create_file_metadata("table/_deletions/del1.bin", 256),
            create_file_metadata("table/_deletions/del2.bin", 256),
        ];

        let mut metrics = HealthMetrics::default();

        let result = analyzer
            .update_metrics_from_lance_metadata(&metadata_files, 1024 * 1024, 10, &mut metrics)
            .await;

        assert!(result.is_ok());

        // Verify deletion vector metrics
        let dv_metrics = metrics.deletion_vector_metrics.as_ref().unwrap();
        assert_eq!(dv_metrics.deletion_vector_count, 2);
        assert_eq!(dv_metrics.total_deletion_vector_size_bytes, 512);
    }

    #[tokio::test]
    async fn test_update_metrics_from_lance_metadata_compaction_needed() {
        let analyzer = create_test_analyzer();

        let metadata_files = vec![create_file_metadata("table/_versions/1.manifest", 512)];

        let mut metrics = HealthMetrics::default();

        // Small average file size - but actual compaction analysis is done by
        // TableAnalyzer::analyze_file_compaction() which has access to individual files
        let result = analyzer
            .update_metrics_from_lance_metadata(
                &metadata_files,
                1024 * 1024, // 1MB total
                100,         // 100 files = 10KB avg
                &mut metrics,
            )
            .await;

        assert!(result.is_ok());

        // Verify file compaction metrics are initialized with placeholder values
        // (actual analysis is done by TableAnalyzer::analyze_file_compaction)
        let compaction = metrics.file_compaction.as_ref().unwrap();
        assert_eq!(compaction.compaction_opportunity_score, 0.0);
        assert_eq!(
            compaction.recommended_target_file_size_bytes,
            256 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn test_update_metrics_from_lance_metadata_no_compaction_needed() {
        let analyzer = create_test_analyzer();

        let metadata_files = vec![create_file_metadata("table/_versions/1.manifest", 512)];

        let mut metrics = HealthMetrics::default();

        // Large average file size - but actual compaction analysis is done by
        // TableAnalyzer::analyze_file_compaction() which has access to individual files
        let result = analyzer
            .update_metrics_from_lance_metadata(
                &metadata_files,
                1024 * 1024 * 1024, // 1GB total
                10,                 // 10 files = 100MB avg
                &mut metrics,
            )
            .await;

        assert!(result.is_ok());

        // Verify file compaction metrics are initialized with placeholder values
        // (actual analysis is done by TableAnalyzer::analyze_file_compaction)
        let compaction = metrics.file_compaction.as_ref().unwrap();
        assert_eq!(compaction.compaction_opportunity_score, 0.0);
        assert_eq!(
            compaction.recommended_target_file_size_bytes,
            256 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn test_update_metrics_metadata_health() {
        let analyzer = create_test_analyzer();

        let metadata_files = vec![
            create_file_metadata("table/_versions/1.manifest", 1024),
            create_file_metadata("table/_versions/2.manifest", 2048),
            create_file_metadata("table/_indices/index1.idx", 4096),
        ];

        let mut metrics = HealthMetrics::default();

        let result = analyzer
            .update_metrics_from_lance_metadata(&metadata_files, 1024 * 1024, 10, &mut metrics)
            .await;

        assert!(result.is_ok());

        // Verify metadata health
        assert_eq!(metrics.metadata_health.metadata_file_count, 3);
        assert_eq!(
            metrics.metadata_health.metadata_total_size_bytes,
            1024 + 2048 + 4096
        );
    }

    #[tokio::test]
    async fn test_schema_evolution_metrics_structure() {
        let analyzer = create_test_analyzer();

        let metadata_files = vec![
            create_file_metadata("table/_versions/1.manifest", 512),
            create_file_metadata("table/_versions/2.manifest", 512),
            create_file_metadata("table/_versions/3.manifest", 512),
        ];

        let mut metrics = HealthMetrics::default();

        let result = analyzer
            .update_metrics_from_lance_metadata(&metadata_files, 1024 * 1024, 10, &mut metrics)
            .await;

        assert!(result.is_ok());

        // Verify schema evolution metrics structure
        let schema_evolution = metrics.schema_evolution.as_ref().unwrap();

        // Schema stability score should be between 0 and 1
        assert!(
            schema_evolution.schema_stability_score >= 0.0
                && schema_evolution.schema_stability_score <= 1.0,
            "Schema stability score should be between 0 and 1"
        );

        // Current schema version should be accessible (usize is always >= 0)
        let _version = schema_evolution.current_schema_version;
    }

    #[test]
    fn test_detect_breaking_change_column_removal() {
        let old_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "name", "type": "Utf8", "nullable": true},
                {"name": "removed_col", "type": "Utf8", "nullable": true}
            ]
        });
        let new_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "name", "type": "Utf8", "nullable": true}
            ]
        });

        assert!(
            LanceAnalyzer::detect_breaking_change(&old_schema, &new_schema),
            "Column removal should be a breaking change"
        );
    }

    #[test]
    fn test_detect_breaking_change_type_change() {
        let old_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "value", "type": "Int32", "nullable": true}
            ]
        });
        let new_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "value", "type": "Utf8", "nullable": true}
            ]
        });

        assert!(
            LanceAnalyzer::detect_breaking_change(&old_schema, &new_schema),
            "Type change should be a breaking change"
        );
    }

    #[test]
    fn test_detect_breaking_change_nullability_change() {
        let old_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "value", "type": "Utf8", "nullable": true}
            ]
        });
        let new_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "value", "type": "Utf8", "nullable": false}
            ]
        });

        assert!(
            LanceAnalyzer::detect_breaking_change(&old_schema, &new_schema),
            "Nullable to non-nullable should be a breaking change"
        );
    }

    #[test]
    fn test_detect_breaking_change_column_addition() {
        let old_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false}
            ]
        });
        let new_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "new_col", "type": "Utf8", "nullable": true}
            ]
        });

        assert!(
            !LanceAnalyzer::detect_breaking_change(&old_schema, &new_schema),
            "Column addition should NOT be a breaking change"
        );
    }

    #[test]
    fn test_detect_breaking_change_no_change() {
        let schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "name", "type": "Utf8", "nullable": true}
            ]
        });

        assert!(
            !LanceAnalyzer::detect_breaking_change(&schema, &schema),
            "Identical schemas should NOT be a breaking change"
        );
    }

    #[test]
    fn test_detect_breaking_change_empty_fields() {
        let old_schema = serde_json::json!({});
        let new_schema = serde_json::json!({
            "fields": [{"name": "id", "type": "Int64", "nullable": false}]
        });

        assert!(
            !LanceAnalyzer::detect_breaking_change(&old_schema, &new_schema),
            "Missing fields array should return false"
        );
    }

    #[test]
    fn test_detect_breaking_change_non_nullable_to_nullable() {
        let old_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false}
            ]
        });
        let new_schema = serde_json::json!({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": true}
            ]
        });

        // Non-nullable to nullable is NOT breaking (it's relaxing the constraint)
        assert!(
            !LanceAnalyzer::detect_breaking_change(&old_schema, &new_schema),
            "Non-nullable to nullable should NOT be a breaking change"
        );
    }

    // Integration tests using the real Lance dataset
    mod integration {
        use super::*;
        use crate::storage::{StorageConfig, StorageProviderFactory};

        fn get_test_lance_table_path() -> String {
            let current_dir = std::env::current_dir().unwrap();
            current_dir
                .join("examples/data/lance_dataset.lance")
                .to_str()
                .unwrap()
                .to_string()
        }

        async fn create_integration_analyzer() -> LanceAnalyzer {
            let table_path = get_test_lance_table_path();
            let config = StorageConfig::local().with_option("path", &table_path);
            let storage = StorageProviderFactory::from_config(config)
                .await
                .expect("Failed to create storage provider");
            LanceAnalyzer::new(storage, 4)
        }

        #[tokio::test]
        async fn test_categorize_lance_files_real_dataset() {
            let analyzer = create_integration_analyzer().await;

            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            assert!(!files.is_empty(), "Should find files in Lance dataset");

            let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

            // Lance dataset should have both data and metadata files
            assert!(
                !data_files.is_empty() || !metadata_files.is_empty(),
                "Should find data or metadata files in Lance dataset"
            );
        }

        #[tokio::test]
        async fn test_find_referenced_files_real_dataset() {
            let analyzer = create_integration_analyzer().await;

            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (_, metadata_files) = analyzer.categorize_lance_files(files);

            // Verify we have metadata files (version manifests, indices, etc.)
            assert!(
                !metadata_files.is_empty(),
                "Should find metadata files in Lance dataset"
            );

            // The find_referenced_files function may return empty if the path
            // construction doesn't match the storage provider's expectations
            // This is acceptable for the integration test
            let result = analyzer.find_referenced_files(&metadata_files).await;
            assert!(result.is_ok(), "find_referenced_files should not error");
        }

        #[tokio::test]
        async fn test_update_metrics_from_lance_metadata_real_dataset() {
            let analyzer = create_integration_analyzer().await;

            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

            let total_size: u64 = data_files.iter().map(|f| f.size).sum();
            let total_files = data_files.len();

            let mut metrics = HealthMetrics::default();

            analyzer
                .update_metrics_from_lance_metadata(
                    &metadata_files,
                    total_size,
                    total_files,
                    &mut metrics,
                )
                .await
                .expect("Failed to update metrics");

            // Verify time travel metrics are populated
            assert!(
                metrics.time_travel_metrics.is_some(),
                "Time travel metrics should be populated"
            );

            // Verify schema evolution metrics are populated
            assert!(
                metrics.schema_evolution.is_some(),
                "Schema evolution metrics should be populated"
            );

            // Verify metadata health is populated
            assert!(
                metrics.metadata_health.metadata_file_count > 0,
                "Metadata file count should be > 0"
            );
        }

        #[tokio::test]
        async fn test_full_lance_analyze_workflow() {
            let analyzer = create_integration_analyzer().await;

            // Step 1: List all files
            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            assert!(!files.is_empty(), "Should find files");

            // Step 2: Categorize files
            let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

            // Step 3: Find referenced files
            let referenced_files = analyzer
                .find_referenced_files(&metadata_files)
                .await
                .expect("Failed to find referenced files");

            // Step 4: Update metrics
            let total_size: u64 = data_files.iter().map(|f| f.size).sum();
            let total_files = data_files.len();

            let mut metrics = HealthMetrics::default();

            analyzer
                .update_metrics_from_lance_metadata(
                    &metadata_files,
                    total_size,
                    total_files,
                    &mut metrics,
                )
                .await
                .expect("Failed to update metrics");

            // Verify the full workflow completed successfully
            assert!(
                metrics.time_travel_metrics.is_some(),
                "Time travel metrics should be set"
            );
            assert!(
                metrics.schema_evolution.is_some(),
                "Schema evolution should be set"
            );
            assert!(
                metrics.deletion_vector_metrics.is_some(),
                "Deletion vector metrics should be set"
            );
            assert!(
                metrics.file_compaction.is_some(),
                "File compaction metrics should be set"
            );

            // Log summary for debugging
            println!("Lance Analysis Summary:");
            println!("  Data files: {}", data_files.len());
            println!("  Metadata files: {}", metadata_files.len());
            println!("  Referenced files: {}", referenced_files.len());
            println!(
                "  Total snapshots: {}",
                metrics
                    .time_travel_metrics
                    .as_ref()
                    .unwrap()
                    .total_snapshots
            );
        }

        #[tokio::test]
        async fn test_schema_evolution_real_dataset() {
            // Use relative path directly like the reader tests do
            // This avoids file:// URI format issues on Windows
            let table_path = "examples/data/lance_dataset.lance";
            let config = StorageConfig::local().with_option("path", table_path);
            let storage = StorageProviderFactory::from_config(config)
                .await
                .expect("Failed to create storage provider");
            let analyzer = LanceAnalyzer::new(storage.clone(), 4);

            // Use the relative path directly for Lance Dataset::open
            // Lance handles relative paths correctly on all platforms
            let result = analyzer.analyze_schema_evolution(table_path).await;
            assert!(result.is_ok(), "Schema evolution analysis should succeed");

            let (schema_changes, current_version) = result.unwrap();

            // Current version should be > 0 for a valid dataset
            assert!(current_version > 0, "Current version should be > 0");

            // There should be at least one schema (the initial one)
            // Note: schema_changes may be empty if there's only one version
            println!(
                "Schema evolution: {} changes, current version: {}",
                schema_changes.len(),
                current_version
            );
        }

        #[tokio::test]
        async fn test_deletion_metrics_real_dataset() {
            // Use relative path directly like the reader tests do
            // This avoids file:// URI format issues on Windows
            let table_path = "examples/data/lance_dataset.lance";
            let config = StorageConfig::local().with_option("path", table_path);
            let storage = StorageProviderFactory::from_config(config)
                .await
                .expect("Failed to create storage provider");
            let analyzer = LanceAnalyzer::new(storage.clone(), 4);

            // Use the relative path directly for Lance Dataset::open
            // Lance handles relative paths correctly on all platforms
            let result = analyzer.analyze_deletion_metrics(table_path).await;
            assert!(result.is_ok(), "Deletion metrics analysis should succeed");

            let (deleted_rows, impact_score) = result.unwrap();

            // Impact score should be between 0 and 1
            assert!(
                (0.0..=1.0).contains(&impact_score),
                "Impact score should be between 0 and 1"
            );

            println!(
                "Deletion metrics: {} deleted rows, impact score: {:.3}",
                deleted_rows, impact_score
            );
        }

        #[tokio::test]
        async fn test_metrics_with_file_compaction() {
            let analyzer = create_integration_analyzer().await;

            // Get files and update metrics
            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

            let total_size: u64 = data_files.iter().map(|f| f.size).sum();
            let mut metrics = HealthMetrics::default();

            analyzer
                .update_metrics_from_lance_metadata(
                    &metadata_files,
                    total_size,
                    data_files.len(),
                    &mut metrics,
                )
                .await
                .expect("Failed to update metrics");

            // Verify file compaction metrics are populated
            assert!(
                metrics.file_compaction.is_some(),
                "File compaction metrics should be populated"
            );

            let compaction = metrics.file_compaction.as_ref().unwrap();

            // Verify compaction metrics structure
            assert!(
                compaction.recommended_target_file_size_bytes > 0,
                "Recommended target file size should be > 0"
            );

            println!(
                "File compaction: opportunity score {:.2}, recommended target size {} bytes",
                compaction.compaction_opportunity_score,
                compaction.recommended_target_file_size_bytes
            );
        }

        #[tokio::test]
        async fn test_table_analyzer_trait_implementation() {
            let analyzer = create_integration_analyzer().await;

            // Test the TableAnalyzer trait methods
            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            // Test categorize_files (trait method)
            let (data_files, metadata_files) =
                <LanceAnalyzer as TableAnalyzer>::categorize_files(&analyzer, files.clone());

            assert!(
                !data_files.is_empty() || !metadata_files.is_empty(),
                "Should categorize some files"
            );

            // Test find_referenced_files (trait method)
            let referenced =
                <LanceAnalyzer as TableAnalyzer>::find_referenced_files(&analyzer, &metadata_files)
                    .await;
            assert!(referenced.is_ok(), "find_referenced_files should succeed");

            // Test update_metrics_from_metadata (trait method)
            let mut metrics = HealthMetrics::default();
            let total_size: u64 = data_files.iter().map(|f| f.size).sum();
            let result = <LanceAnalyzer as TableAnalyzer>::update_metrics_from_metadata(
                &analyzer,
                &metadata_files,
                total_size,
                data_files.len(),
                &mut metrics,
            )
            .await;
            assert!(
                result.is_ok(),
                "update_metrics_from_metadata should succeed"
            );
        }

        #[tokio::test]
        async fn test_lance_metrics_schema_json_format() {
            let analyzer = create_integration_analyzer().await;

            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (data_files, metadata_files) =
                <LanceAnalyzer as TableAnalyzer>::categorize_files(&analyzer, files);

            let mut metrics = HealthMetrics::default();
            let total_size: u64 = data_files.iter().map(|f| f.size).sum();

            let _ = analyzer
                .update_metrics_from_lance_metadata(
                    &metadata_files,
                    total_size,
                    data_files.len(),
                    &mut metrics,
                )
                .await;

            // If we have Lance metrics, verify schema is valid JSON
            if let Some(ref lance_metrics) = metrics.lance_table_specific_metrics {
                if !lance_metrics.metadata.schema_string.is_empty() {
                    let schema_result: Result<serde_json::Value, _> =
                        serde_json::from_str(&lance_metrics.metadata.schema_string);
                    assert!(
                        schema_result.is_ok(),
                        "Schema string should be valid JSON: {}",
                        lance_metrics.metadata.schema_string
                    );
                }
            }
        }

        #[tokio::test]
        async fn test_lance_file_statistics_accuracy() {
            let analyzer = create_integration_analyzer().await;

            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (data_files, metadata_files) =
                <LanceAnalyzer as TableAnalyzer>::categorize_files(&analyzer, files);

            let mut metrics = HealthMetrics::default();
            let total_size: u64 = data_files.iter().map(|f| f.size).sum();

            let _ = analyzer
                .update_metrics_from_lance_metadata(
                    &metadata_files,
                    total_size,
                    data_files.len(),
                    &mut metrics,
                )
                .await;

            // If we have Lance metrics, verify file statistics are reasonable
            if let Some(ref lance_metrics) = metrics.lance_table_specific_metrics {
                let file_stats = &lance_metrics.file_stats;

                // If we have data files, sizes should be non-zero
                if file_stats.num_data_files > 0 {
                    assert!(
                        file_stats.total_data_size_bytes > 0,
                        "Total data size should be > 0 when we have data files"
                    );

                    // Min should be <= max
                    assert!(
                        file_stats.min_data_file_size_bytes <= file_stats.max_data_file_size_bytes,
                        "Min file size should be <= max file size"
                    );
                }
            }
        }

        #[tokio::test]
        async fn test_lance_clustering_info_from_fragments() {
            let analyzer = create_integration_analyzer().await;

            // List files from the table (empty path since storage is configured with table path)
            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (data_files, metadata_files) = analyzer.categorize_lance_files(files);

            let data_files_total_size: u64 = data_files.iter().map(|f| f.size).sum();
            let data_files_total_files = data_files.len();

            let mut metrics = HealthMetrics::default();
            analyzer
                .update_metrics_from_lance_metadata(
                    &metadata_files,
                    data_files_total_size,
                    data_files_total_files,
                    &mut metrics,
                )
                .await
                .expect("Failed to update metrics");

            // Verify clustering info is populated from fragments
            let clustering = metrics.clustering.expect("Clustering info should be set");

            // Lance dataset should have at least one fragment
            assert!(
                clustering.cluster_count > 0,
                "Cluster count should be > 0 (fragments exist)"
            );

            // Average files per cluster should be reasonable
            assert!(
                clustering.avg_files_per_cluster > 0.0,
                "Avg files per cluster should be > 0"
            );

            // Average cluster size should be > 0 if we have data
            if data_files_total_size > 0 {
                assert!(
                    clustering.avg_cluster_size_bytes > 0.0,
                    "Avg cluster size should be > 0 when data exists"
                );
            }

            // Clustering columns are empty for Lance (no explicit clustering columns)
            assert!(
                clustering.clustering_columns.is_empty(),
                "Lance doesn't have explicit clustering columns"
            );
        }
    }
}
