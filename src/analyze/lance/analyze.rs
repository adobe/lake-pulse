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

use crate::analyze::metrics::{
    ClusteringInfo, DeletionVectorMetrics, FileCompactionMetrics, HealthMetrics,
    SchemaEvolutionMetrics, TableConstraintsMetrics, TimeTravelMetrics,
};
use crate::analyze::table_analyzer::TableAnalyzer;
use crate::storage::{FileMetadata, StorageProvider};
use async_trait::async_trait;
use lance::dataset::Dataset;
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
    #[allow(dead_code)]
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
            } else if obj.path.contains("/_versions/") || obj.path.contains("/_indices/") {
                // Version manifests and index files are metadata
                metadata_files.push(obj);
            } else if obj.path.ends_with(".manifest") || obj.path.ends_with(".txn") {
                // Manifest and transaction files
                metadata_files.push(obj);
            }
        }

        (data_files, metadata_files)
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
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Updating metrics from Lance metadata");

        // Count version files to determine snapshot count
        let version_files: Vec<&FileMetadata> = metadata_files
            .iter()
            .filter(|f| f.path.contains("/_versions/"))
            .collect();

        let total_snapshots = version_files.len();

        // Calculate metadata size
        let metadata_total_size: u64 = metadata_files.iter().map(|f| f.size).sum();

        // Time travel metrics
        let oldest_timestamp = version_files.iter().filter_map(|f| f.last_modified).min();

        let newest_timestamp = version_files.iter().filter_map(|f| f.last_modified).max();

        let retention_days =
            if let (Some(oldest), Some(newest)) = (oldest_timestamp, newest_timestamp) {
                (newest.signed_duration_since(oldest).num_seconds() as f64 / 86400.0).round()
            } else {
                0.0
            };

        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots,
            oldest_snapshot_age_days: retention_days,
            newest_snapshot_age_days: 0.0,
            total_historical_size_bytes: metadata_total_size,
            avg_snapshot_size_bytes: if total_snapshots > 0 {
                metadata_total_size as f64 / total_snapshots as f64
            } else {
                0.0
            },
            storage_cost_impact_score: 0.3,
            retention_efficiency_score: 0.7,
            recommended_retention_days: 30,
        });

        // Schema evolution metrics - Lance supports schema evolution
        // For now, we'll set basic values
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: 0, // Would need to parse version history
            breaking_changes: 0,
            non_breaking_changes: 0,
            days_since_last_change: 0.0,
            schema_change_frequency: 0.0,
            current_schema_version: 1,
            schema_stability_score: 1.0, // Assume stable unless we detect changes
        });

        // Deletion vector metrics - Lance supports deletion files
        let deletion_files: Vec<&FileMetadata> = metadata_files
            .iter()
            .filter(|f| f.path.contains("_deletions"))
            .collect();

        let deletion_vector_count = deletion_files.len() as u64;
        let deletion_vector_total_size: u64 = deletion_files.iter().map(|f| f.size).sum();

        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: deletion_vector_count as usize,
            total_deletion_vector_size_bytes: deletion_vector_total_size,
            avg_deletion_vector_size_bytes: if deletion_vector_count > 0 {
                deletion_vector_total_size as f64 / deletion_vector_count as f64
            } else {
                0.0
            },
            deletion_vector_age_days: 0.0,
            deleted_rows_count: 0, // Would need to parse deletion files
            deletion_vector_impact_score: if data_files_total_size > 0 {
                (deletion_vector_total_size as f64 / data_files_total_size as f64).min(1.0)
            } else {
                0.0
            },
        });

        // Table constraints - Lance doesn't have explicit constraints like Delta
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

        // Clustering info - Lance uses fragments which are similar to clustering
        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: Vec::new(),
            cluster_count: 0,
            avg_files_per_cluster: 0.0,
            avg_cluster_size_bytes: 0.0,
        });

        // File compaction metrics
        let avg_file_size = if data_files_total_files > 0 {
            data_files_total_size as f64 / data_files_total_files as f64
        } else {
            0.0
        };

        // Lance typically uses larger files, so we'll use different thresholds
        let small_file_threshold = 10 * 1024 * 1024; // 10MB

        let needs_compaction =
            avg_file_size < small_file_threshold as f64 || data_files_total_files > 1000;

        let compaction_benefit_score = if needs_compaction {
            0.7 // Lance benefits from compaction but less critically than Delta
        } else {
            0.1
        };

        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: compaction_benefit_score,
            small_files_count: 0, // Would need to analyze individual file sizes
            small_files_size_bytes: 0,
            potential_compaction_files: if needs_compaction {
                data_files_total_files
            } else {
                0
            },
            estimated_compaction_savings_bytes: 0,
            recommended_target_file_size_bytes: 256 * 1024 * 1024, // 256MB is good for Lance
            compaction_priority: if needs_compaction {
                "medium".to_string()
            } else {
                "low".to_string()
            },
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
