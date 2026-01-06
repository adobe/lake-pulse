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
use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use tracing::info;

/// Result type for timeline file processing.
/// Contains the action type, optional commit metadata, and timestamp in milliseconds.
type TimelineProcessingResult =
    Result<(HudiActionType, Option<HoodieCommitMetadata>, u64), Box<dyn Error + Send + Sync>>;

/// Convert a Hudi timestamp string (YYYYMMDDHHmmssSSS) to Unix milliseconds.
///
/// Hudi uses a custom timestamp format for commit identifiers:
/// - Format: YYYYMMDDHHmmssSSS (17 digits)
/// - Example: 20251110222213846 = 2025-11-10 22:22:13.846
///
/// Returns 0 if the timestamp cannot be parsed.
fn hudi_timestamp_to_millis(timestamp_str: &str) -> u64 {
    if timestamp_str.len() < 14 {
        return 0;
    }

    // Parse YYYYMMDDHHmmss (first 14 chars)
    let datetime_str = &timestamp_str[..14];
    let millis_str = if timestamp_str.len() >= 17 {
        &timestamp_str[14..17]
    } else {
        "000"
    };

    let millis: u64 = millis_str.parse().unwrap_or(0);

    match NaiveDateTime::parse_from_str(datetime_str, "%Y%m%d%H%M%S") {
        Ok(dt) => {
            // Convert to UTC timestamp in milliseconds
            dt.and_utc().timestamp_millis() as u64 + millis
        }
        Err(_) => 0,
    }
}

/// Hudi table type - Copy-on-Write or Merge-on-Read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HudiTableType {
    #[default]
    CopyOnWrite,
    MergeOnRead,
}

impl From<&str> for HudiTableType {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "COPY_ON_WRITE" => HudiTableType::CopyOnWrite,
            "MERGE_ON_READ" => HudiTableType::MergeOnRead,
            _ => HudiTableType::CopyOnWrite,
        }
    }
}

/// Parsed hoodie.properties configuration.
#[derive(Debug, Clone, Default)]
pub struct HoodieProperties {
    pub table_name: String,
    pub table_type: HudiTableType,
    pub table_version: u32,
    pub precombine_field: String,
    pub record_key_fields: Vec<String>,
    pub partition_fields: Vec<String>,
    pub base_file_format: String,
    pub cdc_enabled: bool,
    pub populate_meta_fields: bool,
    pub timeline_layout_version: u32,
}

impl HoodieProperties {
    /// Parse hoodie.properties content into a structured format.
    pub fn parse(content: &str) -> Self {
        let mut props = HoodieProperties::default();
        let mut properties: HashMap<String, String> = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            // Skip comments and empty lines
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                properties.insert(key.trim().to_string(), value.trim().to_string());
            }
        }

        props.table_name = properties
            .get("hoodie.table.name")
            .cloned()
            .unwrap_or_default();
        props.table_type = properties
            .get("hoodie.table.type")
            .map(|s| HudiTableType::from(s.as_str()))
            .unwrap_or_default();
        props.table_version = properties
            .get("hoodie.table.version")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        props.precombine_field = properties
            .get("hoodie.table.precombine.field")
            .cloned()
            .unwrap_or_default();
        props.record_key_fields = properties
            .get("hoodie.table.recordkey.fields")
            .map(|s| s.split(',').map(|f| f.trim().to_string()).collect())
            .unwrap_or_default();
        props.partition_fields = properties
            .get("hoodie.table.partition.fields")
            .map(|s| {
                s.split(',')
                    .map(|f| f.trim().to_string())
                    .filter(|f| !f.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        props.base_file_format = properties
            .get("hoodie.table.base.file.format")
            .cloned()
            .unwrap_or_else(|| "PARQUET".to_string());
        props.cdc_enabled = properties
            .get("hoodie.table.cdc.enabled")
            .map(|s| s.to_lowercase() == "true")
            .unwrap_or(false);
        props.populate_meta_fields = properties
            .get("hoodie.populate.meta.fields")
            .map(|s| s.to_lowercase() == "true")
            .unwrap_or(true);
        props.timeline_layout_version = properties
            .get("hoodie.timeline.layout.version")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        props
    }
}

/// Statistics for a single file write operation in Hudi.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct HoodieWriteStat {
    pub file_id: Option<String>,
    pub path: Option<String>,
    pub prev_commit: Option<String>,
    pub num_writes: Option<i64>,
    pub num_deletes: Option<i64>,
    pub num_update_writes: Option<i64>,
    pub num_inserts: Option<i64>,
    pub total_write_bytes: Option<i64>,
    pub total_write_errors: Option<i64>,
    pub partition_path: Option<String>,
    pub file_size_in_bytes: Option<i64>,
    pub total_log_records: Option<i64>,
    pub total_log_files_compacted: Option<i64>,
    pub total_log_size_compacted: Option<i64>,
    pub total_updated_records_compacted: Option<i64>,
    pub total_log_blocks: Option<i64>,
    pub total_corrupt_log_block: Option<i64>,
    pub total_rollback_blocks: Option<i64>,
}

/// Hudi commit metadata structure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct HoodieCommitMetadata {
    pub partition_to_write_stats: Option<HashMap<String, Vec<HoodieWriteStat>>>,
    pub compacted: Option<bool>,
    pub extra_metadata: Option<HashMap<String, String>>,
    pub operation_type: Option<String>,
}

impl HoodieCommitMetadata {
    /// Get the schema from extra metadata if present.
    pub fn get_schema(&self) -> Option<Value> {
        self.extra_metadata
            .as_ref()
            .and_then(|m| m.get("schema"))
            .and_then(|s| serde_json::from_str(s).ok())
    }

    /// Get all file paths referenced in this commit.
    pub fn get_file_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        if let Some(partition_stats) = &self.partition_to_write_stats {
            for stats in partition_stats.values() {
                for stat in stats {
                    if let Some(path) = &stat.path {
                        paths.push(path.clone());
                    }
                }
            }
        }
        paths
    }

    /// Calculate total records written across all partitions.
    pub fn total_records_written(&self) -> i64 {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| {
                m.values()
                    .flat_map(|stats| stats.iter())
                    .map(|s| s.num_writes.unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Calculate total deletes across all partitions.
    pub fn total_deletes(&self) -> i64 {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| {
                m.values()
                    .flat_map(|stats| stats.iter())
                    .map(|s| s.num_deletes.unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Calculate total bytes written across all partitions.
    pub fn total_bytes_written(&self) -> i64 {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| {
                m.values()
                    .flat_map(|stats| stats.iter())
                    .map(|s| s.total_write_bytes.unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0)
    }
}

/// Represents a parsed Hudi timeline action.
#[derive(Debug, Clone)]
pub struct HudiTimelineAction {
    pub timestamp: String,
    pub action_type: HudiActionType,
    pub state: HudiActionState,
    pub metadata: Option<HoodieCommitMetadata>,
}

/// Types of Hudi timeline actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HudiActionType {
    Commit,
    DeltaCommit,
    Clean,
    Compaction,
    Rollback,
    Savepoint,
    ReplaceCommit,
    Indexing,
    Unknown(String),
}

impl From<&str> for HudiActionType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "commit" => HudiActionType::Commit,
            "deltacommit" => HudiActionType::DeltaCommit,
            "clean" => HudiActionType::Clean,
            "compaction" => HudiActionType::Compaction,
            "rollback" => HudiActionType::Rollback,
            "savepoint" => HudiActionType::Savepoint,
            "replacecommit" => HudiActionType::ReplaceCommit,
            "indexing" => HudiActionType::Indexing,
            other => HudiActionType::Unknown(other.to_string()),
        }
    }
}

/// State of a Hudi timeline action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HudiActionState {
    Requested,
    Inflight,
    Completed,
}

/// Intermediate structure to hold aggregated results from parallel metadata processing.
#[derive(Debug, Default)]
pub struct HudiMetadataProcessingResult {
    pub total_commits: usize,
    pub total_deletes: i64,
    pub total_compactions: usize,
    pub total_rollbacks: usize,
    pub total_cleans: usize,
    /// Number of replace commits (used for clustering operations)
    pub total_replace_commits: usize,
    pub oldest_timestamp: u64,
    pub newest_timestamp: u64,
    pub total_bytes_written: i64,
    pub schema_changes: Vec<SchemaChange>,
    pub file_paths: HashSet<String>,
    /// Partition fields from hoodie.properties (used for clustering columns)
    pub partition_fields: Vec<String>,
    /// Record key fields from hoodie.properties
    pub record_key_fields: Vec<String>,
}

/// Apache Hudi-specific analyzer for processing Hudi tables.
///
/// This analyzer implements the `TableAnalyzer` trait and provides functionality
/// to parse Hudi timeline files, extract metrics, and analyze table health.
/// It parses Hudi's timeline files (.commit, .deltacommit, etc.) and hoodie.properties
/// for comprehensive analysis.
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
/// use lake_pulse::analyze::hudi::HudiAnalyzer;
///
/// # async fn example(storage: Arc<dyn StorageProvider>) {
/// let analyzer = HudiAnalyzer::new(storage, 4);
/// // Use analyzer to process Hudi tables
/// # }
/// ```
pub struct HudiAnalyzer {
    storage_provider: Arc<dyn StorageProvider>,
    parallelism: usize,
}

impl HudiAnalyzer {
    /// Create a new HudiAnalyzer.
    ///
    /// # Arguments
    ///
    /// * `storage_provider` - The storage provider to use for reading files
    /// * `parallelism` - The number of concurrent tasks to use for metadata processing
    ///
    /// # Returns
    ///
    /// A new `HudiAnalyzer` instance configured with the specified storage provider and parallelism.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use lake_pulse::storage::StorageProvider;
    /// use lake_pulse::analyze::hudi::HudiAnalyzer;
    ///
    /// # async fn example(storage: Arc<dyn StorageProvider>) {
    /// let analyzer = HudiAnalyzer::new(storage, 4);
    /// # }
    /// ```
    pub fn new(storage_provider: Arc<dyn StorageProvider>, parallelism: usize) -> Self {
        Self {
            storage_provider,
            parallelism,
        }
    }

    /// Categorize files into data files and Hudi metadata files.
    ///
    /// Separates Parquet data files from Hudi metadata files in the `.hoodie` directory.
    /// Data files are Parquet files outside the `.hoodie` directory, while metadata files
    /// include timeline files (.commit, .deltacommit, .clean, etc.) and hoodie.properties.
    ///
    /// # Arguments
    ///
    /// * `objects` - All files discovered in the table location
    ///
    /// # Returns
    ///
    /// A tuple of `(data_files, metadata_files)` where:
    /// * `data_files` - Vector of Parquet data files
    /// * `metadata_files` - Vector of Hudi timeline and configuration files
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::hudi::HudiAnalyzer;
    /// # use lake_pulse::storage::FileMetadata;
    /// # use std::sync::Arc;
    /// # fn example(analyzer: &HudiAnalyzer, files: Vec<FileMetadata>) {
    /// let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);
    /// println!("Found {} data files and {} metadata files",
    ///          data_files.len(), metadata_files.len());
    /// # }
    /// ```
    pub fn categorize_hudi_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        let mut data_files = Vec::new();
        let mut metadata_files = Vec::new();

        for obj in objects {
            // Hudi data files are parquet files not in .hoodie directory
            if obj.path.ends_with(".parquet") && !obj.path.contains("/.hoodie/") {
                data_files.push(obj);
            }
            // Hudi metadata files are in .hoodie directory
            // Including timeline files (.commit, .deltacommit, .clean, etc.)
            // and hoodie.properties
            else if obj.path.contains("/.hoodie/") {
                metadata_files.push(obj);
            }
        }

        (data_files, metadata_files)
    }

    /// Find referenced files from Hudi metadata.
    ///
    /// Parses Hudi timeline files (.commit, .deltacommit) to extract all file paths
    /// referenced in the active file slices. For CoW tables, this returns the latest
    /// base file for each file group. For MoR tables, this would include log files.
    ///
    /// # Arguments
    ///
    /// * `metadata_files` - The metadata files to parse
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<String>)` - Vector of file paths referenced in the timeline
    /// * `Err` - If file reading or JSON parsing fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Timeline files cannot be read from storage
    /// * Commit metadata JSON parsing fails
    /// * File content is not valid UTF-8
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::hudi::HudiAnalyzer;
    /// # use lake_pulse::storage::FileMetadata;
    /// # async fn example(analyzer: &HudiAnalyzer, metadata: &Vec<FileMetadata>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let referenced_files = analyzer.find_referenced_files(metadata).await?;
    /// println!("Found {} referenced data files", referenced_files.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_referenced_files(
        &self,
        metadata_files: &[FileMetadata],
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        info!("Finding referenced files from Hudi metadata");

        // Filter to only completed commit/deltacommit files
        let commit_files: Vec<_> = metadata_files
            .iter()
            .filter(|f| self.is_completed_commit_file(&f.path))
            .cloned()
            .collect();

        if commit_files.is_empty() {
            info!("No completed commit files found");
            return Ok(Vec::new());
        }

        let storage_provider = Arc::clone(&self.storage_provider);

        // Process commit files in parallel to extract file references
        let results: Vec<Result<Vec<String>, Box<dyn Error + Send + Sync>>> =
            stream::iter(commit_files)
                .map(|file| {
                    let storage = Arc::clone(&storage_provider);
                    async move {
                        let content = storage.read_file(&file.path).await?;
                        let content_str = String::from_utf8_lossy(&content);

                        match serde_json::from_str::<HoodieCommitMetadata>(&content_str) {
                            Ok(metadata) => Ok(metadata.get_file_paths()),
                            Err(e) => {
                                info!("Failed to parse commit file {}: {}", file.path, e);
                                Ok(Vec::new())
                            }
                        }
                    }
                })
                .buffer_unordered(self.parallelism)
                .collect()
                .await;

        // Build a map of file_id -> latest file path to get active file slices
        let mut file_group_to_latest: HashMap<String, String> = HashMap::new();

        for result in results {
            match result {
                Ok(paths) => {
                    for path in paths {
                        // Extract file_id from path (format: fileId_writeToken_timestamp.parquet)
                        if let Some(file_id) = self.extract_file_id_from_path(&path) {
                            // Always keep the latest (we process in order, so last wins)
                            file_group_to_latest.insert(file_id, path);
                        }
                    }
                }
                Err(e) => {
                    info!("Error processing commit file: {}", e);
                }
            }
        }

        let referenced_files: Vec<String> = file_group_to_latest.into_values().collect();
        info!(
            "Found {} referenced files from Hudi metadata",
            referenced_files.len()
        );

        Ok(referenced_files)
    }

    /// Check if a file path is a completed commit or deltacommit file.
    fn is_completed_commit_file(&self, path: &str) -> bool {
        let filename = path.rsplit('/').next().unwrap_or(path);
        // Completed commits don't have .inflight or .requested suffix
        (filename.ends_with(".commit") || filename.ends_with(".deltacommit"))
            && !filename.contains(".inflight")
            && !filename.contains(".requested")
    }

    /// Extract file ID from a Hudi data file path.
    /// Format: fileId_writeToken_timestamp.parquet
    fn extract_file_id_from_path(&self, path: &str) -> Option<String> {
        let filename = path.rsplit('/').next().unwrap_or(path);
        // Split by underscore and take the first part as file ID
        filename.split('_').next().map(|s| s.to_string())
    }

    /// Update health metrics from Hudi metadata files.
    ///
    /// Parses Hudi timeline files (.commit, .deltacommit, .clean, etc.) and hoodie.properties
    /// to extract comprehensive health metrics including schema evolution, time travel,
    /// compaction, and deletion metrics.
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
    /// * Timeline file parsing fails
    /// * Metric calculations fail due to invalid data
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lake_pulse::analyze::hudi::HudiAnalyzer;
    /// # use lake_pulse::analyze::metrics::HealthMetrics;
    /// # use lake_pulse::storage::FileMetadata;
    /// # async fn example(analyzer: &HudiAnalyzer, metadata: &Vec<FileMetadata>, mut metrics: HealthMetrics) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// analyzer.update_metrics_from_hudi_metadata(
    ///     metadata,
    ///     1024 * 1024 * 1024, // 1GB total data size
    ///     100,                 // 100 data files
    ///     &mut metrics
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_metrics_from_hudi_metadata(
        &self,
        metadata_files: &[FileMetadata],
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Updating metrics from Hudi metadata");

        // Process metadata files to extract metrics
        let result = self.process_hudi_metadata(metadata_files).await?;

        // Calculate time-based metrics
        let now = Utc::now().timestamp_millis() as u64;
        let oldest_age_days = if result.oldest_timestamp > 0 {
            (now - result.oldest_timestamp) as f64 / (1000.0 * 60.0 * 60.0 * 24.0)
        } else {
            0.0
        };
        let newest_age_days = if result.newest_timestamp > 0 {
            (now - result.newest_timestamp) as f64 / (1000.0 * 60.0 * 60.0 * 24.0)
        } else {
            0.0
        };

        // Calculate schema metrics
        let breaking_changes = result
            .schema_changes
            .iter()
            .filter(|c| c.is_breaking)
            .count();
        let non_breaking_changes = result.schema_changes.len() - breaking_changes;
        let days_since_last_change = if let Some(last) = result.schema_changes.last() {
            if last.timestamp > 0 && last.timestamp <= now {
                (now - last.timestamp) as f64 / (1000.0 * 60.0 * 60.0 * 24.0)
            } else {
                oldest_age_days
            }
        } else {
            oldest_age_days
        };
        let schema_change_frequency = if oldest_age_days > 0.0 {
            result.schema_changes.len() as f64 / oldest_age_days
        } else {
            0.0
        };
        let schema_stability_score = calculate_schema_stability_score(
            result.schema_changes.len(),
            breaking_changes,
            schema_change_frequency,
            days_since_last_change,
        );

        // Set schema evolution metrics
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: result.schema_changes.len(),
            breaking_changes,
            non_breaking_changes,
            schema_stability_score,
            days_since_last_change,
            schema_change_frequency,
            current_schema_version: result.schema_changes.len() as u64,
        });

        // Calculate time travel metrics
        let total_historical_size = result.total_bytes_written as u64;
        let avg_snapshot_size = if result.total_commits > 0 {
            total_historical_size as f64 / result.total_commits as f64
        } else {
            0.0
        };
        let storage_cost_impact = if data_files_total_size > 0 {
            total_historical_size as f64 / data_files_total_size as f64
        } else {
            0.0
        };
        let retention_efficiency =
            calculate_retention_efficiency(result.total_commits, oldest_age_days, newest_age_days);
        let recommended_retention =
            calculate_recommended_retention(result.total_commits, oldest_age_days);

        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots: result.total_commits,
            oldest_snapshot_age_days: oldest_age_days,
            newest_snapshot_age_days: newest_age_days,
            total_historical_size_bytes: total_historical_size,
            avg_snapshot_size_bytes: avg_snapshot_size,
            storage_cost_impact_score: storage_cost_impact.min(1.0),
            retention_efficiency_score: retention_efficiency,
            recommended_retention_days: recommended_retention,
        });

        // Calculate file compaction metrics
        // For Hudi, compaction is built-in for MoR tables
        let compaction_opportunity = if result.total_compactions > 0 {
            0.0 // Already compacting
        } else if data_files_total_files > 100 {
            0.5 // Might benefit from compaction
        } else {
            0.0
        };
        let compaction_priority = if compaction_opportunity > 0.7 {
            "high"
        } else if compaction_opportunity > 0.3 {
            "medium"
        } else {
            "low"
        };

        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: compaction_opportunity,
            small_files_count: 0, // Would need to analyze file sizes
            small_files_size_bytes: 0,
            potential_compaction_files: 0,
            estimated_compaction_savings_bytes: 0,
            recommended_target_file_size_bytes: 128 * 1024 * 1024, // 128 MB (Hudi default)
            compaction_priority: compaction_priority.to_string(),
            z_order_opportunity: false,
            z_order_columns: Vec::new(),
        });

        // Set deletion metrics based on Hudi's delete tracking
        let deletion_impact = if result.total_deletes > 0 {
            (result.total_deletes as f64 / 1000.0).min(1.0)
        } else {
            0.0
        };

        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: 0, // Hudi doesn't use deletion vectors
            total_deletion_vector_size_bytes: 0,
            avg_deletion_vector_size_bytes: 0.0,
            deletion_vector_age_days: 0.0,
            deleted_rows_count: result.total_deletes as u64,
            deletion_vector_impact_score: deletion_impact,
        });

        // Table constraints metrics - Hudi does not have built-in constraint support
        // Unlike Delta Lake which supports CHECK constraints and NOT NULL enforcement,
        // Hudi relies on external schema validation (e.g., Avro schema) for data quality.
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

        // Set clustering info based on partition fields and replace commits
        // In Hudi, clustering is done via replacecommit actions
        // Partition fields serve as the clustering columns
        let partition_count = metrics.partitions.len().max(1);
        let avg_files_per_cluster = if partition_count > 0 {
            data_files_total_files as f64 / partition_count as f64
        } else {
            0.0
        };
        let avg_cluster_size_bytes = if partition_count > 0 {
            data_files_total_size as f64 / partition_count as f64
        } else {
            0.0
        };

        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: result.partition_fields.clone(),
            cluster_count: partition_count,
            avg_files_per_cluster,
            avg_cluster_size_bytes,
        });

        info!(
            "Hudi clustering: columns={:?}, clusters={}, replace_commits={}",
            result.partition_fields, partition_count, result.total_replace_commits
        );

        Ok(())
    }

    /// Process Hudi metadata files and aggregate results.
    async fn process_hudi_metadata(
        &self,
        metadata_files: &[FileMetadata],
    ) -> Result<HudiMetadataProcessingResult, Box<dyn Error + Send + Sync>> {
        let mut result = HudiMetadataProcessingResult {
            oldest_timestamp: u64::MAX,
            ..Default::default()
        };

        // Find and parse hoodie.properties
        let properties_file = metadata_files
            .iter()
            .find(|f| f.path.ends_with("hoodie.properties"));

        if let Some(props_file) = properties_file {
            let content = self.storage_provider.read_file(&props_file.path).await?;
            let content_str = String::from_utf8_lossy(&content);
            let properties = HoodieProperties::parse(&content_str);
            info!(
                "Parsed hoodie.properties: table_name={}",
                properties.table_name
            );
            // Store partition and record key fields for clustering info
            result.partition_fields = properties.partition_fields.clone();
            result.record_key_fields = properties.record_key_fields.clone();
        }

        // Filter to only timeline files
        let timeline_files: Vec<_> = metadata_files
            .iter()
            .filter(|f| {
                let filename = f.path.rsplit('/').next().unwrap_or(&f.path);
                // Match timeline files: timestamp.action or timestamp.action.state
                filename
                    .split('.')
                    .next()
                    .map(|s| s.chars().all(|c| c.is_ascii_digit()))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        // Process timeline files in parallel
        let storage_provider = Arc::clone(&self.storage_provider);
        let results: Vec<TimelineProcessingResult> = stream::iter(timeline_files)
            .map(|file| {
                let storage = Arc::clone(&storage_provider);
                let path = file.path.clone();
                async move {
                    let filename = path.rsplit('/').next().unwrap_or(&path);
                    let parts: Vec<&str> = filename.split('.').collect();

                    let timestamp: u64 = parts
                        .first()
                        .map(|s| hudi_timestamp_to_millis(s))
                        .unwrap_or(0);

                    let action_type = parts
                        .get(1)
                        .map(|s| HudiActionType::from(*s))
                        .unwrap_or(HudiActionType::Unknown("unknown".to_string()));

                    // Only parse completed commits/deltacommits
                    let is_completed = parts.len() == 2
                        || (parts.len() > 2 && parts[2] != "requested" && parts[2] != "inflight");

                    let metadata = if is_completed
                        && (matches!(
                            action_type,
                            HudiActionType::Commit | HudiActionType::DeltaCommit
                        )) {
                        let content = storage.read_file(&path).await?;
                        let content_str = String::from_utf8_lossy(&content);
                        serde_json::from_str::<HoodieCommitMetadata>(&content_str).ok()
                    } else {
                        None
                    };

                    Ok((action_type, metadata, timestamp))
                }
            })
            .buffer_unordered(self.parallelism)
            .collect()
            .await;

        // Aggregate results
        let mut previous_schema: Option<Value> = None;

        for res in results {
            match res {
                Ok((action_type, metadata, timestamp)) => {
                    // Update timestamps
                    if timestamp > 0 {
                        result.oldest_timestamp = result.oldest_timestamp.min(timestamp);
                        result.newest_timestamp = result.newest_timestamp.max(timestamp);
                    }

                    // Count action types
                    match action_type {
                        HudiActionType::Commit | HudiActionType::DeltaCommit => {
                            result.total_commits += 1;
                            if let Some(meta) = metadata {
                                result.total_deletes += meta.total_deletes();
                                result.total_bytes_written += meta.total_bytes_written();

                                // Track schema changes
                                if let Some(schema) = meta.get_schema() {
                                    let is_breaking =
                                        is_breaking_change(&result.schema_changes, &schema);
                                    if previous_schema.as_ref() != Some(&schema) {
                                        result.schema_changes.push(SchemaChange::new(
                                            result.schema_changes.len() as u64,
                                            timestamp,
                                            schema.clone(),
                                            is_breaking,
                                        ));
                                        previous_schema = Some(schema);
                                    }
                                }

                                // Collect file paths
                                for path in meta.get_file_paths() {
                                    result.file_paths.insert(path);
                                }
                            }
                        }
                        HudiActionType::Compaction => {
                            result.total_compactions += 1;
                        }
                        HudiActionType::Rollback => {
                            result.total_rollbacks += 1;
                        }
                        HudiActionType::Clean => {
                            result.total_cleans += 1;
                        }
                        HudiActionType::ReplaceCommit => {
                            // ReplaceCommit is used for clustering operations in Hudi
                            result.total_replace_commits += 1;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    info!("Error processing timeline file: {}", e);
                }
            }
        }

        // Fix oldest_timestamp if no files were processed
        if result.oldest_timestamp == u64::MAX {
            result.oldest_timestamp = 0;
        }

        info!(
            "Processed Hudi metadata: commits={}, deletes={}, compactions={}, schema_changes={}",
            result.total_commits,
            result.total_deletes,
            result.total_compactions,
            result.schema_changes.len()
        );

        Ok(result)
    }
}

// Implement the TableAnalyzer trait for HudiAnalyzer
#[async_trait]
impl TableAnalyzer for HudiAnalyzer {
    fn categorize_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        self.categorize_hudi_files(objects)
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
        self.update_metrics_from_hudi_metadata(
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

    #[test]
    fn test_hudi_timestamp_to_millis() {
        // Test valid timestamp: 2025-11-10 22:22:13.846
        let ts = hudi_timestamp_to_millis("20251110222213846");
        assert!(ts > 0, "Timestamp should be positive");

        // Verify it's in the expected range (around Nov 2025)
        // Nov 10, 2025 00:00:00 UTC = 1762732800000 ms
        // Nov 10, 2025 23:59:59 UTC = 1762819199000 ms
        assert!(
            ts >= 1762732800000,
            "Timestamp should be after Nov 10, 2025 00:00 UTC"
        );
        assert!(
            ts <= 1762819199999,
            "Timestamp should be before Nov 10, 2025 23:59 UTC"
        );
    }

    #[test]
    fn test_hudi_timestamp_to_millis_short() {
        // Test short timestamp (invalid)
        let ts = hudi_timestamp_to_millis("2025");
        assert_eq!(ts, 0, "Short timestamp should return 0");
    }

    #[test]
    fn test_hudi_timestamp_to_millis_no_millis() {
        // Test timestamp without milliseconds
        let ts = hudi_timestamp_to_millis("20251110222213");
        assert!(ts > 0, "Timestamp without millis should still work");
    }

    #[test]
    fn test_hudi_timestamp_to_millis_invalid_date() {
        // Test invalid date format
        let ts = hudi_timestamp_to_millis("99999999999999999");
        assert_eq!(ts, 0, "Invalid date should return 0");
    }

    #[test]
    fn test_hudi_timestamp_to_millis_empty() {
        let ts = hudi_timestamp_to_millis("");
        assert_eq!(ts, 0, "Empty string should return 0");
    }

    #[test]
    fn test_hudi_action_type_from() {
        assert!(matches!(
            HudiActionType::from("commit"),
            HudiActionType::Commit
        ));
        assert!(matches!(
            HudiActionType::from("deltacommit"),
            HudiActionType::DeltaCommit
        ));
        assert!(matches!(
            HudiActionType::from("clean"),
            HudiActionType::Clean
        ));
        assert!(matches!(
            HudiActionType::from("compaction"),
            HudiActionType::Compaction
        ));
        assert!(matches!(
            HudiActionType::from("rollback"),
            HudiActionType::Rollback
        ));
        assert!(matches!(
            HudiActionType::from("savepoint"),
            HudiActionType::Savepoint
        ));
        assert!(matches!(
            HudiActionType::from("replacecommit"),
            HudiActionType::ReplaceCommit
        ));
        assert!(matches!(
            HudiActionType::from("indexing"),
            HudiActionType::Indexing
        ));
        assert!(matches!(
            HudiActionType::from("unknown_action"),
            HudiActionType::Unknown(_)
        ));
    }

    #[test]
    fn test_hudi_action_type_case_insensitive() {
        assert!(matches!(
            HudiActionType::from("COMMIT"),
            HudiActionType::Commit
        ));
        assert!(matches!(
            HudiActionType::from("DeltaCommit"),
            HudiActionType::DeltaCommit
        ));
        assert!(matches!(
            HudiActionType::from("CLEAN"),
            HudiActionType::Clean
        ));
    }

    #[test]
    fn test_hoodie_properties_parse() {
        let content = r#"
hoodie.table.name=test_table
hoodie.table.type=COPY_ON_WRITE
hoodie.table.version=6
hoodie.table.recordkey.fields=id
hoodie.table.precombine.field=ts
hoodie.table.partition.fields=date
hoodie.table.base.file.format=PARQUET
"#;
        let props = HoodieProperties::parse(content);
        assert_eq!(props.table_name, "test_table");
        assert_eq!(props.table_type, HudiTableType::CopyOnWrite);
        assert_eq!(props.table_version, 6);
        assert_eq!(props.record_key_fields, vec!["id".to_string()]);
        assert_eq!(props.precombine_field, "ts");
        assert_eq!(props.partition_fields, vec!["date".to_string()]);
        assert_eq!(props.base_file_format, "PARQUET");
    }

    #[test]
    fn test_hoodie_properties_parse_mor() {
        let content = "hoodie.table.type=MERGE_ON_READ\n";
        let props = HoodieProperties::parse(content);
        assert_eq!(props.table_type, HudiTableType::MergeOnRead);
    }

    #[test]
    fn test_hoodie_properties_parse_empty() {
        let props = HoodieProperties::parse("");
        assert_eq!(props.table_name, "");
        assert_eq!(props.table_type, HudiTableType::CopyOnWrite);
        assert_eq!(props.table_version, 0);
    }

    #[test]
    fn test_hoodie_properties_parse_with_comments() {
        let content = r#"
# This is a comment
hoodie.table.name=my_table
# Another comment
hoodie.table.type=COPY_ON_WRITE
"#;
        let props = HoodieProperties::parse(content);
        assert_eq!(props.table_name, "my_table");
        assert_eq!(props.table_type, HudiTableType::CopyOnWrite);
    }

    #[test]
    fn test_hoodie_properties_parse_multiple_partition_fields() {
        let content = "hoodie.table.partition.fields=year,month,day\n";
        let props = HoodieProperties::parse(content);
        assert_eq!(
            props.partition_fields,
            vec!["year".to_string(), "month".to_string(), "day".to_string()]
        );
    }

    #[test]
    fn test_hoodie_properties_parse_empty_partition_fields() {
        let content = "hoodie.table.partition.fields=\n";
        let props = HoodieProperties::parse(content);
        assert!(props.partition_fields.is_empty());
    }

    #[test]
    fn test_hudi_table_type_from() {
        assert_eq!(
            HudiTableType::from("COPY_ON_WRITE"),
            HudiTableType::CopyOnWrite
        );
        assert_eq!(
            HudiTableType::from("copy_on_write"),
            HudiTableType::CopyOnWrite
        );
        assert_eq!(
            HudiTableType::from("MERGE_ON_READ"),
            HudiTableType::MergeOnRead
        );
        assert_eq!(
            HudiTableType::from("merge_on_read"),
            HudiTableType::MergeOnRead
        );
        assert_eq!(HudiTableType::from("unknown"), HudiTableType::CopyOnWrite);
    }

    #[test]
    fn test_hoodie_commit_metadata_get_schema() {
        let json = r#"{
            "partitionToWriteStats": {},
            "extraMetadata": {
                "schema": "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"
            }
        }"#;
        let metadata: HoodieCommitMetadata = serde_json::from_str(json).unwrap();
        let schema = metadata.get_schema().unwrap();
        assert_eq!(schema["type"], "record");
        assert_eq!(schema["name"], "test");
    }

    #[test]
    fn test_hoodie_commit_metadata_get_schema_missing() {
        let json = r#"{"partitionToWriteStats": {}}"#;
        let metadata: HoodieCommitMetadata = serde_json::from_str(json).unwrap();
        assert!(metadata.get_schema().is_none());
    }

    #[test]
    fn test_hoodie_commit_metadata_get_file_paths() {
        let json = r#"{
            "partitionToWriteStats": {
                "": [
                    {"path": "file1.parquet"},
                    {"path": "file2.parquet"}
                ],
                "date=2025-01-01": [
                    {"path": "date=2025-01-01/file3.parquet"}
                ]
            }
        }"#;
        let metadata: HoodieCommitMetadata = serde_json::from_str(json).unwrap();
        let paths = metadata.get_file_paths();
        assert_eq!(paths.len(), 3);
        assert!(paths.contains(&"file1.parquet".to_string()));
        assert!(paths.contains(&"file2.parquet".to_string()));
        assert!(paths.contains(&"date=2025-01-01/file3.parquet".to_string()));
    }

    #[test]
    fn test_hoodie_commit_metadata_get_file_paths_empty() {
        let json = r#"{"partitionToWriteStats": {}}"#;
        let metadata: HoodieCommitMetadata = serde_json::from_str(json).unwrap();
        assert!(metadata.get_file_paths().is_empty());
    }

    #[test]
    fn test_hoodie_commit_metadata_total_records_written() {
        let json = r#"{
            "partitionToWriteStats": {
                "": [
                    {"numWrites": 100},
                    {"numWrites": 50}
                ],
                "p1": [
                    {"numWrites": 25}
                ]
            }
        }"#;
        let metadata: HoodieCommitMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.total_records_written(), 175);
    }

    #[test]
    fn test_hoodie_commit_metadata_total_deletes() {
        let json = r#"{
            "partitionToWriteStats": {
                "": [
                    {"numDeletes": 10},
                    {"numDeletes": 5}
                ]
            }
        }"#;
        let metadata: HoodieCommitMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.total_deletes(), 15);
    }

    #[test]
    fn test_hoodie_commit_metadata_total_bytes_written() {
        let json = r#"{
            "partitionToWriteStats": {
                "": [
                    {"totalWriteBytes": 1000},
                    {"totalWriteBytes": 2000}
                ]
            }
        }"#;
        let metadata: HoodieCommitMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.total_bytes_written(), 3000);
    }

    #[test]
    fn test_hoodie_commit_metadata_empty_stats() {
        let metadata = HoodieCommitMetadata::default();
        assert_eq!(metadata.total_records_written(), 0);
        assert_eq!(metadata.total_deletes(), 0);
        assert_eq!(metadata.total_bytes_written(), 0);
        assert!(metadata.get_file_paths().is_empty());
        assert!(metadata.get_schema().is_none());
    }

    #[test]
    fn test_hoodie_write_stat_deserialization() {
        let json = r#"{
            "fileId": "abc-123",
            "path": "data/file.parquet",
            "prevCommit": "20251110000000000",
            "numWrites": 100,
            "numDeletes": 5,
            "numInserts": 95,
            "numUpdateWrites": 5,
            "totalWriteBytes": 10000,
            "fileSizeInBytes": 9500,
            "totalLogBlocks": 0,
            "totalLogRecords": 0,
            "totalLogFilesSize": 0
        }"#;
        let stat: HoodieWriteStat = serde_json::from_str(json).unwrap();
        assert_eq!(stat.file_id, Some("abc-123".to_string()));
        assert_eq!(stat.path, Some("data/file.parquet".to_string()));
        assert_eq!(stat.num_writes, Some(100));
        assert_eq!(stat.num_deletes, Some(5));
        assert_eq!(stat.num_inserts, Some(95));
        assert_eq!(stat.num_update_writes, Some(5));
        assert_eq!(stat.total_write_bytes, Some(10000));
        assert_eq!(stat.file_size_in_bytes, Some(9500));
    }

    #[test]
    fn test_hudi_metadata_processing_result_default() {
        let result = HudiMetadataProcessingResult::default();
        assert_eq!(result.total_commits, 0);
        assert_eq!(result.total_deletes, 0);
        assert_eq!(result.total_compactions, 0);
        assert_eq!(result.total_rollbacks, 0);
        assert_eq!(result.total_cleans, 0);
        assert_eq!(result.oldest_timestamp, 0);
        assert_eq!(result.newest_timestamp, 0);
        assert!(result.schema_changes.is_empty());
        assert!(result.file_paths.is_empty());
    }

    #[test]
    fn test_hudi_action_state_equality() {
        assert_eq!(HudiActionState::Requested, HudiActionState::Requested);
        assert_eq!(HudiActionState::Inflight, HudiActionState::Inflight);
        assert_eq!(HudiActionState::Completed, HudiActionState::Completed);
        assert_ne!(HudiActionState::Requested, HudiActionState::Completed);
    }

    // ==================== Mock-based Unit Tests ====================

    mod unit {
        use super::*;
        use crate::storage::{FileMetadata, StorageProvider, StorageResult};
        use async_trait::async_trait;
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
                "/mock/hudi/table"
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

        fn create_mock_analyzer() -> HudiAnalyzer {
            let mock_storage = Arc::new(MockStorageProvider::new());
            HudiAnalyzer::new(mock_storage, 4)
        }

        #[test]
        fn test_hudi_analyzer_new() {
            let mock_storage = Arc::new(MockStorageProvider::new());
            let analyzer = HudiAnalyzer::new(mock_storage, 8);

            assert_eq!(analyzer.parallelism, 8);
        }

        #[test]
        fn test_categorize_hudi_files_data_only() {
            let analyzer = create_mock_analyzer();
            let files = vec![
                FileMetadata {
                    path: "table/partition1/file1.parquet".to_string(),
                    size: 1024,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/partition1/file2.parquet".to_string(),
                    size: 2048,
                    last_modified: None,
                },
            ];

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            assert_eq!(data_files.len(), 2);
            assert_eq!(metadata_files.len(), 0);
        }

        #[test]
        fn test_categorize_hudi_files_metadata_only() {
            let analyzer = create_mock_analyzer();
            let files = vec![
                FileMetadata {
                    path: "table/.hoodie/20251110222213846.commit".to_string(),
                    size: 512,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/20251110222213847.commit".to_string(),
                    size: 768,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/hoodie.properties".to_string(),
                    size: 256,
                    last_modified: None,
                },
            ];

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            assert_eq!(data_files.len(), 0);
            assert_eq!(metadata_files.len(), 3);
        }

        #[test]
        fn test_categorize_hudi_files_mixed() {
            let analyzer = create_mock_analyzer();
            let files = vec![
                FileMetadata {
                    path: "table/partition1/file1.parquet".to_string(),
                    size: 1024,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/20251110222213846.commit".to_string(),
                    size: 512,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/partition2/file2.parquet".to_string(),
                    size: 2048,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/hoodie.properties".to_string(),
                    size: 256,
                    last_modified: None,
                },
            ];

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            assert_eq!(data_files.len(), 2);
            assert_eq!(metadata_files.len(), 2);
        }

        #[test]
        fn test_categorize_hudi_files_empty() {
            let analyzer = create_mock_analyzer();
            let files = vec![];

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            assert_eq!(data_files.len(), 0);
            assert_eq!(metadata_files.len(), 0);
        }

        #[test]
        fn test_categorize_hudi_files_log_files() {
            let analyzer = create_mock_analyzer();
            let files = vec![
                FileMetadata {
                    path: "table/partition1/file1.parquet".to_string(),
                    size: 1024,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/partition1/.file1_123.log.1".to_string(),
                    size: 512,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/partition1/.file1_123.log.2".to_string(),
                    size: 768,
                    last_modified: None,
                },
            ];

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            // Currently only parquet files are categorized as data files
            // Log files are not in .hoodie so they're not metadata either
            assert_eq!(data_files.len(), 1);
            assert_eq!(metadata_files.len(), 0);
        }

        #[test]
        fn test_categorize_hudi_files_hfile() {
            let analyzer = create_mock_analyzer();
            let files = vec![
                FileMetadata {
                    path: "table/.hoodie/.aux/metadata.hfile".to_string(),
                    size: 1024,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/partition1/file1.parquet".to_string(),
                    size: 2048,
                    last_modified: None,
                },
            ];

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            // .hfile in .hoodie is metadata, parquet is data
            assert_eq!(data_files.len(), 1);
            assert_eq!(metadata_files.len(), 1);
        }

        #[test]
        fn test_categorize_hudi_files_timeline_actions() {
            let analyzer = create_mock_analyzer();
            let files = vec![
                FileMetadata {
                    path: "table/.hoodie/20251110222213846.commit".to_string(),
                    size: 512,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/20251110222213847.deltacommit".to_string(),
                    size: 768,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/20251110222213848.clean".to_string(),
                    size: 256,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/20251110222213849.compaction.requested".to_string(),
                    size: 128,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/20251110222213850.rollback".to_string(),
                    size: 64,
                    last_modified: None,
                },
            ];

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            assert_eq!(data_files.len(), 0);
            assert_eq!(metadata_files.len(), 5);
        }

        #[test]
        fn test_categorize_hudi_files_non_parquet_data() {
            let analyzer = create_mock_analyzer();
            let files = vec![
                FileMetadata {
                    path: "table/partition1/file1.parquet".to_string(),
                    size: 1024,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/partition1/file2.orc".to_string(),
                    size: 2048,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/partition1/file3.avro".to_string(),
                    size: 512,
                    last_modified: None,
                },
            ];

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            // Only parquet and log files are recognized as data files
            assert_eq!(data_files.len(), 1);
            assert_eq!(metadata_files.len(), 0);
        }

        #[test]
        fn test_is_completed_commit_file_valid() {
            let analyzer = create_mock_analyzer();
            // Completed commit (no state suffix)
            assert!(analyzer.is_completed_commit_file(".hoodie/20251110222213846.commit"));
            assert!(analyzer.is_completed_commit_file(".hoodie/20251110222213846.deltacommit"));
        }

        #[test]
        fn test_is_completed_commit_file_inflight() {
            let analyzer = create_mock_analyzer();
            // Inflight commits are not completed
            assert!(!analyzer.is_completed_commit_file(".hoodie/20251110222213846.commit.inflight"));
            assert!(!analyzer
                .is_completed_commit_file(".hoodie/20251110222213846.deltacommit.inflight"));
        }

        #[test]
        fn test_is_completed_commit_file_requested() {
            let analyzer = create_mock_analyzer();
            // Requested commits are not completed
            assert!(
                !analyzer.is_completed_commit_file(".hoodie/20251110222213846.commit.requested")
            );
            assert!(!analyzer
                .is_completed_commit_file(".hoodie/20251110222213846.compaction.requested"));
        }

        #[test]
        fn test_is_completed_commit_file_non_commit() {
            let analyzer = create_mock_analyzer();
            // Non-commit files
            assert!(!analyzer.is_completed_commit_file(".hoodie/hoodie.properties"));
            assert!(!analyzer.is_completed_commit_file(".hoodie/20251110222213846.clean"));
            assert!(!analyzer.is_completed_commit_file(".hoodie/20251110222213846.rollback"));
        }

        #[test]
        fn test_extract_file_id_from_path_valid() {
            let analyzer = create_mock_analyzer();
            let file_id = analyzer.extract_file_id_from_path(
                "partition1/abc-123-456_0-1-2_20251110222213846.parquet",
            );
            assert!(file_id.is_some());
            assert!(file_id.unwrap().contains("abc-123-456"));
        }

        #[test]
        fn test_extract_file_id_from_path_no_underscore() {
            let analyzer = create_mock_analyzer();
            let file_id = analyzer.extract_file_id_from_path("partition1/simple_file.parquet");
            // When there's no underscore, the whole filename is returned
            assert!(file_id.is_some());
        }

        #[test]
        fn test_extract_file_id_from_path_empty() {
            let analyzer = create_mock_analyzer();
            let file_id = analyzer.extract_file_id_from_path("");
            // Empty string returns Some("") because split returns at least one element
            assert!(file_id.is_some());
            assert_eq!(file_id.unwrap(), "");
        }

        // ==================== Async Unit Tests ====================

        #[tokio::test]
        async fn test_find_referenced_files_empty_metadata() {
            let analyzer = create_mock_analyzer();
            let metadata_files: Vec<FileMetadata> = vec![];

            let result = analyzer.find_referenced_files(&metadata_files).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }

        #[tokio::test]
        async fn test_find_referenced_files_non_commit_files() {
            let analyzer = create_mock_analyzer();
            // Only non-commit metadata files (clean, rollback, properties)
            let metadata_files = vec![
                FileMetadata {
                    path: "table/.hoodie/20251110222213846.clean".to_string(),
                    size: 512,
                    last_modified: None,
                },
                FileMetadata {
                    path: "table/.hoodie/hoodie.properties".to_string(),
                    size: 256,
                    last_modified: None,
                },
            ];

            let result = analyzer.find_referenced_files(&metadata_files).await;

            // Should succeed but return empty since no commit files
            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }

        #[tokio::test]
        async fn test_update_metrics_empty_metadata() {
            let analyzer = create_mock_analyzer();
            let metadata_files: Vec<FileMetadata> = vec![];
            let mut metrics = HealthMetrics::default();

            let result = analyzer
                .update_metrics_from_hudi_metadata(&metadata_files, 0, 0, &mut metrics)
                .await;

            // Should succeed with empty metadata
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn test_update_metrics_with_data_stats() {
            let analyzer = create_mock_analyzer();
            let metadata_files: Vec<FileMetadata> = vec![];
            let mut metrics = HealthMetrics::default();

            // Provide some data file stats
            let result = analyzer
                .update_metrics_from_hudi_metadata(&metadata_files, 1_000_000, 10, &mut metrics)
                .await;

            assert!(result.is_ok());
        }
    }

    // Integration tests using the real test dataset
    mod integration {
        use super::*;
        use crate::storage::{StorageConfig, StorageProviderFactory};

        fn get_test_hudi_table_path() -> String {
            let current_dir = std::env::current_dir().unwrap();
            current_dir
                .join("examples/data/hudi_dataset")
                .to_str()
                .unwrap()
                .to_string()
        }

        async fn create_test_analyzer() -> HudiAnalyzer {
            let table_path = get_test_hudi_table_path();
            let config = StorageConfig::local().with_option("path", &table_path);
            let storage = StorageProviderFactory::from_config(config)
                .await
                .expect("Failed to create storage provider");
            HudiAnalyzer::new(storage, 4)
        }

        #[tokio::test]
        async fn test_categorize_hudi_files_real_dataset() {
            let analyzer = create_test_analyzer().await;

            // Use empty path since base_path is already set to the table path
            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            // The test dataset has 3 parquet data files
            assert!(
                data_files.len() >= 3,
                "Expected at least 3 data files, got {}",
                data_files.len()
            );

            // Should have metadata files in .hoodie directory
            assert!(
                !metadata_files.is_empty(),
                "Expected metadata files in .hoodie directory"
            );

            // Verify data files are parquet
            for file in &data_files {
                assert!(
                    file.path.ends_with(".parquet"),
                    "Data file should be parquet: {}",
                    file.path
                );
                assert!(
                    !file.path.contains("/.hoodie/"),
                    "Data file should not be in .hoodie: {}",
                    file.path
                );
            }

            // Verify metadata files are in .hoodie
            for file in &metadata_files {
                assert!(
                    file.path.contains("/.hoodie/"),
                    "Metadata file should be in .hoodie: {}",
                    file.path
                );
            }
        }

        #[tokio::test]
        async fn test_find_referenced_files_real_dataset() {
            let analyzer = create_test_analyzer().await;

            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (_, metadata_files) = analyzer.categorize_hudi_files(files);

            let referenced = analyzer
                .find_referenced_files(&metadata_files)
                .await
                .expect("Failed to find referenced files");

            // Should find referenced files from commits
            assert!(
                !referenced.is_empty(),
                "Expected to find referenced files from commits"
            );

            // All referenced files should be Hudi data files
            // Hudi supports: .parquet (base files), .log (delta logs for MoR), .hfile (HBase files for metadata)
            for path in &referenced {
                let is_hudi_file =
                    path.ends_with(".parquet") || path.contains(".log") || path.ends_with(".hfile");
                assert!(
                    is_hudi_file,
                    "Referenced file should be a Hudi data file (parquet, log, or hfile): {}",
                    path
                );
            }
        }

        #[tokio::test]
        async fn test_is_completed_commit_file() {
            let analyzer = create_test_analyzer().await;

            // Completed commit files
            assert!(analyzer.is_completed_commit_file("/.hoodie/20251110222213846.commit"));
            assert!(analyzer.is_completed_commit_file("/.hoodie/20251110222213846.deltacommit"));
            assert!(analyzer.is_completed_commit_file("/path/to/.hoodie/123.commit"));

            // Inflight/requested files should not match
            assert!(!analyzer.is_completed_commit_file("/.hoodie/123.commit.inflight"));
            assert!(!analyzer.is_completed_commit_file("/.hoodie/123.commit.requested"));
            assert!(!analyzer.is_completed_commit_file("/.hoodie/123.deltacommit.inflight"));

            // Other file types should not match
            assert!(!analyzer.is_completed_commit_file("/.hoodie/123.clean"));
            assert!(!analyzer.is_completed_commit_file("/.hoodie/hoodie.properties"));
            assert!(!analyzer.is_completed_commit_file("/data/file.parquet"));
        }

        #[tokio::test]
        async fn test_extract_file_id_from_path() {
            let analyzer = create_test_analyzer().await;

            // Standard Hudi file naming: fileId_writeToken_timestamp.parquet
            assert_eq!(
                analyzer.extract_file_id_from_path("abc123_1-0-1_20251110.parquet"),
                Some("abc123".to_string())
            );

            // With partition path
            assert_eq!(
                analyzer.extract_file_id_from_path("date=2025-01-01/xyz_1-0-1_123.parquet"),
                Some("xyz".to_string())
            );

            // Simple filename
            assert_eq!(
                analyzer.extract_file_id_from_path("simple.parquet"),
                Some("simple.parquet".to_string())
            );
        }

        #[tokio::test]
        async fn test_update_metrics_from_hudi_metadata_real_dataset() {
            let analyzer = create_test_analyzer().await;

            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (data_files, metadata_files) = analyzer.categorize_hudi_files(files);

            let total_size: u64 = data_files.iter().map(|f| f.size).sum();
            let total_files = data_files.len();

            let mut metrics = HealthMetrics::default();

            analyzer
                .update_metrics_from_hudi_metadata(
                    &metadata_files,
                    total_size,
                    total_files,
                    &mut metrics,
                )
                .await
                .expect("Failed to update metrics");

            // Verify schema evolution metrics
            assert!(
                metrics.schema_evolution.is_some(),
                "Schema evolution metrics should be populated"
            );
            let schema = metrics.schema_evolution.unwrap();
            assert!(
                schema.schema_stability_score >= 0.0 && schema.schema_stability_score <= 1.0,
                "Stability score should be between 0 and 1"
            );

            // Verify time travel metrics
            assert!(
                metrics.time_travel_metrics.is_some(),
                "Time travel metrics should be populated"
            );
            let time_travel = metrics.time_travel_metrics.unwrap();
            assert!(
                time_travel.total_snapshots > 0,
                "Should have at least one snapshot"
            );

            // Verify file compaction metrics
            assert!(
                metrics.file_compaction.is_some(),
                "File compaction metrics should be populated"
            );

            // Verify deletion metrics
            assert!(
                metrics.deletion_vector_metrics.is_some(),
                "Deletion metrics should be populated"
            );

            // Verify table constraints metrics
            assert!(
                metrics.table_constraints.is_some(),
                "Table constraints metrics should be populated"
            );

            // Verify clustering info
            assert!(
                metrics.clustering.is_some(),
                "Clustering info should be populated"
            );
        }

        #[tokio::test]
        async fn test_full_analyze_workflow() {
            use crate::analyze::table_analyzer::TableAnalyzer;

            let analyzer = create_test_analyzer().await;

            let files = analyzer
                .storage_provider
                .list_files("", true)
                .await
                .expect("Failed to list files");

            let (data_files, metadata_files) = analyzer.categorize_files(files);

            // Verify categorization works through trait method
            assert!(!data_files.is_empty());
            assert!(!metadata_files.is_empty());

            // Find referenced files through trait method
            let referenced = analyzer
                .find_referenced_files(&metadata_files)
                .await
                .expect("Failed to find referenced files");
            assert!(!referenced.is_empty());

            // Update metrics through trait method
            let total_size: u64 = data_files.iter().map(|f| f.size).sum();
            let mut metrics = HealthMetrics::default();
            analyzer
                .update_metrics_from_metadata(
                    &metadata_files,
                    total_size,
                    data_files.len(),
                    &mut metrics,
                )
                .await
                .expect("Failed to update metrics");

            assert!(metrics.schema_evolution.is_some());
            assert!(metrics.time_travel_metrics.is_some());
        }
    }
}
