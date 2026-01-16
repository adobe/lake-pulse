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

use crate::reader::delta::metrics::DeltaMetrics;
#[cfg(feature = "hudi")]
use crate::reader::hudi::metrics::HudiMetrics;
use crate::reader::iceberg::metrics::IcebergMetrics;
#[cfg(feature = "lance")]
use crate::reader::lance::metrics::LanceMetrics;
#[cfg(feature = "paimon")]
use crate::reader::paimon::metrics::PaimonMetrics;
use crate::util::ascii_gantt::to_ascii_gantt;
use crate::util::ascii_gantt::GanttConfig;
use serde::{Deserialize, Serialize};
use serde_json::{json, Error as JsonError};
use std::collections::{HashMap, LinkedList};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Format a size in bytes to a human-readable string with appropriate units.
/// Automatically selects the most appropriate unit (B, KB, MB, GB, TB) based on the size.
///
/// # Examples
/// ```
/// use lake_pulse::analyze::metrics::format_size_smart;
///
/// assert_eq!(format_size_smart(500), "500 B");
/// assert_eq!(format_size_smart(1536), "1.50 KB");
/// assert_eq!(format_size_smart(1_572_864), "1.50 MB");
/// assert_eq!(format_size_smart(1_610_612_736), "1.50 GB");
/// assert_eq!(format_size_smart(1_649_267_441_664), "1.50 TB");
/// ```
pub fn format_size_smart(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Information about a single file in the data lake table.
///
/// Contains metadata about a file including its path, size, modification time,
/// and whether it's referenced by the table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    /// The full path to the file
    pub path: String,
    /// Size of the file in bytes
    pub size_bytes: u64,
    /// Last modification timestamp (ISO 8601 format)
    pub last_modified: Option<String>,
    /// Whether this file is referenced in the table metadata
    pub is_referenced: bool,
}

/// Information about a table partition.
///
/// Contains aggregated metrics for a single partition including file counts,
/// sizes, and the list of files within the partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// Partition key-value pairs (e.g., {"year": "2024", "month": "01"})
    pub partition_values: HashMap<String, String>,
    /// Number of files in this partition
    pub file_count: usize,
    /// Total size of all files in this partition (bytes)
    pub total_size_bytes: u64,
    /// Average file size in this partition (bytes)
    pub avg_file_size_bytes: f64,
    /// List of files in this partition
    pub files: Vec<FileInfo>,
}

/// Clustering information for Iceberg tables.
///
/// Iceberg supports clustering data by specific columns to improve
/// query performance through data locality.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusteringInfo {
    /// Columns used for clustering
    pub clustering_columns: Vec<String>,
    /// Number of clusters in the table
    pub cluster_count: usize,
    /// Average number of files per cluster
    pub avg_files_per_cluster: f64,
    /// Average cluster size (bytes)
    pub avg_cluster_size_bytes: f64,
}

/// Metrics for Delta Lake deletion vectors.
///
/// Deletion vectors are a Delta Lake feature that allows marking rows as deleted
/// without rewriting data files. High deletion vector usage may indicate a need
/// for compaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionVectorMetrics {
    /// Number of deletion vectors in the table
    pub deletion_vector_count: usize,
    /// Total size of all deletion vectors (bytes)
    pub total_deletion_vector_size_bytes: u64,
    /// Average deletion vector size (bytes)
    pub avg_deletion_vector_size_bytes: f64,
    /// Age of the oldest deletion vector (days)
    pub deletion_vector_age_days: f64,
    /// Total number of deleted rows tracked by deletion vectors
    pub deleted_rows_count: u64,
    /// Impact score: 0.0 (no impact) to 1.0 (high impact, compaction recommended)
    pub deletion_vector_impact_score: f64,
}

/// Metrics tracking schema evolution over time.
///
/// Monitors how the table schema has changed, helping identify schema stability
/// and potential compatibility issues.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolutionMetrics {
    /// Total number of schema changes
    pub total_schema_changes: usize,
    /// Number of breaking schema changes
    pub breaking_changes: usize,
    /// Number of non-breaking schema changes
    pub non_breaking_changes: usize,
    /// Schema stability score: 0.0 (unstable) to 1.0 (very stable)
    pub schema_stability_score: f64,
    /// Days since the last schema change
    pub days_since_last_change: f64,
    /// Schema change frequency (changes per day)
    pub schema_change_frequency: f64,
    /// Current schema version number
    pub current_schema_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelMetrics {
    pub total_snapshots: usize,
    pub oldest_snapshot_age_days: f64,
    pub newest_snapshot_age_days: f64,
    pub total_historical_size_bytes: u64,
    pub avg_snapshot_size_bytes: f64,
    pub storage_cost_impact_score: f64, // 0.0 = low cost, 1.0 = high cost
    pub retention_efficiency_score: f64, // 0.0 = inefficient, 1.0 = very efficient
    pub recommended_retention_days: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConstraintsMetrics {
    pub total_constraints: usize,
    pub check_constraints: usize,
    pub not_null_constraints: usize,
    pub unique_constraints: usize,
    pub foreign_key_constraints: usize,
    pub constraint_violation_risk: f64, // 0.0 = low risk, 1.0 = high risk
    pub data_quality_score: f64,        // 0.0 = poor quality, 1.0 = excellent quality
    pub constraint_coverage_score: f64, // 0.0 = no coverage, 1.0 = full coverage
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCompactionMetrics {
    pub compaction_opportunity_score: f64, // 0.0 = no opportunity, 1.0 = high opportunity
    pub small_files_count: usize,
    pub small_files_size_bytes: u64,
    pub potential_compaction_files: usize,
    pub estimated_compaction_savings_bytes: u64,
    pub recommended_target_file_size_bytes: u64,
    pub compaction_priority: String, // "low", "medium", "high", "critical"
    pub z_order_opportunity: bool,
    pub z_order_columns: Vec<String>,
}

/// Comprehensive health metrics for a data lake table.
///
/// This struct contains all the metrics collected during table analysis,
/// including file statistics, partition information, data quality metrics,
/// and format-specific metrics.
///
/// # Examples
///
/// ```no_run
/// use lake_pulse::analyze::metrics::HealthMetrics;
///
/// let mut metrics = HealthMetrics::new();
/// metrics.calculate_data_skew();
/// let score = metrics.calculate_health_score();
/// println!("Health score: {:.2}", score);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMetrics {
    /// Total number of data files in the table
    pub total_files: usize,
    /// Total size of all data files (bytes)
    pub total_size_bytes: u64,
    /// List of files not referenced in table metadata
    pub unreferenced_files: Vec<FileInfo>,
    /// Total size of unreferenced files (bytes)
    pub unreferenced_size_bytes: u64,
    /// Number of partitions in the table
    pub partition_count: usize,
    /// Detailed information about each partition
    pub partitions: Vec<PartitionInfo>,
    /// Clustering information (Iceberg-specific)
    pub clustering: Option<ClusteringInfo>,
    /// Average file size across all files (bytes)
    pub avg_file_size_bytes: f64,
    /// Distribution of files by size category
    pub file_size_distribution: FileSizeDistribution,
    /// List of actionable recommendations
    pub recommendations: Vec<String>,
    /// Overall health score (0.0 to 1.0, higher is better)
    pub health_score: f64,
    /// Data skew metrics
    pub data_skew: DataSkewMetrics,
    /// Metadata health metrics
    pub metadata_health: MetadataHealth,
    /// Snapshot health metrics
    pub snapshot_health: SnapshotHealth,
    /// Deletion vector metrics (Delta-specific)
    pub deletion_vector_metrics: Option<DeletionVectorMetrics>,
    /// Schema evolution metrics
    pub schema_evolution: Option<SchemaEvolutionMetrics>,
    /// Time travel metrics
    pub time_travel_metrics: Option<TimeTravelMetrics>,
    /// Table constraints metrics
    pub table_constraints: Option<TableConstraintsMetrics>,
    /// File compaction opportunity metrics
    pub file_compaction: Option<FileCompactionMetrics>,
    /// Delta Lake specific metrics
    pub delta_table_specific_metrics: Option<DeltaMetrics>,
    /// Apache Hudi specific metrics (requires `hudi` feature)
    #[cfg(feature = "hudi")]
    pub hudi_table_specific_metrics: Option<HudiMetrics>,
    /// Apache Iceberg specific metrics
    pub iceberg_table_specific_metrics: Option<IcebergMetrics>,
    /// Lance specific metrics (requires `lance` feature)
    #[cfg(feature = "lance")]
    pub lance_table_specific_metrics: Option<LanceMetrics>,
    /// Apache Paimon specific metrics (requires `paimon` feature)
    #[cfg(feature = "paimon")]
    pub paimon_table_specific_metrics: Option<PaimonMetrics>,
}

/// Distribution of files by size category.
///
/// Categorizes files into size buckets to help identify potential
/// performance issues from too many small files or very large files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSizeDistribution {
    /// Number of small files (< 16MB)
    pub small_files: usize,
    /// Number of medium files (16MB - 128MB)
    pub medium_files: usize,
    /// Number of large files (128MB - 1GB)
    pub large_files: usize,
    /// Number of very large files (> 1GB)
    pub very_large_files: usize,
}

/// Metrics for detecting data skew in partitions and file sizes.
///
/// Data skew can lead to performance issues where some tasks process
/// significantly more data than others.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSkewMetrics {
    /// Partition skew score: 0.0 (perfectly balanced) to 1.0 (highly skewed)
    pub partition_skew_score: f64,
    /// File size skew score: 0.0 (uniform sizes) to 1.0 (highly varied)
    pub file_size_skew_score: f64,
    /// Size of the largest partition (bytes)
    pub largest_partition_size: u64,
    /// Size of the smallest partition (bytes)
    pub smallest_partition_size: u64,
    /// Average partition size (bytes)
    pub avg_partition_size: u64,
    /// Standard deviation of partition sizes
    pub partition_size_std_dev: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataHealth {
    pub metadata_file_count: usize,
    pub metadata_total_size_bytes: u64,
    pub avg_metadata_file_size: f64,
    pub metadata_growth_rate: f64,  // bytes per day (estimated)
    pub manifest_file_count: usize, // For Iceberg
    pub first_file_name: Option<String>,
    pub last_file_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHealth {
    pub snapshot_count: usize,
    pub oldest_snapshot_age_days: f64,
    pub newest_snapshot_age_days: f64,
    pub avg_snapshot_age_days: f64,
    pub snapshot_retention_risk: f64, // 0.0 (good) to 1.0 (high risk)
}

/// Complete health report for a data lake table.
///
/// This is the main output of the analysis process, containing all metrics,
/// recommendations, and timing information.
///
/// # Examples
///
/// ```no_run
/// use lake_pulse::{Analyzer, StorageConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let config = StorageConfig::local()
///     .with_option("path", "./examples/data");
///
/// let analyzer = Analyzer::builder(config)
///     .build()
///     .await?;
///
/// let report = analyzer.analyze("delta_dataset").await?;
///
/// // Print the report
/// println!("{}", report);
///
/// // Export to JSON
/// let json = report.to_json(false)?;
/// println!("{}", json);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// Path to the analyzed table
    pub table_path: String,
    /// Table format type ("delta", "iceberg", "hudi", or "lance")
    pub table_type: String,
    /// Timestamp when the analysis was performed (ISO 8601 format)
    pub analysis_timestamp: String,
    /// Comprehensive health metrics
    pub metrics: HealthMetrics,
    /// Overall health score (0.0 to 1.0, higher is better)
    pub health_score: f64,
    /// Performance timing metrics
    pub timed_metrics: TimedLikeMetrics,
}

/// Performance timing metrics for analysis operations.
///
/// Tracks the duration of various operations during table analysis,
/// useful for performance profiling and optimization.
///
/// # Examples
///
/// ```no_run
/// use lake_pulse::analyze::metrics::TimedLikeMetrics;
///
/// # fn example(metrics: &TimedLikeMetrics) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// // Export to Chrome tracing format
/// let tracing_json = metrics.to_chrome_tracing()?;
///
/// // Generate ASCII Gantt chart
/// let gantt = metrics.duration_collection_as_gantt(None)?;
/// println!("{}", gantt);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimedLikeMetrics {
    /// Collection of (operation_name, start_time_micros, duration_micros) tuples
    pub duration_collection: LinkedList<(String, u128, u128)>,
}

impl TimedLikeMetrics {
    pub fn to_chrome_tracing(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut events = Vec::new();
        for (name, start, duration) in &self.duration_collection {
            events.push(json!({
                "name": name,
                "cat": "PERF",
                "pid": "1",
                "ph": "B",
                "ts": start * 1000,
            }));
            events.push(json!({
                "name": name,
                "cat": "PERF",
                "pid": "1",
                "ph": "E",
                "ts": start * 1000 + duration * 1000,
            }));
        }
        Ok(serde_json::to_string(&events)?)
    }

    /// Generate an ASCII Gantt chart representation of the timing data
    ///
    /// This creates a visual timeline showing when each operation started and how long it took.
    ///
    /// # Arguments
    ///
    /// * `config` - Optional configuration for the chart appearance. If None, uses default settings.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let gantt = metrics.to_ascii_gantt(None);
    /// println!("{}", gantt);
    /// ```
    ///
    /// Output example:
    /// ```text
    /// Timeline (ms):
    ///                           1000        1550        2100        2650
    ///                           |-----------|-----------|-----------|
    /// storage_config_new_dur    [] 50ms
    /// analyzer_new_dur           [====] 300ms
    /// analyze_total_dur                [========================] 2400ms
    /// ```
    pub fn duration_collection_as_gantt(
        &self,
        config: Option<GanttConfig>,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        to_ascii_gantt(&self.duration_collection, config)
    }
}

impl Display for HealthReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let report = self;

        // Header (no vertical borders for path to allow longer paths)
        writeln!(f, "\n{}", "━".repeat(80))?;
        writeln!(
            f,
            " {:<60} Score: {:>5.1}% ",
            "Table Health Report",
            report.health_score * 100.0
        )?;
        writeln!(f, "{}", "━".repeat(80))?;
        writeln!(f, " {}", report.table_path)?;
        writeln!(f, " {} ({})", report.analysis_timestamp, report.table_type)?;
        writeln!(f, "{}", "━".repeat(80))?;

        // Key Metrics and File Size Distribution (side by side)
        writeln!(f)?;
        writeln!(f, " {:<41} File Size Distribution", "Key Metrics")?;
        writeln!(f, "{}", "━".repeat(80))?;

        let dist = &report.metrics.file_size_distribution;
        let total_files =
            (dist.small_files + dist.medium_files + dist.large_files + dist.very_large_files)
                as f64;

        let size_str = format_size_smart(report.metrics.total_size_bytes);
        let avg_size_str = format_size_smart(report.metrics.avg_file_size_bytes as u64);

        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}  {:>5.1}%",
            "Total Data Files",
            format!("{}", report.metrics.total_files),
            "Small (<16MB)",
            dist.small_files,
            if total_files > 0.0 {
                dist.small_files as f64 / total_files * 100.0
            } else {
                0.0
            }
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}  {:>5.1}%",
            "Total Data Size",
            size_str,
            "Medium (16-128MB)",
            dist.medium_files,
            if total_files > 0.0 {
                dist.medium_files as f64 / total_files * 100.0
            } else {
                0.0
            }
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}  {:>5.1}%",
            "Avg File Size",
            avg_size_str,
            "Large (128MB-1GB)",
            dist.large_files,
            if total_files > 0.0 {
                dist.large_files as f64 / total_files * 100.0
            } else {
                0.0
            }
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}  {:>5.1}%",
            "Partitions",
            format!("{}", report.metrics.partition_count),
            "Very Large (>1GB)",
            dist.very_large_files,
            if total_files > 0.0 {
                dist.very_large_files as f64 / total_files * 100.0
            } else {
                0.0
            }
        )?;

        // Data Skew Analysis and Metadata Health (side by side)
        writeln!(f)?;
        writeln!(f, " {:<41} Metadata Health", "Data Skew Analysis")?;
        writeln!(f, "{}", "━".repeat(80))?;

        let skew = &report.metrics.data_skew;
        let meta = &report.metrics.metadata_health;

        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "Partition Skew",
            format!("{:.2}", skew.partition_skew_score),
            "Count",
            format!("{}", meta.metadata_file_count)
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "File Size Skew",
            format!("{:.2}", skew.file_size_skew_score),
            "Size",
            format_size_smart(meta.metadata_total_size_bytes)
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "Largest Partition",
            if skew.avg_partition_size > 0 {
                format_size_smart(skew.largest_partition_size)
            } else {
                "N/A".to_string()
            },
            "Avg Size",
            if meta.metadata_file_count > 0 {
                format_size_smart(meta.avg_metadata_file_size as u64)
            } else {
                "N/A".to_string()
            }
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "Smallest Partition",
            if skew.avg_partition_size > 0 {
                format_size_smart(skew.smallest_partition_size)
            } else {
                "N/A".to_string()
            },
            "Manifests",
            format!("{}", meta.manifest_file_count)
        )?;
        // Truncate file names if too long
        let first_file = meta.first_file_name.as_deref().unwrap_or("N/A");
        let first_file_display = if first_file.len() > 30 {
            format!("...{}", &first_file[first_file.len() - 27..])
        } else {
            first_file.to_string()
        };

        let last_file = meta.last_file_name.as_deref().unwrap_or("N/A");
        let last_file_display = if last_file.len() > 30 {
            format!("...{}", &last_file[last_file.len() - 27..])
        } else {
            last_file.to_string()
        };

        writeln!(
            f,
            " {:<19} {:>8}              {:<12} {:>15}",
            "Avg Partition Size",
            if skew.avg_partition_size > 0 {
                format_size_smart(skew.avg_partition_size)
            } else {
                "N/A".to_string()
            },
            "First File",
            first_file_display
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<12} {:>15}",
            "", "", "Last File", last_file_display
        )?;

        // Snapshot Health and Unreferenced Files (side by side)
        writeln!(f)?;
        writeln!(f, " {:<41} Unreferenced Files", "Snapshot Health")?;
        writeln!(f, "{}", "━".repeat(80))?;

        let snap = &report.metrics.snapshot_health;
        let has_unreferenced = !report.metrics.unreferenced_files.is_empty();
        let wasted_str = format_size_smart(report.metrics.unreferenced_size_bytes);

        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "Snapshot Count",
            format!("{}", snap.snapshot_count),
            "Count",
            if has_unreferenced {
                format!("{}", report.metrics.unreferenced_files.len())
            } else {
                "0".to_string()
            }
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "Retention Risk",
            format!("{:.1}%", snap.snapshot_retention_risk * 100.0),
            "Wasted Space",
            wasted_str
        )?;
        writeln!(
            f,
            " {:<19} {:>8}",
            "Oldest Snapshot",
            if snap.oldest_snapshot_age_days > 0.0 {
                format!("{:.1} days", snap.oldest_snapshot_age_days)
            } else {
                "N/A".to_string()
            }
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<39}",
            "Newest Snapshot",
            if snap.newest_snapshot_age_days > 0.0 {
                format!("{:.1} days", snap.newest_snapshot_age_days)
            } else {
                "N/A".to_string()
            },
            if has_unreferenced {
                "WARNING: Files exist in storage"
            } else {
                ""
            }
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<39}",
            "Avg Snapshot Age",
            if snap.avg_snapshot_age_days > 0.0 {
                format!("{:.1} days", snap.avg_snapshot_age_days)
            } else {
                "N/A".to_string()
            },
            if has_unreferenced {
                "but not referenced in metadata"
            } else {
                ""
            }
        )?;

        // Clustering (Iceberg) and Deletion Vectors (Delta) - side by side
        // Only show clustering if there are actual clustering columns defined
        let has_clustering = report
            .metrics
            .clustering
            .as_ref()
            .is_some_and(|c| !c.clustering_columns.is_empty());
        let has_deletion_vectors = report.metrics.deletion_vector_metrics.is_some();

        if has_clustering || has_deletion_vectors {
            writeln!(f)?;
            writeln!(
                f,
                " {:<41} {}",
                if has_clustering { "Clustering" } else { "" },
                if has_deletion_vectors {
                    "Deletion Vectors"
                } else {
                    ""
                }
            )?;
            writeln!(f, "{}", "━".repeat(80))?;

            let max_rows = if has_clustering { 4 } else { 5 };

            for i in 0..max_rows {
                let left = if has_clustering {
                    if let Some(ref clustering) = report.metrics.clustering {
                        match i {
                            0 => format!(
                                " {:<19} {:>8}",
                                "Avg Cluster Size",
                                format_size_smart(clustering.avg_cluster_size_bytes as u64)
                            ),
                            1 => format!(
                                " {:<19} {:>8}",
                                "Clusters",
                                format!("{}", clustering.cluster_count)
                            ),
                            2 => format!(
                                " {:<19} {:>8}",
                                "Avg Files/Cluster",
                                format!("{:.2}", clustering.avg_files_per_cluster)
                            ),
                            3 => clustering
                                .clustering_columns
                                .iter()
                                .enumerate()
                                .map(|(i, v)| {
                                    if i == 0 {
                                        format!(" {:<14} {:>13}", "Columns", v)
                                    } else {
                                        format!(" {:<14} {:>13}", "", v)
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join("\n"),
                            _ => format!("{:<40}", ""),
                        }
                    } else {
                        format!("{:<40}", "")
                    }
                } else {
                    format!("{:<40}", "")
                };

                let right = if let Some(ref dv_metrics) = report.metrics.deletion_vector_metrics {
                    let dv_size_str =
                        format_size_smart(dv_metrics.total_deletion_vector_size_bytes);

                    match i {
                        0 => format!(
                            " {:<19} {:>8}",
                            "Vectors",
                            format!("{}", dv_metrics.deletion_vector_count)
                        ),
                        1 => format!(" {:<19} {:>8}", "DV Size", dv_size_str),
                        2 => format!(
                            " {:<19} {:>8}",
                            "Deleted Rows",
                            format!("{}", dv_metrics.deleted_rows_count)
                        ),
                        3 => format!(
                            " {:<19} {:>8}",
                            "Oldest Age (days)",
                            format!("{:.1}", dv_metrics.deletion_vector_age_days)
                        ),
                        4 => format!(
                            " {:<19} {:>8}",
                            "Impact (0-1)",
                            format!("{:.2}", dv_metrics.deletion_vector_impact_score)
                        ),
                        _ => format!("{:<40}", ""),
                    }
                } else {
                    format!("{:<40}", "")
                };

                if has_clustering && has_deletion_vectors {
                    writeln!(f, "{}             {}", left, right)?;
                } else if has_clustering {
                    writeln!(f, "{}", left)?;
                } else {
                    writeln!(f, "{:<40}  {}", "", right)?;
                }
            }
        }

        // Schema Evolution and Time Travel - side by side
        let has_schema = report.metrics.schema_evolution.is_some();
        let has_time_travel = report.metrics.time_travel_metrics.is_some();

        if has_schema || has_time_travel {
            writeln!(f)?;
            writeln!(
                f,
                " {:<41} {}",
                if has_schema { "Schema Evolution" } else { "" },
                if has_time_travel {
                    "Time Travel Analysis"
                } else {
                    ""
                }
            )?;
            writeln!(f, "{}", "━".repeat(80))?;

            for i in 0..7 {
                let left = if let Some(ref schema_metrics) = report.metrics.schema_evolution {
                    match i {
                        0 => format!(
                            " {:<19} {:>8}",
                            "Total Changes",
                            format!("{}", schema_metrics.total_schema_changes)
                        ),
                        1 => format!(
                            " {:<19} {:>8}",
                            "Breaking",
                            format!("{}", schema_metrics.breaking_changes)
                        ),
                        2 => format!(
                            " {:<19} {:>8}",
                            "Non-Breaking",
                            format!("{}", schema_metrics.non_breaking_changes)
                        ),
                        3 => format!(
                            " {:<19} {:>8}",
                            "Stability (0-1)",
                            format!("{:.2}", schema_metrics.schema_stability_score)
                        ),
                        4 => format!(
                            " {:<19} {:>8}",
                            "Days Since Last",
                            format!("{:.1}d", schema_metrics.days_since_last_change)
                        ),
                        5 => format!(
                            " {:<19} {:>8}",
                            "Change Freq",
                            format!("{:.1}/d", schema_metrics.schema_change_frequency)
                        ),
                        6 => format!(
                            " {:<19} {:>8}",
                            "Version",
                            format!("{}", schema_metrics.current_schema_version)
                        ),
                        _ => format!("{:<40}", ""),
                    }
                } else {
                    format!("{:<40}", "")
                };

                let right = if let Some(ref tt_metrics) = report.metrics.time_travel_metrics {
                    let historical_str = format_size_smart(tt_metrics.total_historical_size_bytes);

                    match i {
                        0 => format!(
                            " {:<19} {:>8}",
                            "Snapshots",
                            format!("{}", tt_metrics.total_snapshots)
                        ),
                        1 => format!(
                            " {:<19} {:>8}",
                            "Oldest (days)",
                            format!("{:.1}", tt_metrics.oldest_snapshot_age_days)
                        ),
                        2 => format!(
                            " {:<19} {:>8}",
                            "Newest (days)",
                            format!("{:.1}", tt_metrics.newest_snapshot_age_days)
                        ),
                        3 => format!(" {:<19} {:>8}", "Historical Size", historical_str),
                        4 => format!(
                            " {:<19} {:>8}",
                            "Cost Impact (0-1)",
                            format!("{:.2}", tt_metrics.storage_cost_impact_score)
                        ),
                        5 => format!(
                            " {:<19} {:>8}",
                            "Reten Eff (0-1)",
                            format!("{:.2}", tt_metrics.retention_efficiency_score)
                        ),
                        6 => format!(
                            " {:<19} {:>8}",
                            "Recommended (days)",
                            format!("{}", tt_metrics.recommended_retention_days)
                        ),
                        _ => format!("{:<40}", ""),
                    }
                } else {
                    format!("{:<40}", "")
                };

                if has_schema && has_time_travel {
                    writeln!(f, "{}             {}", left, right)?;
                } else if has_schema {
                    writeln!(f, "{}", left)?;
                } else {
                    writeln!(f, "{:<40}  {}", "", right)?;
                }
            }
        }

        // Table Constraints and File Compaction - side by side
        let has_constraints = report.metrics.table_constraints.is_some();
        let has_compaction = report.metrics.file_compaction.is_some();

        if has_constraints || has_compaction {
            writeln!(f)?;
            writeln!(
                f,
                " {:<41} {}",
                if has_constraints {
                    "Table Constraints"
                } else {
                    ""
                },
                if has_compaction {
                    "File Compaction"
                } else {
                    ""
                }
            )?;
            writeln!(f, "{}", "━".repeat(80))?;

            let max_rows = 9;

            for i in 0..max_rows {
                let left = if let Some(ref constraint_metrics) = report.metrics.table_constraints {
                    match i {
                        0 => format!(
                            " {:<19} {:>8}",
                            "Total",
                            format!("{}", constraint_metrics.total_constraints)
                        ),
                        1 => format!(
                            " {:<19} {:>8}",
                            "Check",
                            format!("{}", constraint_metrics.check_constraints)
                        ),
                        2 => format!(
                            " {:<19} {:>8}",
                            "NOT NULL",
                            format!("{}", constraint_metrics.not_null_constraints)
                        ),
                        3 => format!(
                            " {:<19} {:>8}",
                            "Unique",
                            format!("{}", constraint_metrics.unique_constraints)
                        ),
                        4 => format!(
                            " {:<19} {:>8}",
                            "Foreign Key",
                            format!("{}", constraint_metrics.foreign_key_constraints)
                        ),
                        5 => format!(
                            " {:<20} {:>7}",
                            "Violation Risk (0-1)",
                            format!("{:.2}", constraint_metrics.constraint_violation_risk)
                        ),
                        6 => format!(
                            " {:<19} {:>8}",
                            "Quality Score (0-1)",
                            format!("{:.2}", constraint_metrics.data_quality_score)
                        ),
                        7 => format!(
                            " {:<19} {:>8}",
                            "Coverage (0-1)",
                            format!("{:.2}", constraint_metrics.constraint_coverage_score)
                        ),
                        _ => format!("{:<29}", ""),
                    }
                } else {
                    format!("{:<40}", "")
                };

                let right = if let Some(ref compaction_metrics) = report.metrics.file_compaction {
                    let small_files_str =
                        format_size_smart(compaction_metrics.small_files_size_bytes);
                    let savings_str =
                        format_size_smart(compaction_metrics.estimated_compaction_savings_bytes);
                    let target_str =
                        format_size_smart(compaction_metrics.recommended_target_file_size_bytes);

                    match i {
                        0 => format!(
                            " {:<19} {:>8}",
                            "Opportunity (0-1)",
                            format!("{:.2}", compaction_metrics.compaction_opportunity_score)
                        ),
                        1 => format!(
                            " {:<19} {:>8}",
                            "Small Files",
                            format!("{}", compaction_metrics.small_files_count)
                        ),
                        2 => format!(" {:<19} {:>8}", "Small Size", small_files_str),
                        3 => format!(
                            " {:<19} {:>8}",
                            "Potential Files",
                            format!("{}", compaction_metrics.potential_compaction_files)
                        ),
                        4 => format!(" {:<19} {:>8}", "Savings", savings_str),
                        5 => format!(" {:<19} {:>8}", "Target Size", target_str),
                        6 => format!(
                            " {:<19} {:>8}",
                            "Priority",
                            compaction_metrics.compaction_priority.to_uppercase()
                        ),
                        7 => format!(
                            " {:<19} {:>8}",
                            "Z-Order",
                            if compaction_metrics.z_order_opportunity {
                                "Yes"
                            } else {
                                "No"
                            }
                        ),
                        8 => {
                            if !compaction_metrics.z_order_columns.is_empty() {
                                let print_rows = compaction_metrics
                                    .z_order_columns
                                    .iter()
                                    .enumerate()
                                    .map(|(i, v)| {
                                        if i == 0 {
                                            format!(" {:<14} {:>13}", "Z-Columns", v)
                                        } else {
                                            format!(" {:<14} {:>13}", "", v)
                                        }
                                    })
                                    .collect::<Vec<_>>();

                                print_rows.join("\n")

                                // format!(
                                //     " {:<19} {:>8}",
                                //     "Z-Columns",
                                //     compaction_metrics.z_order_columns.join(", ")
                                // )
                            } else {
                                format!("{:<40}", "")
                            }
                        }
                        _ => format!("{:<40}", ""),
                    }
                } else {
                    format!("{:<40}", "")
                };

                // Skip empty rows (e.g., row 8 when no Z-Order columns)
                let left_empty = left.trim().is_empty();
                let right_empty = right.trim().is_empty();

                if left_empty && right_empty {
                    continue;
                }

                if has_constraints && has_compaction {
                    writeln!(f, "{}             {}", left, right)?;
                } else if has_constraints {
                    writeln!(f, "{}", left)?;
                } else {
                    writeln!(f, "{:<40}  {}", "", right)?;
                }
            }
        }

        // Format-Specific Metrics
        if let Some(ref delta_metrics) = report.metrics.delta_table_specific_metrics {
            write!(f, "{}", delta_metrics)?;
        }
        if let Some(ref iceberg_metrics) = report.metrics.iceberg_table_specific_metrics {
            write!(f, "{}", iceberg_metrics)?;
        }
        #[cfg(feature = "hudi")]
        if let Some(ref hudi_metrics) = report.metrics.hudi_table_specific_metrics {
            write!(f, "{}", hudi_metrics)?;
        }
        #[cfg(feature = "lance")]
        if let Some(ref lance_metrics) = report.metrics.lance_table_specific_metrics {
            write!(f, "{}", lance_metrics)?;
        }
        #[cfg(feature = "paimon")]
        if let Some(ref paimon_metrics) = report.metrics.paimon_table_specific_metrics {
            write!(f, "{}", paimon_metrics)?;
        }

        // Recommendations (full width)
        writeln!(f)?;
        writeln!(f, " Recommendations")?;
        writeln!(f, "{}", "━".repeat(80))?;
        if !report.metrics.recommendations.is_empty() {
            for (i, rec) in report.metrics.recommendations.iter().enumerate() {
                writeln!(f, " {}. {}", i + 1, rec)?;
            }
        } else {
            writeln!(f, "  No recommendations - table is in excellent health!")?;
        }

        Ok(())
    }
}

impl HealthReport {
    pub fn to_json(&self, exclude_files: bool) -> Result<String, JsonError> {
        if exclude_files {
            let mut report = self.clone();
            report.metrics.unreferenced_files = Vec::new();
            report
                .metrics
                .partitions
                .iter_mut()
                .for_each(|p| p.files = Vec::new());
            serde_json::to_string_pretty(&report)
        } else {
            serde_json::to_string_pretty(self)
        }
    }
}

impl Default for HealthMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthMetrics {
    pub fn new() -> Self {
        Self {
            total_files: 0,
            total_size_bytes: 0,
            unreferenced_files: Vec::new(),
            unreferenced_size_bytes: 0,
            partition_count: 0,
            partitions: Vec::new(),
            clustering: None,
            avg_file_size_bytes: 0.0,
            file_size_distribution: FileSizeDistribution {
                small_files: 0,
                medium_files: 0,
                large_files: 0,
                very_large_files: 0,
            },
            recommendations: Vec::new(),
            health_score: 0.0,
            data_skew: DataSkewMetrics {
                partition_skew_score: 0.0,
                file_size_skew_score: 0.0,
                largest_partition_size: 0,
                smallest_partition_size: 0,
                avg_partition_size: 0,
                partition_size_std_dev: 0.0,
            },
            metadata_health: MetadataHealth {
                metadata_file_count: 0,
                metadata_total_size_bytes: 0,
                avg_metadata_file_size: 0.0,
                metadata_growth_rate: 0.0,
                manifest_file_count: 0,
                first_file_name: None,
                last_file_name: None,
            },
            snapshot_health: SnapshotHealth {
                snapshot_count: 0,
                oldest_snapshot_age_days: 0.0,
                newest_snapshot_age_days: 0.0,
                avg_snapshot_age_days: 0.0,
                snapshot_retention_risk: 0.0,
            },
            deletion_vector_metrics: None,
            schema_evolution: None,
            time_travel_metrics: None,
            table_constraints: None,
            file_compaction: None,
            delta_table_specific_metrics: None,
            #[cfg(feature = "hudi")]
            hudi_table_specific_metrics: None,
            iceberg_table_specific_metrics: None,
            #[cfg(feature = "lance")]
            lance_table_specific_metrics: None,
            #[cfg(feature = "paimon")]
            paimon_table_specific_metrics: None,
        }
    }

    pub fn calculate_health_score(&self) -> f64 {
        let mut score = 1.0;

        // Penalize unreferenced files
        if self.total_files > 0 {
            let unreferenced_ratio = self.unreferenced_files.len() as f64 / self.total_files as f64;
            score -= unreferenced_ratio * 0.3;
        }

        // Penalize small files (inefficient)
        if self.total_files > 0 {
            let small_file_ratio =
                self.file_size_distribution.small_files as f64 / self.total_files as f64;
            score -= small_file_ratio * 0.2;
        }

        // Penalize very large files (potential performance issues)
        if self.total_files > 0 {
            let very_large_ratio =
                self.file_size_distribution.very_large_files as f64 / self.total_files as f64;
            score -= very_large_ratio * 0.1;
        }

        // Reward good partitioning
        if self.partition_count > 0 && self.total_files > 0 {
            let avg_files_per_partition = self.total_files as f64 / self.partition_count as f64;
            if avg_files_per_partition > 100.0 {
                score -= 0.1; // Too many files per partition
            } else if avg_files_per_partition < 5.0 {
                score -= 0.05; // Too few files per partition
            }
        }

        // Penalize data skew
        score -= self.data_skew.partition_skew_score * 0.15;
        score -= self.data_skew.file_size_skew_score * 0.1;

        // Penalize metadata bloat
        if self.metadata_health.metadata_total_size_bytes > 100 * 1024 * 1024 {
            // > 100MB
            score -= 0.05;
        }

        // Penalize snapshot retention issues
        score -= self.snapshot_health.snapshot_retention_risk * 0.1;

        // Penalize deletion vector impact
        if let Some(ref dv_metrics) = self.deletion_vector_metrics {
            score -= dv_metrics.deletion_vector_impact_score * 0.15;
        }

        // Factor in schema stability
        if let Some(ref schema_metrics) = self.schema_evolution {
            score -= (1.0 - schema_metrics.schema_stability_score) * 0.2;
        }

        // Factor in time travel storage costs
        if let Some(ref tt_metrics) = self.time_travel_metrics {
            score -= tt_metrics.storage_cost_impact_score * 0.1;
            score -= (1.0 - tt_metrics.retention_efficiency_score) * 0.05;
        }

        // Factor in data quality from constraints
        if let Some(ref constraint_metrics) = self.table_constraints {
            score -= (1.0 - constraint_metrics.data_quality_score) * 0.15;
            score -= constraint_metrics.constraint_violation_risk * 0.1;
        }

        // Factor in file compaction opportunities
        if let Some(ref compaction_metrics) = self.file_compaction {
            score -= (1.0 - compaction_metrics.compaction_opportunity_score) * 0.1;
        }

        score.clamp(0.0, 1.0)
    }

    pub fn calculate_data_skew(&mut self) {
        if self.partitions.is_empty() {
            return;
        }

        let partition_sizes: Vec<u64> =
            self.partitions.iter().map(|p| p.total_size_bytes).collect();
        let file_counts: Vec<usize> = self.partitions.iter().map(|p| p.file_count).collect();

        // Calculate partition size skew
        if !partition_sizes.is_empty() {
            let total_size: u64 = partition_sizes.iter().sum();
            let avg_size = total_size as f64 / partition_sizes.len() as f64;

            let variance = partition_sizes
                .iter()
                .map(|&size| (size as f64 - avg_size).powi(2))
                .sum::<f64>()
                / partition_sizes.len() as f64;

            let std_dev = variance.sqrt();
            let coefficient_of_variation = if avg_size > 0.0 {
                std_dev / avg_size
            } else {
                0.0
            };

            self.data_skew.partition_skew_score = coefficient_of_variation.min(1.0);
            self.data_skew.largest_partition_size = *partition_sizes.iter().max().unwrap_or(&0);
            self.data_skew.smallest_partition_size = *partition_sizes.iter().min().unwrap_or(&0);
            self.data_skew.avg_partition_size = avg_size as u64;
            self.data_skew.partition_size_std_dev = std_dev;
        }

        // Calculate file count skew
        if !file_counts.is_empty() {
            let total_files: usize = file_counts.iter().sum();
            let avg_files = total_files as f64 / file_counts.len() as f64;

            let variance = file_counts
                .iter()
                .map(|&count| (count as f64 - avg_files).powi(2))
                .sum::<f64>()
                / file_counts.len() as f64;

            let std_dev = variance.sqrt();
            let coefficient_of_variation = if avg_files > 0.0 {
                std_dev / avg_files
            } else {
                0.0
            };

            self.data_skew.file_size_skew_score = coefficient_of_variation.min(1.0);
        }
    }

    pub fn calculate_snapshot_health(&mut self, fallback_snapshot_count: usize) {
        // Use snapshot count from time_travel_metrics if available (set by table-specific analyzer),
        // otherwise fall back to the provided count (typically metadata_files.len())
        let snapshot_count = self
            .time_travel_metrics
            .as_ref()
            .map(|ttm| ttm.total_snapshots)
            .unwrap_or(fallback_snapshot_count);

        self.snapshot_health.snapshot_count = snapshot_count;

        // Use age info from time_travel_metrics if available
        if let Some(ref ttm) = self.time_travel_metrics {
            self.snapshot_health.oldest_snapshot_age_days = ttm.oldest_snapshot_age_days;
            self.snapshot_health.newest_snapshot_age_days = ttm.newest_snapshot_age_days;
            self.snapshot_health.avg_snapshot_age_days =
                (ttm.oldest_snapshot_age_days + ttm.newest_snapshot_age_days) / 2.0;
        } else {
            // Simplified snapshot age calculation (would need actual timestamps)
            self.snapshot_health.oldest_snapshot_age_days = 0.0;
            self.snapshot_health.newest_snapshot_age_days = 0.0;
            self.snapshot_health.avg_snapshot_age_days = 0.0;
        }

        // Calculate retention risk based on snapshot count
        if snapshot_count > 100 {
            self.snapshot_health.snapshot_retention_risk = 0.8;
        } else if snapshot_count > 50 {
            self.snapshot_health.snapshot_retention_risk = 0.5;
        } else if snapshot_count > 20 {
            self.snapshot_health.snapshot_retention_risk = 0.2;
        } else {
            self.snapshot_health.snapshot_retention_risk = 0.0;
        }
    }

    pub fn generate_recommendations(&mut self) {
        // Check for unreferenced files
        if !self.unreferenced_files.is_empty() {
            self.recommendations.push(format!(
                "Found {} unreferenced files ({}). Consider cleaning up orphaned data files.",
                self.unreferenced_files.len(),
                format_size_smart(self.unreferenced_size_bytes)
            ));
        }

        // Check file size distribution
        let total_files = self.total_files as f64;
        if total_files > 0.0 {
            let small_file_ratio = self.file_size_distribution.small_files as f64 / total_files;
            if small_file_ratio > 0.5 {
                self.recommendations.push(
                    "High percentage of small files detected. Consider compacting to improve query performance.".to_string()
                );
            }

            let very_large_ratio =
                self.file_size_distribution.very_large_files as f64 / total_files;
            if very_large_ratio > 0.1 {
                self.recommendations.push(
                    "Some very large files detected. Consider splitting large files for better parallelism.".to_string()
                );
            }
        }

        // Check partitioning
        if self.partition_count > 0 {
            let avg_files_per_partition = total_files / self.partition_count as f64;
            if avg_files_per_partition > 100.0 {
                self.recommendations.push(
                    "High number of files per partition. Consider repartitioning to reduce file count.".to_string()
                );
            } else if avg_files_per_partition < 5.0 {
                self.recommendations.push(
                    "Low number of files per partition. Consider consolidating partitions."
                        .to_string(),
                );
            }
        }

        // Check for empty partitions
        let empty_partitions = self.partitions.iter().filter(|p| p.file_count == 0).count();
        if empty_partitions > 0 {
            self.recommendations.push(format!(
                "Found {} empty partitions. Consider removing empty partition directories.",
                empty_partitions
            ));
        }

        // Check data skew
        if self.data_skew.partition_skew_score > 0.5 {
            self.recommendations.push(
                "High partition skew detected. Consider repartitioning to balance data distribution.".to_string()
            );
        }

        if self.data_skew.file_size_skew_score > 0.5 {
            self.recommendations.push(
                "High file size skew detected. Consider running OPTIMIZE to balance file sizes."
                    .to_string(),
            );
        }

        // Check metadata health
        if self.metadata_health.metadata_total_size_bytes > 50 * 1024 * 1024 {
            // > 50MB
            self.recommendations.push(
                "Large metadata size detected. Consider running VACUUM to clean up old transaction logs.".to_string()
            );
        }

        // Check snapshot health
        if self.snapshot_health.snapshot_retention_risk > 0.7 {
            self.recommendations.push(
                "High snapshot retention risk. Consider running VACUUM to remove old snapshots."
                    .to_string(),
            );
        }

        // Check clustering
        if let Some(ref clustering) = self.clustering {
            if clustering.avg_files_per_cluster > 50.0 {
                self.recommendations.push(
                    "High number of files per cluster. Consider optimizing clustering strategy."
                        .to_string(),
                );
            }

            if clustering.clustering_columns.len() > 4 {
                self.recommendations.push(
                    "Too many clustering columns detected. Consider reducing to 4 or fewer columns for optimal performance.".to_string()
                );
            }

            if clustering.clustering_columns.is_empty() {
                self.recommendations.push(
                    "No clustering detected. Consider enabling liquid clustering for better query performance.".to_string()
                );
            }
        }

        // Check deletion vectors
        if let Some(ref dv_metrics) = self.deletion_vector_metrics {
            if dv_metrics.deletion_vector_impact_score > 0.7 {
                self.recommendations.push(
                    "High deletion vector impact detected. Consider running VACUUM to clean up old deletion vectors.".to_string()
                );
            }

            if dv_metrics.deletion_vector_count > 50 {
                self.recommendations.push(
                    "Many deletion vectors detected. Consider optimizing delete operations to reduce fragmentation.".to_string()
                );
            }

            if dv_metrics.deletion_vector_age_days > 30.0 {
                self.recommendations.push(
                    "Old deletion vectors detected. Consider running VACUUM to clean up deletion vectors older than 30 days.".to_string()
                );
            }
        }

        // Check schema evolution
        if let Some(ref schema_metrics) = self.schema_evolution {
            if schema_metrics.schema_stability_score < 0.5 {
                self.recommendations.push(
                    "Unstable schema detected. Consider planning schema changes more carefully to improve performance.".to_string()
                );
            }

            if schema_metrics.breaking_changes > 5 {
                self.recommendations.push(
                    "Many breaking schema changes detected. Consider using schema evolution features to avoid breaking changes.".to_string()
                );
            }

            if schema_metrics.schema_change_frequency > 1.0 {
                self.recommendations.push(
                    "High schema change frequency detected. Consider batching schema changes to reduce performance impact.".to_string()
                );
            }

            if schema_metrics.days_since_last_change < 1.0 {
                self.recommendations.push(
                    "Recent schema changes detected. Monitor query performance for potential issues.".to_string()
                );
            }
        }

        // Check time travel storage costs
        if let Some(ref tt_metrics) = self.time_travel_metrics {
            if tt_metrics.storage_cost_impact_score > 0.7 {
                self.recommendations.push(
                    "High time travel storage costs detected. Consider running VACUUM to clean up old snapshots.".to_string()
                );
            }

            if tt_metrics.retention_efficiency_score < 0.5 {
                self.recommendations.push(
                    "Inefficient snapshot retention detected. Consider optimizing retention policy.".to_string()
                );
            }

            if tt_metrics.total_snapshots > 1000 {
                self.recommendations.push(
                    "High snapshot count detected. Consider reducing retention period to improve performance.".to_string()
                );
            }
        }

        // Check table constraints
        if let Some(ref constraint_metrics) = self.table_constraints {
            if constraint_metrics.data_quality_score < 0.5 {
                self.recommendations.push(
                    "Low data quality score detected. Consider adding more table constraints."
                        .to_string(),
                );
            }

            if constraint_metrics.constraint_violation_risk > 0.7 {
                self.recommendations.push(
                    "High constraint violation risk detected. Monitor data quality and consider data validation.".to_string()
                );
            }

            if constraint_metrics.constraint_coverage_score < 0.3 {
                self.recommendations.push(
                    "Low constraint coverage detected. Consider adding check constraints for better data quality.".to_string()
                );
            }
        }

        // Check file compaction opportunities
        if let Some(ref compaction_metrics) = self.file_compaction {
            if compaction_metrics.compaction_opportunity_score > 0.7 {
                self.recommendations.push(
                    "High file compaction opportunity detected. Consider running OPTIMIZE to improve performance.".to_string()
                );
            }

            if compaction_metrics.compaction_priority == "critical" {
                self.recommendations.push(
                    "Critical compaction priority detected. Run OPTIMIZE immediately to improve query performance.".to_string()
                );
            }

            if compaction_metrics.z_order_opportunity {
                self.recommendations.push(
                    format!("Z-ordering opportunity detected. Consider running OPTIMIZE ZORDER BY ({}) to improve query performance.",
                            compaction_metrics.z_order_columns.join(", ")).to_string()
                );
            }

            if compaction_metrics.estimated_compaction_savings_bytes > 100 * 1024 * 1024 {
                // > 100MB
                self.recommendations.push(format!(
                    "Significant compaction savings available: {}. Consider running OPTIMIZE.",
                    format_size_smart(compaction_metrics.estimated_compaction_savings_bytes)
                ));
            }
        }

        // Check Lance-specific index recommendations
        #[cfg(feature = "lance")]
        if let Some(ref lance_metrics) = self.lance_table_specific_metrics {
            let index_info = &lance_metrics.index_info;

            // Recommend creating indices for large tables without any indices
            if index_info.num_indices == 0 {
                if let Some(num_rows) = lance_metrics.metadata.num_rows {
                    if num_rows > 10_000 {
                        self.recommendations.push(
                            "No indices found on Lance table. Consider creating vector or scalar indices for frequently queried columns.".to_string()
                        );
                    }
                }
            }

            // Check for tables with many fragments that could benefit from indexing
            if lance_metrics.fragment_info.num_fragments > 100 && index_info.num_indices == 0 {
                self.recommendations.push(
                    "Large number of fragments detected without indices. Consider creating indices to improve query performance.".to_string()
                );
            }

            // Check for high deletion ratio that might affect index performance
            if let Some(num_deleted) = lance_metrics.metadata.num_deleted_rows {
                if let Some(num_rows) = lance_metrics.metadata.num_rows {
                    if num_rows > 0 {
                        let deletion_ratio = num_deleted as f64 / (num_rows + num_deleted) as f64;
                        if deletion_ratio > 0.2 && index_info.num_indices > 0 {
                            self.recommendations.push(
                                "High deletion ratio detected with existing indices. Consider rebuilding indices after compaction.".to_string()
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== format_size_smart tests ====================

    #[test]
    fn test_format_size_smart_bytes() {
        assert_eq!(format_size_smart(0), "0 B");
        assert_eq!(format_size_smart(1), "1 B");
        assert_eq!(format_size_smart(500), "500 B");
        assert_eq!(format_size_smart(1023), "1023 B");
    }

    #[test]
    fn test_format_size_smart_kilobytes() {
        assert_eq!(format_size_smart(1024), "1.00 KB");
        assert_eq!(format_size_smart(1536), "1.50 KB");
        assert_eq!(format_size_smart(2048), "2.00 KB");
        assert_eq!(format_size_smart(10 * 1024), "10.00 KB");
        assert_eq!(format_size_smart(1024 * 1024 - 1), "1024.00 KB");
    }

    #[test]
    fn test_format_size_smart_megabytes() {
        assert_eq!(format_size_smart(1024 * 1024), "1.00 MB");
        assert_eq!(format_size_smart(1_572_864), "1.50 MB"); // 1.5 MB
        assert_eq!(format_size_smart(100 * 1024 * 1024), "100.00 MB");
        assert_eq!(format_size_smart(1024 * 1024 * 1024 - 1), "1024.00 MB");
    }

    #[test]
    fn test_format_size_smart_gigabytes() {
        assert_eq!(format_size_smart(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_size_smart(1_610_612_736), "1.50 GB"); // 1.5 GB
        assert_eq!(format_size_smart(50 * 1024 * 1024 * 1024), "50.00 GB");
        assert_eq!(
            format_size_smart(1024 * 1024 * 1024 * 1024 - 1),
            "1024.00 GB"
        );
    }

    #[test]
    fn test_format_size_smart_terabytes() {
        assert_eq!(format_size_smart(1024 * 1024 * 1024 * 1024), "1.00 TB");
        assert_eq!(format_size_smart(1_649_267_441_664), "1.50 TB"); // 1.5 TB
        assert_eq!(
            format_size_smart(10 * 1024 * 1024 * 1024 * 1024),
            "10.00 TB"
        );
    }

    #[test]
    fn test_format_size_smart_boundary_values() {
        // Test exact boundaries between units
        assert_eq!(format_size_smart(1023), "1023 B");
        assert_eq!(format_size_smart(1024), "1.00 KB");

        assert_eq!(format_size_smart(1024 * 1024 - 1), "1024.00 KB");
        assert_eq!(format_size_smart(1024 * 1024), "1.00 MB");

        assert_eq!(format_size_smart(1024 * 1024 * 1024 - 1), "1024.00 MB");
        assert_eq!(format_size_smart(1024 * 1024 * 1024), "1.00 GB");

        assert_eq!(
            format_size_smart(1024 * 1024 * 1024 * 1024 - 1),
            "1024.00 GB"
        );
        assert_eq!(format_size_smart(1024 * 1024 * 1024 * 1024), "1.00 TB");
    }

    #[test]
    fn test_format_size_smart_realistic_file_sizes() {
        // Small parquet file
        assert_eq!(format_size_smart(4240), "4.14 KB");
        // Medium parquet file
        assert_eq!(format_size_smart(16 * 1024 * 1024), "16.00 MB");
        // Large parquet file
        assert_eq!(format_size_smart(128 * 1024 * 1024), "128.00 MB");
        // Very large file
        assert_eq!(format_size_smart(2 * 1024 * 1024 * 1024), "2.00 GB");
    }

    // ==================== Chrome tracing tests ====================

    #[test]
    fn test_chrome_tracing_with_real_data_reproduces_overflow() {
        // This test reproduces the overflow bug with real Chrome tracing data
        // The input has timestamps in microseconds (very large numbers)
        let chrome_trace_input = r#"[{"cat":"PERF","name":"storage_config_new_dur","ph":"B","pid":"1","ts":1762455078689000},{"cat":"PERF","name":"storage_config_new_dur","ph":"E","pid":"1","ts":1762455078690000}]"#;

        let events: Vec<serde_json::Value> =
            serde_json::from_str(chrome_trace_input).expect("Failed to parse Chrome tracing JSON");

        // Extract timing data - timestamps are in microseconds
        let begin_ts = events[0]["ts"].as_u64().unwrap() as u128;
        let end_ts = events[1]["ts"].as_u64().unwrap() as u128;

        println!("Begin timestamp (microseconds): {}", begin_ts);
        println!("End timestamp (microseconds): {}", end_ts);
        println!("Duration (microseconds): {}", end_ts - begin_ts);

        // The current code expects milliseconds and multiplies by 1000
        // If we naively use these microsecond values as milliseconds, we get overflow
        let mut collection = LinkedList::new();

        // This is what would cause overflow - treating microseconds as milliseconds
        // collection.push_back(("storage_config_new_dur".to_string(), begin_ts, end_ts - begin_ts));

        // The correct approach: timestamps in the input are already in microseconds
        // So we should NOT multiply by 1000 in to_chrome_tracing(), OR
        // we need to store timestamps in milliseconds in our internal format

        // For now, let's test with the corrected approach (divide by 1000 to get milliseconds)
        collection.push_back((
            "storage_config_new_dur".to_string(),
            begin_ts / 1000,            // Convert to milliseconds
            (end_ts - begin_ts) / 1000, // Duration in milliseconds
        ));

        let metrics = TimedLikeMetrics {
            duration_collection: collection,
        };

        // This should work without overflow now
        let output = metrics
            .to_chrome_tracing()
            .expect("Should generate Chrome tracing output");

        let output_events: Vec<serde_json::Value> =
            serde_json::from_str(&output).expect("Should parse output");

        // Verify the output
        assert_eq!(output_events.len(), 2);

        // The output timestamps should be in microseconds (original format)
        let output_begin_ts = output_events[0]["ts"].as_u64().unwrap();
        let output_end_ts = output_events[1]["ts"].as_u64().unwrap();

        println!("Output begin timestamp: {}", output_begin_ts);
        println!("Output end timestamp: {}", output_end_ts);

        // Verify the timestamps are approximately correct (within rounding error)
        assert!(
            (output_begin_ts as i128 - begin_ts as i128).abs() < 1000,
            "Begin timestamp should be approximately preserved"
        );
        assert!(
            (output_end_ts as i128 - end_ts as i128).abs() < 1000,
            "End timestamp should be approximately preserved"
        );
    }

    #[test]
    #[should_panic(expected = "attempt to multiply with overflow")]
    fn test_chrome_tracing_overflow_bug() {
        // This test demonstrates the overflow bug when using very large timestamps
        let mut collection = LinkedList::new();

        // Use a timestamp that will overflow when multiplied by 1000
        // u128::MAX / 1000 = 340282366920938463463374607431768211
        // Anything larger than this will overflow
        let large_timestamp: u128 = u128::MAX / 500; // This will overflow when multiplied by 1000

        collection.push_back(("test_metric".to_string(), large_timestamp, 1000));

        let metrics = TimedLikeMetrics {
            duration_collection: collection,
        };

        // This should panic with overflow in debug mode
        let _ = metrics.to_chrome_tracing();
    }

    #[test]
    fn test_parse_full_chrome_trace_and_regenerate() {
        // This test parses the FULL Chrome tracing JSON provided by the user
        // and attempts to regenerate it, which should expose any overflow issues
        let chrome_trace_input = r#"[{"cat":"PERF","name":"storage_config_new_dur","ph":"B","pid":"1","ts":1762455078689000},{"cat":"PERF","name":"storage_config_new_dur","ph":"E","pid":"1","ts":1762455078690000},{"cat":"PERF","name":"analyzer_new_dur","ph":"B","pid":"1","ts":1762455078691000},{"cat":"PERF","name":"analyzer_new_dur","ph":"E","pid":"1","ts":1762455078972000},{"cat":"PERF","name":"validate_connection_dur","ph":"B","pid":"1","ts":1762455078973000},{"cat":"PERF","name":"validate_connection_dur","ph":"E","pid":"1","ts":1762455080930000},{"cat":"PERF","name":"discover_partitions","ph":"B","pid":"1","ts":1762455080932000},{"cat":"PERF","name":"discover_partitions","ph":"E","pid":"1","ts":1762455081493000},{"cat":"PERF","name":"list_files_parallel","ph":"B","pid":"1","ts":1762455081495000},{"cat":"PERF","name":"list_files_parallel","ph":"E","pid":"1","ts":1762455082170000},{"cat":"PERF","name":"detect_table_type","ph":"B","pid":"1","ts":1762455082171000},{"cat":"PERF","name":"detect_table_type","ph":"E","pid":"1","ts":1762455082172000},{"cat":"PERF","name":"categorize_files","ph":"B","pid":"1","ts":1762455082173000},{"cat":"PERF","name":"categorize_files","ph":"E","pid":"1","ts":1762455082173000},{"cat":"PERF","name":"find_referenced_files","ph":"B","pid":"1","ts":1762455082174000},{"cat":"PERF","name":"find_referenced_files","ph":"E","pid":"1","ts":1762455082683000},{"cat":"PERF","name":"find_unreferenced_files","ph":"B","pid":"1","ts":1762455082684000},{"cat":"PERF","name":"find_unreferenced_files","ph":"E","pid":"1","ts":1762455082685000},{"cat":"PERF","name":"analyze_partitioning","ph":"B","pid":"1","ts":1762455082687000},{"cat":"PERF","name":"analyze_partitioning","ph":"E","pid":"1","ts":1762455082689000},{"cat":"PERF","name":"update_metrics_from_metadata","ph":"B","pid":"1","ts":1762455082689000},{"cat":"PERF","name":"update_metrics_from_metadata","ph":"E","pid":"1","ts":1762455083173000},{"cat":"PERF","name":"calculate_file_size_distribution","ph":"B","pid":"1","ts":1762455083175000},{"cat":"PERF","name":"calculate_file_size_distribution","ph":"E","pid":"1","ts":1762455083175000},{"cat":"PERF","name":"calculate_metadata_health","ph":"B","pid":"1","ts":1762455083176000},{"cat":"PERF","name":"calculate_metadata_health","ph":"E","pid":"1","ts":1762455083176000},{"cat":"PERF","name":"calculate_data_skew","ph":"B","pid":"1","ts":1762455083177000},{"cat":"PERF","name":"calculate_data_skew","ph":"E","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"calculate_snapshot_health","ph":"B","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"calculate_snapshot_health","ph":"E","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"analyze_file_compaction","ph":"B","pid":"1","ts":1762455083180000},{"cat":"PERF","name":"analyze_file_compaction","ph":"E","pid":"1","ts":1762455083180000},{"cat":"PERF","name":"generate_recommendations","ph":"B","pid":"1","ts":1762455083181000},{"cat":"PERF","name":"generate_recommendations","ph":"E","pid":"1","ts":1762455083181000},{"cat":"PERF","name":"calculate_health_score","ph":"B","pid":"1","ts":1762455083182000},{"cat":"PERF","name":"calculate_health_score","ph":"E","pid":"1","ts":1762455083182000},{"cat":"PERF","name":"analyze_after_validation_dur","ph":"B","pid":"1","ts":1762455081495000},{"cat":"PERF","name":"analyze_after_validation_dur","ph":"E","pid":"1","ts":1762455083183000},{"cat":"PERF","name":"delta_reader","ph":"B","pid":"1","ts":1762455083183000},{"cat":"PERF","name":"delta_reader","ph":"E","pid":"1","ts":1762455085985000},{"cat":"PERF","name":"analyze_total_dur","ph":"B","pid":"1","ts":1762455078972000},{"cat":"PERF","name":"analyze_total_dur","ph":"E","pid":"1","ts":1762455085987000},{"cat":"PERF","name":"total_dur","ph":"B","pid":"1","ts":1762455078689000},{"cat":"PERF","name":"total_dur","ph":"E","pid":"1","ts":1762455085987000}]"#;

        // Parse the Chrome tracing JSON
        let events: Vec<serde_json::Value> =
            serde_json::from_str(chrome_trace_input).expect("Failed to parse Chrome tracing JSON");

        // Extract timing data from the parsed events
        let mut timing_map: HashMap<String, (u128, u128)> = HashMap::new();

        for event in &events {
            let name = event["name"].as_str().expect("Event should have a name");
            let ts = event["ts"].as_u64().expect("Event should have a timestamp") as u128;
            let phase = event["ph"].as_str().expect("Event should have a phase");

            if phase == "B" {
                // Begin event - store start time (in microseconds)
                timing_map.entry(name.to_string()).or_insert((ts, 0)).0 = ts;
            } else if phase == "E" {
                // End event - calculate duration (in microseconds)
                if let Some(entry) = timing_map.get_mut(name) {
                    entry.1 = ts - entry.0;
                }
            }
        }

        // Create TimedLikeMetrics from the parsed data
        // Convert from microseconds to milliseconds for internal storage
        let mut collection = LinkedList::new();
        for (name, (start_ts_us, duration_us)) in timing_map.iter() {
            collection.push_back((
                name.clone(),
                start_ts_us / 1000, // Convert microseconds to milliseconds
                duration_us / 1000, // Convert microseconds to milliseconds
            ));
        }

        let metrics = TimedLikeMetrics {
            duration_collection: collection,
        };

        // Generate Chrome tracing output - this should work without overflow
        let output = metrics
            .to_chrome_tracing()
            .expect("Should generate Chrome tracing output without overflow");

        // Parse the generated output
        let output_events: Vec<serde_json::Value> =
            serde_json::from_str(&output).expect("Should parse generated output");

        // Verify we have the correct number of events
        assert_eq!(
            output_events.len(),
            44,
            "Should have 44 events (22 metrics * 2)"
        );

        // Verify all expected metrics are present
        let metric_names: std::collections::HashSet<String> = output_events
            .iter()
            .filter_map(|e| e["name"].as_str().map(String::from))
            .collect();

        assert!(metric_names.contains("storage_config_new_dur"));
        assert!(metric_names.contains("analyzer_new_dur"));
        assert!(metric_names.contains("validate_connection_dur"));
        assert!(metric_names.contains("total_dur"));
        assert!(metric_names.contains("delta_reader"));
    }

    #[test]
    fn test_chrome_tracing_empty_metrics() {
        let metrics = TimedLikeMetrics {
            duration_collection: LinkedList::new(),
        };

        let output = metrics
            .to_chrome_tracing()
            .expect("Should handle empty metrics");

        let events: Vec<serde_json::Value> =
            serde_json::from_str(&output).expect("Should parse empty array");

        assert_eq!(
            events.len(),
            0,
            "Empty metrics should produce empty events array"
        );
    }

    #[test]
    fn test_chrome_tracing_single_metric() {
        let mut collection = LinkedList::new();
        collection.push_back(("test_metric".to_string(), 1000, 500));

        let metrics = TimedLikeMetrics {
            duration_collection: collection,
        };

        let output = metrics
            .to_chrome_tracing()
            .expect("Should generate output for single metric");

        let events: Vec<serde_json::Value> =
            serde_json::from_str(&output).expect("Should parse output");

        assert_eq!(
            events.len(),
            2,
            "Single metric should produce 2 events (B and E)"
        );

        // Verify Begin event
        assert_eq!(events[0]["name"].as_str(), Some("test_metric"));
        assert_eq!(events[0]["ph"].as_str(), Some("B"));
        assert_eq!(events[0]["ts"].as_u64(), Some(1000000)); // 1000ms * 1000

        // Verify End event
        assert_eq!(events[1]["name"].as_str(), Some("test_metric"));
        assert_eq!(events[1]["ph"].as_str(), Some("E"));
        assert_eq!(events[1]["ts"].as_u64(), Some(1500000)); // (1000 + 500)ms * 1000
    }

    #[test]
    fn test_chrome_tracing_duration_calculation() {
        let mut collection = LinkedList::new();
        // Add metrics with known durations
        collection.push_back(("metric1".to_string(), 0, 100));
        collection.push_back(("metric2".to_string(), 50, 200));
        collection.push_back(("metric3".to_string(), 100, 50));

        let metrics = TimedLikeMetrics {
            duration_collection: collection,
        };

        let output = metrics.to_chrome_tracing().expect("Should generate output");

        let events: Vec<serde_json::Value> =
            serde_json::from_str(&output).expect("Should parse output");

        // Should have 6 events (3 metrics * 2 events each)
        assert_eq!(events.len(), 6);

        // Verify metric1 duration
        let metric1_begin = events
            .iter()
            .find(|e| e["name"] == "metric1" && e["ph"] == "B")
            .expect("Should find metric1 begin");
        let metric1_end = events
            .iter()
            .find(|e| e["name"] == "metric1" && e["ph"] == "E")
            .expect("Should find metric1 end");

        let duration = metric1_end["ts"].as_u64().unwrap() - metric1_begin["ts"].as_u64().unwrap();
        assert_eq!(
            duration, 100000,
            "metric1 duration should be 100ms (100000 microseconds)"
        );
    }

    /// This test documents the proper way to use Chrome tracing data with TimedLikeMetrics
    ///
    /// Chrome tracing format uses microseconds for timestamps, but TimedLikeMetrics
    /// expects milliseconds internally and multiplies by 1000 when generating output.
    ///
    /// IMPORTANT: When parsing Chrome tracing JSON, you MUST divide timestamps by 1000
    /// to convert from microseconds to milliseconds before storing in TimedLikeMetrics.
    #[test]
    fn test_chrome_tracing_format_documentation() {
        // Chrome tracing uses microseconds
        let chrome_ts_microseconds: u128 = 1762455078689000;

        // TimedLikeMetrics expects milliseconds
        let internal_ts_milliseconds: u128 = chrome_ts_microseconds / 1000;

        // When to_chrome_tracing() is called, it multiplies by 1000 to get back to microseconds
        let output_ts_microseconds: u128 = internal_ts_milliseconds * 1000;

        // Verify the round-trip works correctly
        assert_eq!(
            chrome_ts_microseconds, output_ts_microseconds,
            "Chrome tracing timestamps should round-trip correctly"
        );

        println!("Chrome tracing format:");
        println!("  Input (microseconds):    {}", chrome_ts_microseconds);
        println!("  Internal (milliseconds): {}", internal_ts_milliseconds);
        println!("  Output (microseconds):   {}", output_ts_microseconds);
    }

    /// This test verifies that the ASCII Gantt chart bug is FIXED
    /// It uses the ACTUAL Chrome tracing data with absolute timestamps
    /// and should NOT panic after the fix to ascii_gantt.rs
    #[test]
    fn test_chrome_trace_with_absolute_timestamps_fixed() {
        let chrome_trace_input = r#"[{"cat":"PERF","name":"storage_config_new_dur","ph":"B","pid":"1","ts":1762455078689000},{"cat":"PERF","name":"storage_config_new_dur","ph":"E","pid":"1","ts":1762455078690000},{"cat":"PERF","name":"analyzer_new_dur","ph":"B","pid":"1","ts":1762455078691000},{"cat":"PERF","name":"analyzer_new_dur","ph":"E","pid":"1","ts":1762455078972000},{"cat":"PERF","name":"validate_connection_dur","ph":"B","pid":"1","ts":1762455078973000},{"cat":"PERF","name":"validate_connection_dur","ph":"E","pid":"1","ts":1762455080930000},{"cat":"PERF","name":"discover_partitions","ph":"B","pid":"1","ts":1762455080932000},{"cat":"PERF","name":"discover_partitions","ph":"E","pid":"1","ts":1762455081493000},{"cat":"PERF","name":"list_files_parallel","ph":"B","pid":"1","ts":1762455081495000},{"cat":"PERF","name":"list_files_parallel","ph":"E","pid":"1","ts":1762455082170000},{"cat":"PERF","name":"detect_table_type","ph":"B","pid":"1","ts":1762455082171000},{"cat":"PERF","name":"detect_table_type","ph":"E","pid":"1","ts":1762455082172000},{"cat":"PERF","name":"categorize_files","ph":"B","pid":"1","ts":1762455082173000},{"cat":"PERF","name":"categorize_files","ph":"E","pid":"1","ts":1762455082173000},{"cat":"PERF","name":"find_referenced_files","ph":"B","pid":"1","ts":1762455082174000},{"cat":"PERF","name":"find_referenced_files","ph":"E","pid":"1","ts":1762455082683000},{"cat":"PERF","name":"find_unreferenced_files","ph":"B","pid":"1","ts":1762455082684000},{"cat":"PERF","name":"find_unreferenced_files","ph":"E","pid":"1","ts":1762455082685000},{"cat":"PERF","name":"analyze_partitioning","ph":"B","pid":"1","ts":1762455082687000},{"cat":"PERF","name":"analyze_partitioning","ph":"E","pid":"1","ts":1762455082689000},{"cat":"PERF","name":"update_metrics_from_metadata","ph":"B","pid":"1","ts":1762455082689000},{"cat":"PERF","name":"update_metrics_from_metadata","ph":"E","pid":"1","ts":1762455083173000},{"cat":"PERF","name":"calculate_file_size_distribution","ph":"B","pid":"1","ts":1762455083175000},{"cat":"PERF","name":"calculate_file_size_distribution","ph":"E","pid":"1","ts":1762455083175000},{"cat":"PERF","name":"calculate_metadata_health","ph":"B","pid":"1","ts":1762455083176000},{"cat":"PERF","name":"calculate_metadata_health","ph":"E","pid":"1","ts":1762455083176000},{"cat":"PERF","name":"calculate_data_skew","ph":"B","pid":"1","ts":1762455083177000},{"cat":"PERF","name":"calculate_data_skew","ph":"E","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"calculate_snapshot_health","ph":"B","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"calculate_snapshot_health","ph":"E","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"analyze_file_compaction","ph":"B","pid":"1","ts":1762455083180000},{"cat":"PERF","name":"analyze_file_compaction","ph":"E","pid":"1","ts":1762455083180000},{"cat":"PERF","name":"generate_recommendations","ph":"B","pid":"1","ts":1762455083181000},{"cat":"PERF","name":"generate_recommendations","ph":"E","pid":"1","ts":1762455083181000},{"cat":"PERF","name":"calculate_health_score","ph":"B","pid":"1","ts":1762455083182000},{"cat":"PERF","name":"calculate_health_score","ph":"E","pid":"1","ts":1762455083182000},{"cat":"PERF","name":"analyze_after_validation_dur","ph":"B","pid":"1","ts":1762455081495000},{"cat":"PERF","name":"analyze_after_validation_dur","ph":"E","pid":"1","ts":1762455083183000},{"cat":"PERF","name":"delta_reader","ph":"B","pid":"1","ts":1762455083183000},{"cat":"PERF","name":"delta_reader","ph":"E","pid":"1","ts":1762455085985000},{"cat":"PERF","name":"analyze_total_dur","ph":"B","pid":"1","ts":1762455078972000},{"cat":"PERF","name":"analyze_total_dur","ph":"E","pid":"1","ts":1762455085987000},{"cat":"PERF","name":"total_dur","ph":"B","pid":"1","ts":1762455078689000},{"cat":"PERF","name":"total_dur","ph":"E","pid":"1","ts":1762455085987000}]"#;

        // Parse the Chrome tracing JSON
        let events: Vec<serde_json::Value> =
            serde_json::from_str(chrome_trace_input).expect("Failed to parse Chrome tracing JSON");

        // Extract timing data from the parsed events
        let mut timing_map: HashMap<String, (u128, u128)> = HashMap::new();

        for event in &events {
            let name = event["name"].as_str().expect("Event should have a name");
            let ts = event["ts"].as_u64().expect("Event should have a timestamp") as u128;
            let phase = event["ph"].as_str().expect("Event should have a phase");

            if phase == "B" {
                timing_map.entry(name.to_string()).or_insert((ts, 0)).0 = ts;
            } else if phase == "E" {
                if let Some(entry) = timing_map.get_mut(name) {
                    entry.1 = ts - entry.0;
                }
            }
        }

        // Create TimedLikeMetrics with ABSOLUTE timestamps (in milliseconds)
        // This is the scenario that was causing the overflow bug
        let mut collection = LinkedList::new();
        for (name, (start_ts_us, duration_us)) in timing_map.iter() {
            // Convert from microseconds to milliseconds
            // These will be LARGE numbers like 1762455078689
            collection.push_back((name.clone(), start_ts_us / 1000, duration_us / 1000));
        }

        let metrics = TimedLikeMetrics {
            duration_collection: collection,
        };

        // This should NOW work without panic after the fix to ascii_gantt.rs
        let gantt_output = metrics
            .duration_collection_as_gantt(None)
            .expect("Should generate ASCII Gantt chart with absolute timestamps after fix");

        println!("\nASCII Gantt Chart with absolute timestamps:");
        println!("{}", gantt_output);

        // Verify the output contains expected metrics
        assert!(gantt_output.contains("storage_config_new_dur"));
        assert!(gantt_output.contains("analyzer_new_dur"));
        assert!(gantt_output.contains("total_dur"));
        assert!(gantt_output.contains("Timeline"));
    }

    /// This test parses the FULL Chrome tracing data and works correctly
    /// by using RELATIVE timestamps (offset from the minimum) instead of absolute timestamps
    #[test]
    fn test_chrome_trace_with_relative_timestamps() {
        let chrome_trace_input = r#"[{"cat":"PERF","name":"storage_config_new_dur","ph":"B","pid":"1","ts":1762455078689000},{"cat":"PERF","name":"storage_config_new_dur","ph":"E","pid":"1","ts":1762455078690000},{"cat":"PERF","name":"analyzer_new_dur","ph":"B","pid":"1","ts":1762455078691000},{"cat":"PERF","name":"analyzer_new_dur","ph":"E","pid":"1","ts":1762455078972000},{"cat":"PERF","name":"validate_connection_dur","ph":"B","pid":"1","ts":1762455078973000},{"cat":"PERF","name":"validate_connection_dur","ph":"E","pid":"1","ts":1762455080930000},{"cat":"PERF","name":"discover_partitions","ph":"B","pid":"1","ts":1762455080932000},{"cat":"PERF","name":"discover_partitions","ph":"E","pid":"1","ts":1762455081493000},{"cat":"PERF","name":"list_files_parallel","ph":"B","pid":"1","ts":1762455081495000},{"cat":"PERF","name":"list_files_parallel","ph":"E","pid":"1","ts":1762455082170000},{"cat":"PERF","name":"detect_table_type","ph":"B","pid":"1","ts":1762455082171000},{"cat":"PERF","name":"detect_table_type","ph":"E","pid":"1","ts":1762455082172000},{"cat":"PERF","name":"categorize_files","ph":"B","pid":"1","ts":1762455082173000},{"cat":"PERF","name":"categorize_files","ph":"E","pid":"1","ts":1762455082173000},{"cat":"PERF","name":"find_referenced_files","ph":"B","pid":"1","ts":1762455082174000},{"cat":"PERF","name":"find_referenced_files","ph":"E","pid":"1","ts":1762455082683000},{"cat":"PERF","name":"find_unreferenced_files","ph":"B","pid":"1","ts":1762455082684000},{"cat":"PERF","name":"find_unreferenced_files","ph":"E","pid":"1","ts":1762455082685000},{"cat":"PERF","name":"analyze_partitioning","ph":"B","pid":"1","ts":1762455082687000},{"cat":"PERF","name":"analyze_partitioning","ph":"E","pid":"1","ts":1762455082689000},{"cat":"PERF","name":"update_metrics_from_metadata","ph":"B","pid":"1","ts":1762455082689000},{"cat":"PERF","name":"update_metrics_from_metadata","ph":"E","pid":"1","ts":1762455083173000},{"cat":"PERF","name":"calculate_file_size_distribution","ph":"B","pid":"1","ts":1762455083175000},{"cat":"PERF","name":"calculate_file_size_distribution","ph":"E","pid":"1","ts":1762455083175000},{"cat":"PERF","name":"calculate_metadata_health","ph":"B","pid":"1","ts":1762455083176000},{"cat":"PERF","name":"calculate_metadata_health","ph":"E","pid":"1","ts":1762455083176000},{"cat":"PERF","name":"calculate_data_skew","ph":"B","pid":"1","ts":1762455083177000},{"cat":"PERF","name":"calculate_data_skew","ph":"E","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"calculate_snapshot_health","ph":"B","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"calculate_snapshot_health","ph":"E","pid":"1","ts":1762455083179000},{"cat":"PERF","name":"analyze_file_compaction","ph":"B","pid":"1","ts":1762455083180000},{"cat":"PERF","name":"analyze_file_compaction","ph":"E","pid":"1","ts":1762455083180000},{"cat":"PERF","name":"generate_recommendations","ph":"B","pid":"1","ts":1762455083181000},{"cat":"PERF","name":"generate_recommendations","ph":"E","pid":"1","ts":1762455083181000},{"cat":"PERF","name":"calculate_health_score","ph":"B","pid":"1","ts":1762455083182000},{"cat":"PERF","name":"calculate_health_score","ph":"E","pid":"1","ts":1762455083182000},{"cat":"PERF","name":"analyze_after_validation_dur","ph":"B","pid":"1","ts":1762455081495000},{"cat":"PERF","name":"analyze_after_validation_dur","ph":"E","pid":"1","ts":1762455083183000},{"cat":"PERF","name":"delta_reader","ph":"B","pid":"1","ts":1762455083183000},{"cat":"PERF","name":"delta_reader","ph":"E","pid":"1","ts":1762455085985000},{"cat":"PERF","name":"analyze_total_dur","ph":"B","pid":"1","ts":1762455078972000},{"cat":"PERF","name":"analyze_total_dur","ph":"E","pid":"1","ts":1762455085987000},{"cat":"PERF","name":"total_dur","ph":"B","pid":"1","ts":1762455078689000},{"cat":"PERF","name":"total_dur","ph":"E","pid":"1","ts":1762455085987000}]"#;

        // Parse the Chrome tracing JSON
        let events: Vec<serde_json::Value> =
            serde_json::from_str(chrome_trace_input).expect("Failed to parse Chrome tracing JSON");

        // Extract timing data from the parsed events
        let mut timing_map: HashMap<String, (u128, u128)> = HashMap::new();

        for event in &events {
            let name = event["name"].as_str().expect("Event should have a name");
            let ts = event["ts"].as_u64().expect("Event should have a timestamp") as u128;
            let phase = event["ph"].as_str().expect("Event should have a phase");

            if phase == "B" {
                timing_map.entry(name.to_string()).or_insert((ts, 0)).0 = ts;
            } else if phase == "E" {
                if let Some(entry) = timing_map.get_mut(name) {
                    entry.1 = ts - entry.0;
                }
            }
        }

        // Find the minimum timestamp to use as offset
        let min_timestamp = timing_map
            .values()
            .map(|(start, _)| *start)
            .min()
            .unwrap_or(0);

        println!("Min timestamp (microseconds): {}", min_timestamp);
        println!("Min timestamp (milliseconds): {}", min_timestamp / 1000);

        // Create TimedLikeMetrics with RELATIVE timestamps (offset from minimum)
        let mut collection = LinkedList::new();
        for (name, (start_ts_us, duration_us)) in timing_map.iter() {
            // Use relative timestamps: subtract the minimum and convert to milliseconds
            let relative_start_ms = (start_ts_us - min_timestamp) / 1000;
            let duration_ms = duration_us / 1000;

            collection.push_back((name.clone(), relative_start_ms, duration_ms));
        }

        let metrics = TimedLikeMetrics {
            duration_collection: collection,
        };

        // Test Chrome tracing generation - should work fine
        let chrome_output = metrics
            .to_chrome_tracing()
            .expect("Should generate Chrome tracing output");

        let output_events: Vec<serde_json::Value> =
            serde_json::from_str(&chrome_output).expect("Should parse output");

        assert_eq!(output_events.len(), 44, "Should have 44 events");

        // Test ASCII Gantt generation - should work now with relative timestamps
        let gantt_output = metrics
            .duration_collection_as_gantt(None)
            .expect("Should generate ASCII Gantt chart with relative timestamps");

        println!("\nASCII Gantt Chart:");
        println!("{}", gantt_output);

        // Verify the output contains expected metrics
        assert!(gantt_output.contains("storage_config_new_dur"));
        assert!(gantt_output.contains("analyzer_new_dur"));
        assert!(gantt_output.contains("total_dur"));
    }

    // HealthMetrics tests
    #[test]
    fn test_health_metrics_new() {
        let metrics = HealthMetrics::new();

        assert_eq!(metrics.total_files, 0);
        assert_eq!(metrics.total_size_bytes, 0);
        assert_eq!(metrics.partition_count, 0);
        assert_eq!(metrics.avg_file_size_bytes, 0.0);
        assert_eq!(metrics.health_score, 0.0);
        assert_eq!(metrics.file_size_distribution.small_files, 0);
        assert_eq!(metrics.file_size_distribution.medium_files, 0);
        assert_eq!(metrics.file_size_distribution.large_files, 0);
        assert_eq!(metrics.file_size_distribution.very_large_files, 0);
        assert!(metrics.unreferenced_files.is_empty());
        assert!(metrics.partitions.is_empty());
        assert!(metrics.recommendations.is_empty());
        assert!(metrics.clustering.is_none());
        assert!(metrics.deletion_vector_metrics.is_none());
        assert!(metrics.schema_evolution.is_none());
        assert!(metrics.time_travel_metrics.is_none());
        assert!(metrics.table_constraints.is_none());
        assert!(metrics.file_compaction.is_none());
    }

    #[test]
    fn test_health_metrics_default() {
        let metrics = HealthMetrics::default();

        assert_eq!(metrics.total_files, 0);
        assert_eq!(metrics.health_score, 0.0);
    }

    #[test]
    fn test_calculate_health_score_perfect() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution.medium_files = 100; // All medium files

        let score = metrics.calculate_health_score();

        assert!(
            (0.9..=1.0).contains(&score),
            "Perfect health should score high"
        );
    }

    #[test]
    fn test_calculate_health_score_with_unreferenced_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.unreferenced_files = vec![
            FileInfo {
                path: "file1.parquet".to_string(),
                size_bytes: 1024,
                last_modified: None,
                is_referenced: false,
            };
            30
        ]; // 30% unreferenced

        let score = metrics.calculate_health_score();

        // Should be penalized by 30% * 0.3 = 0.09
        assert!(score < 1.0);
        assert!((0.85..=0.95).contains(&score));
    }

    #[test]
    fn test_calculate_health_score_with_small_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution.small_files = 60; // 60% small files

        let score = metrics.calculate_health_score();

        // Should be penalized by 60% * 0.2 = 0.12
        assert!(score < 1.0);
        assert!((0.8..=0.9).contains(&score));
    }

    #[test]
    fn test_calculate_health_score_with_very_large_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution.very_large_files = 20; // 20% very large

        let score = metrics.calculate_health_score();

        // Should be penalized by 20% * 0.1 = 0.02
        assert!((0.95..=1.0).contains(&score));
    }

    #[test]
    fn test_calculate_health_score_with_data_skew() {
        let mut metrics = HealthMetrics::new();
        metrics.data_skew.partition_skew_score = 0.8; // High partition skew
        metrics.data_skew.file_size_skew_score = 0.6; // Moderate file size skew

        let score = metrics.calculate_health_score();

        // Should be penalized by 0.8 * 0.15 + 0.6 * 0.1 = 0.12 + 0.06 = 0.18
        assert!((0.75..=0.85).contains(&score));
    }

    #[test]
    fn test_calculate_health_score_clamped() {
        let mut metrics = HealthMetrics::new();
        // Set extreme values that would result in negative score
        metrics.total_files = 100;
        metrics.unreferenced_files = vec![
            FileInfo {
                path: "file.parquet".to_string(),
                size_bytes: 1024,
                last_modified: None,
                is_referenced: false,
            };
            100
        ];
        metrics.file_size_distribution.small_files = 100;
        metrics.data_skew.partition_skew_score = 1.0;
        metrics.data_skew.file_size_skew_score = 1.0;

        let score = metrics.calculate_health_score();

        // Score should be clamped to [0.0, 1.0]
        assert!((0.0..=1.0).contains(&score));
    }

    #[test]
    fn test_calculate_data_skew_empty_partitions() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_data_skew();

        assert_eq!(metrics.data_skew.partition_skew_score, 0.0);
        assert_eq!(metrics.data_skew.file_size_skew_score, 0.0);
    }

    #[test]
    fn test_calculate_data_skew_balanced() {
        let mut metrics = HealthMetrics::new();
        // Create balanced partitions
        metrics.partitions = vec![
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 10,
                total_size_bytes: 1000,
                avg_file_size_bytes: 100.0,
                files: Vec::new(),
            };
            5
        ];

        metrics.calculate_data_skew();

        // Perfectly balanced should have low skew
        assert!(metrics.data_skew.partition_skew_score < 0.1);
        assert_eq!(metrics.data_skew.largest_partition_size, 1000);
        assert_eq!(metrics.data_skew.smallest_partition_size, 1000);
        assert_eq!(metrics.data_skew.avg_partition_size, 1000);
    }

    #[test]
    fn test_calculate_data_skew_unbalanced() {
        let mut metrics = HealthMetrics::new();
        // Create unbalanced partitions
        metrics.partitions = vec![
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 100,
                total_size_bytes: 10000,
                avg_file_size_bytes: 100.0,
                files: Vec::new(),
            },
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 10,
                total_size_bytes: 1000,
                avg_file_size_bytes: 100.0,
                files: Vec::new(),
            },
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 5,
                total_size_bytes: 500,
                avg_file_size_bytes: 100.0,
                files: Vec::new(),
            },
        ];

        metrics.calculate_data_skew();

        // Unbalanced should have higher skew
        assert!(metrics.data_skew.partition_skew_score > 0.3);
        assert_eq!(metrics.data_skew.largest_partition_size, 10000);
        assert_eq!(metrics.data_skew.smallest_partition_size, 500);
    }

    #[test]
    fn test_calculate_snapshot_health_low_count() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_snapshot_health(10);

        assert_eq!(metrics.snapshot_health.snapshot_count, 10);
        assert_eq!(metrics.snapshot_health.snapshot_retention_risk, 0.0);
    }

    #[test]
    fn test_calculate_snapshot_health_medium_count() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_snapshot_health(30);

        assert_eq!(metrics.snapshot_health.snapshot_count, 30);
        assert_eq!(metrics.snapshot_health.snapshot_retention_risk, 0.2);
    }

    #[test]
    fn test_calculate_snapshot_health_high_count() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_snapshot_health(75);

        assert_eq!(metrics.snapshot_health.snapshot_count, 75);
        assert_eq!(metrics.snapshot_health.snapshot_retention_risk, 0.5);
    }

    #[test]
    fn test_calculate_snapshot_health_very_high_count() {
        let mut metrics = HealthMetrics::new();

        metrics.calculate_snapshot_health(150);

        assert_eq!(metrics.snapshot_health.snapshot_count, 150);
        assert_eq!(metrics.snapshot_health.snapshot_retention_risk, 0.8);
    }

    #[test]
    fn test_generate_recommendations_unreferenced_files() {
        let mut metrics = HealthMetrics::new();
        metrics.unreferenced_files = vec![
            FileInfo {
                path: "file1.parquet".to_string(),
                size_bytes: 1024,
                last_modified: None,
                is_referenced: false,
            },
            FileInfo {
                path: "file2.parquet".to_string(),
                size_bytes: 2048,
                last_modified: None,
                is_referenced: false,
            },
        ];
        metrics.unreferenced_size_bytes = 3072;

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics.recommendations[0].contains("unreferenced files"));
        assert!(metrics.recommendations[0].contains("3.00 KB")); // 3072 bytes = 3.00 KB
    }

    #[test]
    fn test_generate_recommendations_small_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution.small_files = 60; // 60% small files

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("small files") && r.contains("compacting")));
    }

    #[test]
    fn test_generate_recommendations_very_large_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.file_size_distribution.very_large_files = 15; // 15% very large

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("very large files") && r.contains("splitting")));
    }

    #[test]
    fn test_generate_recommendations_too_many_files_per_partition() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 1000;
        metrics.partition_count = 5; // 200 files per partition

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("files per partition") && r.contains("repartitioning")));
    }

    #[test]
    fn test_generate_recommendations_too_few_files_per_partition() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 10;
        metrics.partition_count = 5; // 2 files per partition

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Low number of files") && r.contains("consolidating")));
    }

    #[test]
    fn test_generate_recommendations_empty_partitions() {
        let mut metrics = HealthMetrics::new();
        metrics.partitions = vec![
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 0,
                total_size_bytes: 0,
                avg_file_size_bytes: 0.0,
                files: Vec::new(),
            },
            PartitionInfo {
                partition_values: HashMap::new(),
                file_count: 10,
                total_size_bytes: 1000,
                avg_file_size_bytes: 100.0,
                files: Vec::new(),
            },
        ];

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("empty partitions")));
    }

    #[test]
    fn test_generate_recommendations_high_partition_skew() {
        let mut metrics = HealthMetrics::new();
        metrics.data_skew.partition_skew_score = 0.8;

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("partition skew") && r.contains("repartitioning")));
    }

    #[test]
    fn test_generate_recommendations_high_file_size_skew() {
        let mut metrics = HealthMetrics::new();
        metrics.data_skew.file_size_skew_score = 0.7;

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("file size skew") && r.contains("OPTIMIZE")));
    }

    #[test]
    fn test_generate_recommendations_large_metadata() {
        let mut metrics = HealthMetrics::new();
        metrics.metadata_health.metadata_total_size_bytes = 60 * 1024 * 1024; // 60MB

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("metadata size") && r.contains("VACUUM")));
    }

    #[test]
    fn test_generate_recommendations_high_snapshot_retention_risk() {
        let mut metrics = HealthMetrics::new();
        metrics.snapshot_health.snapshot_retention_risk = 0.9;

        metrics.generate_recommendations();

        assert!(!metrics.recommendations.is_empty());
        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("snapshot retention risk") && r.contains("VACUUM")));
    }

    #[test]
    fn test_generate_recommendations_clustering_high_files_per_cluster() {
        let mut metrics = HealthMetrics::new();
        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: vec!["col1".to_string()],
            cluster_count: 5,
            avg_files_per_cluster: 60.0, // > 50
            avg_cluster_size_bytes: 1024.0 * 1024.0,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("files per cluster") && r.contains("optimizing")));
    }

    #[test]
    fn test_generate_recommendations_clustering_too_many_columns() {
        let mut metrics = HealthMetrics::new();
        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: vec![
                "col1".to_string(),
                "col2".to_string(),
                "col3".to_string(),
                "col4".to_string(),
                "col5".to_string(),
            ],
            cluster_count: 10,
            avg_files_per_cluster: 5.0,
            avg_cluster_size_bytes: 1024.0 * 1024.0,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Too many clustering columns")));
    }

    #[test]
    fn test_generate_recommendations_clustering_no_columns() {
        let mut metrics = HealthMetrics::new();
        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: vec![],
            cluster_count: 0,
            avg_files_per_cluster: 0.0,
            avg_cluster_size_bytes: 0.0,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("No clustering detected")));
    }

    #[test]
    fn test_generate_recommendations_deletion_vectors_high_impact() {
        let mut metrics = HealthMetrics::new();
        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: 10,
            total_deletion_vector_size_bytes: 1024,
            avg_deletion_vector_size_bytes: 102.4,
            deletion_vector_age_days: 5.0,
            deleted_rows_count: 100,
            deletion_vector_impact_score: 0.8, // > 0.7
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("deletion vector impact") && r.contains("VACUUM")));
    }

    #[test]
    fn test_generate_recommendations_deletion_vectors_many_vectors() {
        let mut metrics = HealthMetrics::new();
        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: 60, // > 50
            total_deletion_vector_size_bytes: 1024,
            avg_deletion_vector_size_bytes: 17.0,
            deletion_vector_age_days: 5.0,
            deleted_rows_count: 100,
            deletion_vector_impact_score: 0.3,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Many deletion vectors")));
    }

    #[test]
    fn test_generate_recommendations_deletion_vectors_old() {
        let mut metrics = HealthMetrics::new();
        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: 5,
            total_deletion_vector_size_bytes: 1024,
            avg_deletion_vector_size_bytes: 204.8,
            deletion_vector_age_days: 45.0, // > 30
            deleted_rows_count: 100,
            deletion_vector_impact_score: 0.3,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Old deletion vectors")));
    }

    #[test]
    fn test_generate_recommendations_schema_unstable() {
        let mut metrics = HealthMetrics::new();
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: 10,
            breaking_changes: 2,
            non_breaking_changes: 8,
            schema_stability_score: 0.3, // < 0.5
            days_since_last_change: 30.0,
            schema_change_frequency: 0.5,
            current_schema_version: 10,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Unstable schema")));
    }

    #[test]
    fn test_generate_recommendations_schema_many_breaking_changes() {
        let mut metrics = HealthMetrics::new();
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: 10,
            breaking_changes: 7, // > 5
            non_breaking_changes: 3,
            schema_stability_score: 0.6,
            days_since_last_change: 30.0,
            schema_change_frequency: 0.5,
            current_schema_version: 10,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("breaking schema changes")));
    }

    #[test]
    fn test_generate_recommendations_schema_high_frequency() {
        let mut metrics = HealthMetrics::new();
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: 10,
            breaking_changes: 1,
            non_breaking_changes: 9,
            schema_stability_score: 0.7,
            days_since_last_change: 30.0,
            schema_change_frequency: 1.5, // > 1.0
            current_schema_version: 10,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("schema change frequency")));
    }

    #[test]
    fn test_generate_recommendations_schema_recent_change() {
        let mut metrics = HealthMetrics::new();
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: 5,
            breaking_changes: 0,
            non_breaking_changes: 5,
            schema_stability_score: 0.9,
            days_since_last_change: 0.5, // < 1.0
            schema_change_frequency: 0.1,
            current_schema_version: 5,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Recent schema changes")));
    }

    #[test]
    fn test_generate_recommendations_time_travel_high_cost() {
        let mut metrics = HealthMetrics::new();
        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots: 50,
            oldest_snapshot_age_days: 90.0,
            newest_snapshot_age_days: 0.5,
            total_historical_size_bytes: 1024 * 1024 * 1024,
            avg_snapshot_size_bytes: 20.0 * 1024.0 * 1024.0,
            storage_cost_impact_score: 0.8, // > 0.7
            retention_efficiency_score: 0.6,
            recommended_retention_days: 30,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("time travel storage costs")));
    }

    #[test]
    fn test_generate_recommendations_time_travel_inefficient_retention() {
        let mut metrics = HealthMetrics::new();
        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots: 50,
            oldest_snapshot_age_days: 90.0,
            newest_snapshot_age_days: 0.5,
            total_historical_size_bytes: 1024 * 1024 * 1024,
            avg_snapshot_size_bytes: 20.0 * 1024.0 * 1024.0,
            storage_cost_impact_score: 0.3,
            retention_efficiency_score: 0.3, // < 0.5
            recommended_retention_days: 30,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Inefficient snapshot retention")));
    }

    #[test]
    fn test_generate_recommendations_time_travel_high_snapshot_count() {
        let mut metrics = HealthMetrics::new();
        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots: 1500, // > 1000
            oldest_snapshot_age_days: 365.0,
            newest_snapshot_age_days: 0.1,
            total_historical_size_bytes: 10 * 1024 * 1024 * 1024,
            avg_snapshot_size_bytes: 6.7 * 1024.0 * 1024.0,
            storage_cost_impact_score: 0.5,
            retention_efficiency_score: 0.6,
            recommended_retention_days: 30,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("High snapshot count")));
    }

    #[test]
    fn test_generate_recommendations_constraints_low_quality() {
        let mut metrics = HealthMetrics::new();
        metrics.table_constraints = Some(TableConstraintsMetrics {
            total_constraints: 5,
            check_constraints: 1,
            not_null_constraints: 3,
            unique_constraints: 1,
            foreign_key_constraints: 0,
            constraint_violation_risk: 0.2,
            data_quality_score: 0.3, // < 0.5
            constraint_coverage_score: 0.5,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Low data quality score")));
    }

    #[test]
    fn test_generate_recommendations_constraints_high_violation_risk() {
        let mut metrics = HealthMetrics::new();
        metrics.table_constraints = Some(TableConstraintsMetrics {
            total_constraints: 5,
            check_constraints: 1,
            not_null_constraints: 3,
            unique_constraints: 1,
            foreign_key_constraints: 0,
            constraint_violation_risk: 0.8, // > 0.7
            data_quality_score: 0.7,
            constraint_coverage_score: 0.5,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("constraint violation risk")));
    }

    #[test]
    fn test_generate_recommendations_constraints_low_coverage() {
        let mut metrics = HealthMetrics::new();
        metrics.table_constraints = Some(TableConstraintsMetrics {
            total_constraints: 2,
            check_constraints: 0,
            not_null_constraints: 2,
            unique_constraints: 0,
            foreign_key_constraints: 0,
            constraint_violation_risk: 0.2,
            data_quality_score: 0.7,
            constraint_coverage_score: 0.2, // < 0.3
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Low constraint coverage")));
    }

    #[test]
    fn test_generate_recommendations_compaction_high_opportunity() {
        let mut metrics = HealthMetrics::new();
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.8, // > 0.7
            small_files_count: 100,
            small_files_size_bytes: 50 * 1024 * 1024,
            potential_compaction_files: 80,
            estimated_compaction_savings_bytes: 20 * 1024 * 1024,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "high".to_string(),
            z_order_opportunity: false,
            z_order_columns: vec![],
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("file compaction opportunity")));
    }

    #[test]
    fn test_generate_recommendations_compaction_critical_priority() {
        let mut metrics = HealthMetrics::new();
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.5,
            small_files_count: 200,
            small_files_size_bytes: 100 * 1024 * 1024,
            potential_compaction_files: 180,
            estimated_compaction_savings_bytes: 50 * 1024 * 1024,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "critical".to_string(),
            z_order_opportunity: false,
            z_order_columns: vec![],
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Critical compaction priority")));
    }

    #[test]
    fn test_generate_recommendations_compaction_z_order_opportunity() {
        let mut metrics = HealthMetrics::new();
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.5,
            small_files_count: 50,
            small_files_size_bytes: 25 * 1024 * 1024,
            potential_compaction_files: 40,
            estimated_compaction_savings_bytes: 10 * 1024 * 1024,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "medium".to_string(),
            z_order_opportunity: true,
            z_order_columns: vec!["date".to_string(), "region".to_string()],
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Z-ordering opportunity") && r.contains("date, region")));
    }

    #[test]
    fn test_generate_recommendations_compaction_significant_savings() {
        let mut metrics = HealthMetrics::new();
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.5,
            small_files_count: 500,
            small_files_size_bytes: 500 * 1024 * 1024,
            potential_compaction_files: 400,
            estimated_compaction_savings_bytes: 200 * 1024 * 1024, // > 100MB
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "medium".to_string(),
            z_order_opportunity: false,
            z_order_columns: vec![],
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Significant compaction savings")));
    }

    // HealthReport tests
    #[test]
    fn test_health_report_to_json() {
        let report = HealthReport {
            table_path: "/path/to/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics: HealthMetrics::new(),
            health_score: 0.85,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let json = report.to_json(false).expect("Should serialize to JSON");

        assert!(json.contains("table_path"));
        assert!(json.contains("/path/to/table"));
        assert!(json.contains("delta"));
        assert!(json.contains("0.85"));
    }

    #[test]
    fn test_health_report_to_json_exclude_files() {
        let mut metrics = HealthMetrics::new();
        metrics.unreferenced_files = vec![FileInfo {
            path: "file1.parquet".to_string(),
            size_bytes: 1024,
            last_modified: None,
            is_referenced: false,
        }];
        metrics.partitions = vec![PartitionInfo {
            partition_values: HashMap::new(),
            file_count: 1,
            total_size_bytes: 1024,
            avg_file_size_bytes: 1024.0,
            files: vec![FileInfo {
                path: "file2.parquet".to_string(),
                size_bytes: 1024,
                last_modified: None,
                is_referenced: true,
            }],
        }];

        let report = HealthReport {
            table_path: "/path/to/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.85,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let json = report.to_json(true).expect("Should serialize to JSON");

        // Should not contain file paths when exclude_files is true
        assert!(!json.contains("file1.parquet"));
        assert!(!json.contains("file2.parquet"));
    }

    #[test]
    fn test_health_report_display() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.total_size_bytes = 1024 * 1024 * 1024; // 1GB
        metrics.avg_file_size_bytes = 10.0 * 1024.0 * 1024.0; // 10MB
        metrics.partition_count = 5;
        metrics.file_size_distribution.small_files = 20;
        metrics.file_size_distribution.medium_files = 60;
        metrics.file_size_distribution.large_files = 15;
        metrics.file_size_distribution.very_large_files = 5;

        let report = HealthReport {
            table_path: "/path/to/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.85,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);

        assert!(display.contains("Table Health Report"));
        assert!(display.contains("85.0%"));
        assert!(display.contains("/path/to/table"));
        assert!(display.contains("delta"));
        assert!(display.contains("Total Data Files"));
        assert!(display.contains("100"));
    }

    // FileInfo tests
    #[test]
    fn test_file_info_creation() {
        let file_info = FileInfo {
            path: "data/file.parquet".to_string(),
            size_bytes: 1024 * 1024,
            last_modified: Some("2024-01-01T00:00:00Z".to_string()),
            is_referenced: true,
        };

        assert_eq!(file_info.path, "data/file.parquet");
        assert_eq!(file_info.size_bytes, 1024 * 1024);
        assert_eq!(
            file_info.last_modified,
            Some("2024-01-01T00:00:00Z".to_string())
        );
        assert!(file_info.is_referenced);
    }

    #[test]
    fn test_file_info_serialization() {
        let file_info = FileInfo {
            path: "data/file.parquet".to_string(),
            size_bytes: 1024,
            last_modified: None,
            is_referenced: false,
        };

        let json = serde_json::to_string(&file_info).expect("Should serialize");

        assert!(json.contains("data/file.parquet"));
        assert!(json.contains("1024"));
        assert!(json.contains("false"));
    }

    // PartitionInfo tests
    #[test]
    fn test_partition_info_creation() {
        let mut partition_values = HashMap::new();
        partition_values.insert("year".to_string(), "2024".to_string());
        partition_values.insert("month".to_string(), "01".to_string());

        let partition_info = PartitionInfo {
            partition_values,
            file_count: 10,
            total_size_bytes: 10240,
            avg_file_size_bytes: 1024.0,
            files: Vec::new(),
        };

        assert_eq!(partition_info.file_count, 10);
        assert_eq!(partition_info.total_size_bytes, 10240);
        assert_eq!(partition_info.avg_file_size_bytes, 1024.0);
        assert_eq!(partition_info.partition_values.len(), 2);
    }

    // ClusteringInfo tests
    #[test]
    fn test_clustering_info_creation() {
        let clustering_info = ClusteringInfo {
            clustering_columns: vec!["col1".to_string(), "col2".to_string()],
            cluster_count: 10,
            avg_files_per_cluster: 5.5,
            avg_cluster_size_bytes: 1024.0 * 1024.0,
        };

        assert_eq!(clustering_info.clustering_columns.len(), 2);
        assert_eq!(clustering_info.cluster_count, 10);
        assert_eq!(clustering_info.avg_files_per_cluster, 5.5);
    }

    // DeletionVectorMetrics tests
    #[test]
    fn test_deletion_vector_metrics_creation() {
        let dv_metrics = DeletionVectorMetrics {
            deletion_vector_count: 5,
            total_deletion_vector_size_bytes: 5120,
            avg_deletion_vector_size_bytes: 1024.0,
            deletion_vector_age_days: 15.5,
            deleted_rows_count: 1000,
            deletion_vector_impact_score: 0.6,
        };

        assert_eq!(dv_metrics.deletion_vector_count, 5);
        assert_eq!(dv_metrics.total_deletion_vector_size_bytes, 5120);
        assert_eq!(dv_metrics.deleted_rows_count, 1000);
        assert_eq!(dv_metrics.deletion_vector_impact_score, 0.6);
    }

    // SchemaEvolutionMetrics tests
    #[test]
    fn test_schema_evolution_metrics_creation() {
        let schema_metrics = SchemaEvolutionMetrics {
            total_schema_changes: 10,
            breaking_changes: 2,
            non_breaking_changes: 8,
            schema_stability_score: 0.8,
            days_since_last_change: 5.0,
            schema_change_frequency: 0.5,
            current_schema_version: 11,
        };

        assert_eq!(schema_metrics.total_schema_changes, 10);
        assert_eq!(schema_metrics.breaking_changes, 2);
        assert_eq!(schema_metrics.non_breaking_changes, 8);
        assert_eq!(schema_metrics.schema_stability_score, 0.8);
    }

    // TimeTravelMetrics tests
    #[test]
    fn test_time_travel_metrics_creation() {
        let tt_metrics = TimeTravelMetrics {
            total_snapshots: 50,
            oldest_snapshot_age_days: 30.0,
            newest_snapshot_age_days: 1.0,
            total_historical_size_bytes: 1024 * 1024 * 1024,
            avg_snapshot_size_bytes: 20.0 * 1024.0 * 1024.0,
            storage_cost_impact_score: 0.5,
            retention_efficiency_score: 0.7,
            recommended_retention_days: 30,
        };

        assert_eq!(tt_metrics.total_snapshots, 50);
        assert_eq!(tt_metrics.oldest_snapshot_age_days, 30.0);
        assert_eq!(tt_metrics.recommended_retention_days, 30);
    }

    // TableConstraintsMetrics tests
    #[test]
    fn test_table_constraints_metrics_creation() {
        let constraint_metrics = TableConstraintsMetrics {
            total_constraints: 10,
            check_constraints: 3,
            not_null_constraints: 5,
            unique_constraints: 1,
            foreign_key_constraints: 1,
            constraint_violation_risk: 0.2,
            data_quality_score: 0.9,
            constraint_coverage_score: 0.8,
        };

        assert_eq!(constraint_metrics.total_constraints, 10);
        assert_eq!(constraint_metrics.check_constraints, 3);
        assert_eq!(constraint_metrics.not_null_constraints, 5);
        assert_eq!(constraint_metrics.data_quality_score, 0.9);
    }

    // FileCompactionMetrics tests
    #[test]
    fn test_file_compaction_metrics_creation() {
        let compaction_metrics = FileCompactionMetrics {
            compaction_opportunity_score: 0.8,
            small_files_count: 100,
            small_files_size_bytes: 1024 * 1024,
            potential_compaction_files: 80,
            estimated_compaction_savings_bytes: 512 * 1024,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "high".to_string(),
            z_order_opportunity: true,
            z_order_columns: vec!["col1".to_string(), "col2".to_string()],
        };

        assert_eq!(compaction_metrics.compaction_opportunity_score, 0.8);
        assert_eq!(compaction_metrics.small_files_count, 100);
        assert_eq!(compaction_metrics.compaction_priority, "high");
        assert!(compaction_metrics.z_order_opportunity);
        assert_eq!(compaction_metrics.z_order_columns.len(), 2);
    }

    // ========== Display implementation coverage tests ==========

    #[test]
    fn test_health_report_display_with_clustering() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 50;
        metrics.total_size_bytes = 500 * 1024 * 1024;
        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: vec!["date".to_string(), "region".to_string()],
            cluster_count: 10,
            avg_files_per_cluster: 5.0,
            avg_cluster_size_bytes: 50.0 * 1024.0 * 1024.0,
        });

        let report = HealthReport {
            table_path: "/test/table".to_string(),
            table_type: "iceberg".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.9,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("Clustering"));
        assert!(display.contains("Avg Cluster Size"));
        assert!(display.contains("Clusters"));
        assert!(display.contains("10"));
    }

    #[test]
    fn test_health_report_display_with_deletion_vectors() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: 25,
            total_deletion_vector_size_bytes: 2 * 1024 * 1024,
            avg_deletion_vector_size_bytes: 80.0 * 1024.0,
            deletion_vector_age_days: 15.5,
            deleted_rows_count: 50000,
            deletion_vector_impact_score: 0.65,
        });

        let report = HealthReport {
            table_path: "/test/delta_table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.75,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("Deletion Vectors"));
        assert!(display.contains("Vectors"));
        assert!(display.contains("25"));
        assert!(display.contains("Deleted Rows"));
        assert!(display.contains("50000"));
    }

    #[test]
    fn test_health_report_display_with_schema_evolution() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: 5,
            breaking_changes: 1,
            non_breaking_changes: 4,
            schema_stability_score: 0.8,
            days_since_last_change: 30.0,
            schema_change_frequency: 0.1,
            current_schema_version: 5,
        });

        let report = HealthReport {
            table_path: "/test/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.85,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("Schema Evolution"));
        assert!(display.contains("Total Changes"));
        assert!(display.contains("Breaking"));
        assert!(display.contains("Non-Breaking"));
        assert!(display.contains("Stability"));
    }

    #[test]
    fn test_health_report_display_with_time_travel() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots: 50,
            oldest_snapshot_age_days: 90.0,
            newest_snapshot_age_days: 0.5,
            total_historical_size_bytes: 2 * 1024 * 1024 * 1024,
            avg_snapshot_size_bytes: 40.0 * 1024.0 * 1024.0,
            storage_cost_impact_score: 0.3,
            retention_efficiency_score: 0.85,
            recommended_retention_days: 30,
        });

        let report = HealthReport {
            table_path: "/test/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.8,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("Time Travel"));
        assert!(display.contains("Snapshots"));
        assert!(display.contains("50"));
        assert!(display.contains("Historical Size"));
    }

    #[test]
    fn test_health_report_display_with_constraints_and_compaction() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.table_constraints = Some(TableConstraintsMetrics {
            total_constraints: 10,
            check_constraints: 3,
            not_null_constraints: 5,
            unique_constraints: 1,
            foreign_key_constraints: 1,
            constraint_violation_risk: 0.1,
            data_quality_score: 0.95,
            constraint_coverage_score: 0.7,
        });
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.6,
            small_files_count: 40,
            small_files_size_bytes: 100 * 1024 * 1024,
            potential_compaction_files: 35,
            estimated_compaction_savings_bytes: 50 * 1024 * 1024,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "MEDIUM".to_string(),
            z_order_opportunity: false,
            z_order_columns: vec![],
        });

        let report = HealthReport {
            table_path: "/test/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.7,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("Table Constraints"));
        assert!(display.contains("File Compaction"));
        assert!(display.contains("NOT NULL"));
        assert!(display.contains("Small Files"));
        assert!(display.contains("40"));
    }

    #[test]
    fn test_health_report_display_with_unreferenced_files() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.unreferenced_files = vec![
            FileInfo {
                path: "orphan1.parquet".to_string(),
                size_bytes: 1024 * 1024,
                last_modified: None,
                is_referenced: false,
            },
            FileInfo {
                path: "orphan2.parquet".to_string(),
                size_bytes: 2 * 1024 * 1024,
                last_modified: None,
                is_referenced: false,
            },
        ];
        metrics.unreferenced_size_bytes = 3 * 1024 * 1024;

        let report = HealthReport {
            table_path: "/test/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.6,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("Unreferenced Files"));
        assert!(display.contains("Count"));
        assert!(display.contains("2"));
        assert!(display.contains("Wasted Space"));
    }

    #[test]
    fn test_health_report_display_with_all_optional_fields() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 200;
        metrics.total_size_bytes = 5 * 1024 * 1024 * 1024;
        metrics.avg_file_size_bytes = 25.0 * 1024.0 * 1024.0;
        metrics.partition_count = 20;

        // Add all optional fields
        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: vec!["col1".to_string()],
            cluster_count: 5,
            avg_files_per_cluster: 40.0,
            avg_cluster_size_bytes: 1024.0 * 1024.0 * 1024.0,
        });
        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: 10,
            total_deletion_vector_size_bytes: 500 * 1024,
            avg_deletion_vector_size_bytes: 50.0 * 1024.0,
            deletion_vector_age_days: 5.0,
            deleted_rows_count: 1000,
            deletion_vector_impact_score: 0.2,
        });
        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: 3,
            breaking_changes: 0,
            non_breaking_changes: 3,
            schema_stability_score: 0.95,
            days_since_last_change: 60.0,
            schema_change_frequency: 0.05,
            current_schema_version: 3,
        });
        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots: 100,
            oldest_snapshot_age_days: 180.0,
            newest_snapshot_age_days: 0.1,
            total_historical_size_bytes: 10 * 1024 * 1024 * 1024,
            avg_snapshot_size_bytes: 100.0 * 1024.0 * 1024.0,
            storage_cost_impact_score: 0.5,
            retention_efficiency_score: 0.7,
            recommended_retention_days: 90,
        });
        metrics.table_constraints = Some(TableConstraintsMetrics {
            total_constraints: 15,
            check_constraints: 5,
            not_null_constraints: 8,
            unique_constraints: 1,
            foreign_key_constraints: 1,
            constraint_violation_risk: 0.05,
            data_quality_score: 0.98,
            constraint_coverage_score: 0.85,
        });
        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.3,
            small_files_count: 20,
            small_files_size_bytes: 50 * 1024 * 1024,
            potential_compaction_files: 15,
            estimated_compaction_savings_bytes: 20 * 1024 * 1024,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: "LOW".to_string(),
            z_order_opportunity: true,
            z_order_columns: vec!["date".to_string()],
        });

        let report = HealthReport {
            table_path: "/production/data/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-06-15T12:00:00Z".to_string(),
            metrics,
            health_score: 0.92,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);

        // Verify all sections are present
        assert!(display.contains("Table Health Report"));
        assert!(display.contains("92.0%"));
        assert!(display.contains("Key Metrics"));
        assert!(display.contains("File Size Distribution"));
        assert!(display.contains("Data Skew Analysis"));
        assert!(display.contains("Metadata Health"));
        assert!(display.contains("Snapshot Health"));
        assert!(display.contains("Clustering"));
        assert!(display.contains("Deletion Vectors"));
        assert!(display.contains("Schema Evolution"));
        assert!(display.contains("Time Travel"));
        assert!(display.contains("Table Constraints"));
        assert!(display.contains("File Compaction"));
    }

    #[test]
    fn test_health_report_display_size_formatting_gb() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 1000;
        metrics.total_size_bytes = 50 * 1024 * 1024 * 1024; // 50 GB

        let report = HealthReport {
            table_path: "/test/large_table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.8,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("GB"));
    }

    #[test]
    fn test_health_report_display_metadata_file_names_truncation() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.metadata_health.first_file_name =
            Some("very_long_metadata_file_name_that_exceeds_thirty_characters.json".to_string());
        metrics.metadata_health.last_file_name =
            Some("another_very_long_file_name_for_testing_truncation.json".to_string());

        let report = HealthReport {
            table_path: "/test/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.8,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        // Truncated names should contain "..."
        assert!(display.contains("..."));
    }

    #[test]
    fn test_health_report_display_zero_partitions() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 10;
        metrics.partition_count = 0;
        metrics.data_skew.avg_partition_size = 0;

        let report = HealthReport {
            table_path: "/test/unpartitioned".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.7,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("N/A"));
    }

    #[test]
    fn test_health_report_display_with_recommendations() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.recommendations = vec![
            "Consider running OPTIMIZE".to_string(),
            "Too many small files detected".to_string(),
        ];

        let report = HealthReport {
            table_path: "/test/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.5,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("Recommendations"));
        assert!(display.contains("Consider running OPTIMIZE"));
        assert!(display.contains("Too many small files detected"));
    }

    #[test]
    fn test_health_report_display_snapshot_ages() {
        let mut metrics = HealthMetrics::new();
        metrics.total_files = 50;
        metrics.snapshot_health.oldest_snapshot_age_days = 45.5;
        metrics.snapshot_health.newest_snapshot_age_days = 0.5;
        metrics.snapshot_health.avg_snapshot_age_days = 20.0;

        let report = HealthReport {
            table_path: "/test/table".to_string(),
            table_type: "delta".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.8,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);
        assert!(display.contains("Oldest Snapshot"));
        assert!(display.contains("45.5 days"));
        assert!(display.contains("Newest Snapshot"));
        assert!(display.contains("0.5 days"));
    }

    #[cfg(feature = "hudi")]
    #[test]
    fn test_health_report_display_with_hudi_metrics() {
        use crate::reader::hudi::metrics::{
            FileStatistics, HudiMetrics, PartitionMetrics, TableMetadata, TimelineMetrics,
        };

        let mut metrics = HealthMetrics::new();
        metrics.total_files = 100;
        metrics.hudi_table_specific_metrics = Some(HudiMetrics {
            table_type: "COPY_ON_WRITE".to_string(),
            table_name: "test_hudi_table".to_string(),
            metadata: TableMetadata {
                name: "test_hudi_table".to_string(),
                base_path: "/data/hudi/test_table".to_string(),
                schema_string: "{}".to_string(),
                field_count: 10,
                partition_columns: vec!["date".to_string()],
                created_time: Some(1700000000000),
                format_provider: "parquet".to_string(),
                format_options: std::collections::HashMap::new(),
            },
            table_properties: {
                let mut props = std::collections::HashMap::new();
                props.insert("hoodie.table.name".to_string(), "test".to_string());
                props
            },
            file_stats: FileStatistics {
                num_files: 50,
                total_size_bytes: 1024 * 1024 * 100,
                avg_file_size_bytes: 1024.0 * 1024.0 * 2.0,
                min_file_size_bytes: 1024,
                max_file_size_bytes: 1024 * 1024 * 10,
                num_log_files: 10,
                total_log_size_bytes: 1024 * 1024 * 20,
            },
            partition_info: PartitionMetrics {
                num_partition_columns: 1,
                num_partitions: 30,
                partition_paths: vec!["date=2024-01-01".to_string()],
                largest_partition_size_bytes: 1024 * 1024 * 50,
                smallest_partition_size_bytes: 1024 * 1024,
                avg_partition_size_bytes: 1024.0 * 1024.0 * 25.0,
            },
            timeline_info: TimelineMetrics {
                total_commits: 100,
                total_delta_commits: 0,
                total_compactions: 5,
                total_cleans: 10,
                total_rollbacks: 2,
                total_savepoints: 1,
                latest_commit_timestamp: Some("20240101120000000".to_string()),
                earliest_commit_timestamp: Some("20231201000000000".to_string()),
                pending_compactions: 0,
            },
        });

        let report = HealthReport {
            table_path: "/data/hudi/test_table".to_string(),
            table_type: "hudi".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.85,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);

        // Verify Hudi-specific metrics are displayed
        assert!(display.contains("Hudi Specific Metrics"));
        assert!(display.contains("COPY_ON_WRITE"));
        assert!(display.contains("test_hudi_table"));
        assert!(display.contains("File Statistics"));
        assert!(display.contains("Timeline Info"));
        assert!(display.contains("Total Commits"));
    }

    #[cfg(feature = "lance")]
    #[test]
    fn test_generate_recommendations_lance_no_indices_large_table() {
        use crate::reader::lance::metrics::{
            FileStatistics, FragmentMetrics, IndexMetrics, LanceMetrics, TableMetadata,
        };

        let mut metrics = HealthMetrics::new();
        metrics.lance_table_specific_metrics = Some(LanceMetrics {
            version: 1,
            metadata: TableMetadata {
                uuid: "test-uuid".to_string(),
                schema_string: String::new(),
                field_count: 5,
                created_time: None,
                last_modified_time: None,
                num_rows: Some(50_000), // > 10,000 rows
                num_deleted_rows: None,
            },
            table_properties: std::collections::HashMap::new(),
            file_stats: FileStatistics {
                num_data_files: 10,
                num_deletion_files: 0,
                total_data_size_bytes: 1024 * 1024 * 100,
                total_deletion_size_bytes: 0,
                avg_data_file_size_bytes: 1024.0 * 1024.0 * 10.0,
                min_data_file_size_bytes: 1024 * 1024,
                max_data_file_size_bytes: 1024 * 1024 * 20,
            },
            fragment_info: FragmentMetrics {
                num_fragments: 10,
                num_fragments_with_deletions: 0,
                avg_rows_per_fragment: 5000.0,
                min_rows_per_fragment: 1000,
                max_rows_per_fragment: 10000,
                total_physical_rows: 50000,
            },
            index_info: IndexMetrics {
                num_indices: 0, // No indices
                indexed_columns: vec![],
                index_types: vec![],
                total_index_size_bytes: 0,
            },
            operation_metrics: None,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("No indices found on Lance table")));
    }

    #[cfg(feature = "lance")]
    #[test]
    fn test_generate_recommendations_lance_many_fragments_no_indices() {
        use crate::reader::lance::metrics::{
            FileStatistics, FragmentMetrics, IndexMetrics, LanceMetrics, TableMetadata,
        };

        let mut metrics = HealthMetrics::new();
        metrics.lance_table_specific_metrics = Some(LanceMetrics {
            version: 1,
            metadata: TableMetadata {
                uuid: "test-uuid".to_string(),
                schema_string: String::new(),
                field_count: 5,
                created_time: None,
                last_modified_time: None,
                num_rows: Some(5_000), // Small table
                num_deleted_rows: None,
            },
            table_properties: std::collections::HashMap::new(),
            file_stats: FileStatistics {
                num_data_files: 150,
                num_deletion_files: 0,
                total_data_size_bytes: 1024 * 1024 * 100,
                total_deletion_size_bytes: 0,
                avg_data_file_size_bytes: 1024.0 * 1024.0,
                min_data_file_size_bytes: 1024,
                max_data_file_size_bytes: 1024 * 1024 * 5,
            },
            fragment_info: FragmentMetrics {
                num_fragments: 150, // > 100 fragments
                num_fragments_with_deletions: 0,
                avg_rows_per_fragment: 33.0,
                min_rows_per_fragment: 10,
                max_rows_per_fragment: 100,
                total_physical_rows: 5000,
            },
            index_info: IndexMetrics {
                num_indices: 0, // No indices
                indexed_columns: vec![],
                index_types: vec![],
                total_index_size_bytes: 0,
            },
            operation_metrics: None,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("Large number of fragments")));
    }

    #[cfg(feature = "lance")]
    #[test]
    fn test_generate_recommendations_lance_high_deletion_ratio_with_indices() {
        use crate::reader::lance::metrics::{
            FileStatistics, FragmentMetrics, IndexMetrics, LanceMetrics, TableMetadata,
        };

        let mut metrics = HealthMetrics::new();
        metrics.lance_table_specific_metrics = Some(LanceMetrics {
            version: 1,
            metadata: TableMetadata {
                uuid: "test-uuid".to_string(),
                schema_string: String::new(),
                field_count: 5,
                created_time: None,
                last_modified_time: None,
                num_rows: Some(8_000),
                num_deleted_rows: Some(3_000), // 3000 / (8000 + 3000) = 27% > 20%
            },
            table_properties: std::collections::HashMap::new(),
            file_stats: FileStatistics {
                num_data_files: 10,
                num_deletion_files: 5,
                total_data_size_bytes: 1024 * 1024 * 100,
                total_deletion_size_bytes: 1024 * 100,
                avg_data_file_size_bytes: 1024.0 * 1024.0 * 10.0,
                min_data_file_size_bytes: 1024 * 1024,
                max_data_file_size_bytes: 1024 * 1024 * 20,
            },
            fragment_info: FragmentMetrics {
                num_fragments: 10,
                num_fragments_with_deletions: 5,
                avg_rows_per_fragment: 800.0,
                min_rows_per_fragment: 500,
                max_rows_per_fragment: 1500,
                total_physical_rows: 11000,
            },
            index_info: IndexMetrics {
                num_indices: 2, // Has indices
                indexed_columns: vec!["id".to_string(), "embedding".to_string()],
                index_types: vec!["IVF_PQ".to_string()],
                total_index_size_bytes: 1024 * 1024 * 10,
            },
            operation_metrics: None,
        });

        metrics.generate_recommendations();

        assert!(metrics
            .recommendations
            .iter()
            .any(|r| r.contains("High deletion ratio") && r.contains("rebuilding indices")));
    }

    #[cfg(feature = "lance")]
    #[test]
    fn test_health_report_display_with_lance_metrics() {
        use crate::reader::lance::metrics::{
            FileStatistics, FragmentMetrics, IndexMetrics, LanceMetrics, TableMetadata,
        };

        let mut metrics = HealthMetrics::new();
        metrics.lance_table_specific_metrics = Some(LanceMetrics {
            version: 5,
            metadata: TableMetadata {
                uuid: "test-lance-uuid".to_string(),
                schema_string: String::new(),
                field_count: 10,
                created_time: None,
                last_modified_time: None,
                num_rows: Some(100_000),
                num_deleted_rows: Some(500),
            },
            table_properties: std::collections::HashMap::new(),
            file_stats: FileStatistics {
                num_data_files: 50,
                num_deletion_files: 5,
                total_data_size_bytes: 1024 * 1024 * 500,
                total_deletion_size_bytes: 1024 * 100,
                avg_data_file_size_bytes: 1024.0 * 1024.0 * 10.0,
                min_data_file_size_bytes: 1024 * 1024,
                max_data_file_size_bytes: 1024 * 1024 * 50,
            },
            fragment_info: FragmentMetrics {
                num_fragments: 50,
                num_fragments_with_deletions: 5,
                avg_rows_per_fragment: 2000.0,
                min_rows_per_fragment: 500,
                max_rows_per_fragment: 5000,
                total_physical_rows: 100500,
            },
            index_info: IndexMetrics {
                num_indices: 2,
                indexed_columns: vec!["id".to_string(), "embedding".to_string()],
                index_types: vec!["IVF_PQ".to_string()],
                total_index_size_bytes: 1024 * 1024 * 50,
            },
            operation_metrics: None,
        });

        let report = HealthReport {
            table_path: "/data/lance/test_table.lance".to_string(),
            table_type: "lance".to_string(),
            analysis_timestamp: "2024-01-01T00:00:00Z".to_string(),
            metrics,
            health_score: 0.90,
            timed_metrics: TimedLikeMetrics {
                duration_collection: LinkedList::new(),
            },
        };

        let display = format!("{}", report);

        // Verify Lance-specific metrics are displayed
        assert!(display.contains("Lance Specific Metrics"));
        assert!(display.contains("test-lance-uuid"));
        assert!(display.contains("File Statistics"));
        assert!(display.contains("Fragment Info"));
        assert!(display.contains("Index Info"));
    }
}
