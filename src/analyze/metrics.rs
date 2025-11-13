use crate::reader::delta::metrics::DeltaMetrics;
use crate::reader::hudi::metrics::HudiMetrics;
use crate::reader::iceberg::metrics::IcebergMetrics;
use crate::util::ascii_gantt::to_ascii_gantt;
use crate::util::ascii_gantt::GanttConfig;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Error as JsonError};
use std::collections::{HashMap, LinkedList};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub last_modified: Option<String>,
    pub is_referenced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_values: HashMap<String, String>,
    pub file_count: usize,
    pub total_size_bytes: u64,
    pub avg_file_size_bytes: f64,
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusteringInfo {
    pub clustering_columns: Vec<String>,
    pub cluster_count: usize,
    pub avg_files_per_cluster: f64,
    pub avg_cluster_size_bytes: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionVectorMetrics {
    pub deletion_vector_count: usize,
    pub total_deletion_vector_size_bytes: u64,
    pub avg_deletion_vector_size_bytes: f64,
    pub deletion_vector_age_days: f64,
    pub deleted_rows_count: u64,
    pub deletion_vector_impact_score: f64, // 0.0 = no impact, 1.0 = high impact
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolutionMetrics {
    pub total_schema_changes: usize,
    pub breaking_changes: usize,
    pub non_breaking_changes: usize,
    pub schema_stability_score: f64, // 0.0 = unstable, 1.0 = very stable
    pub days_since_last_change: f64,
    pub schema_change_frequency: f64, // changes per day
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMetrics {
    pub total_files: usize,
    pub total_size_bytes: u64,
    pub unreferenced_files: Vec<FileInfo>,
    pub unreferenced_size_bytes: u64,
    pub partition_count: usize,
    pub partitions: Vec<PartitionInfo>,
    pub clustering: Option<ClusteringInfo>,
    pub avg_file_size_bytes: f64,
    pub file_size_distribution: FileSizeDistribution,
    pub recommendations: Vec<String>,
    pub health_score: f64,
    pub data_skew: DataSkewMetrics,
    pub metadata_health: MetadataHealth,
    pub snapshot_health: SnapshotHealth,
    pub deletion_vector_metrics: Option<DeletionVectorMetrics>,
    pub schema_evolution: Option<SchemaEvolutionMetrics>,
    pub time_travel_metrics: Option<TimeTravelMetrics>,
    pub table_constraints: Option<TableConstraintsMetrics>,
    pub file_compaction: Option<FileCompactionMetrics>,
    pub delta_table_specific_metrics: Option<DeltaMetrics>,
    pub hudi_table_specific_metrics: Option<HudiMetrics>,
    pub iceberg_table_specific_metrics: Option<IcebergMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSizeDistribution {
    pub small_files: usize,      // < 16MB
    pub medium_files: usize,     // 16MB - 128MB
    pub large_files: usize,      // 128MB - 1GB
    pub very_large_files: usize, // > 1GB
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSkewMetrics {
    pub partition_skew_score: f64, // 0.0 (perfect) to 1.0 (highly skewed)
    pub file_size_skew_score: f64, // 0.0 (perfect) to 1.0 (highly skewed)
    pub largest_partition_size: u64,
    pub smallest_partition_size: u64,
    pub avg_partition_size: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub table_path: String,
    pub table_type: String, // "delta" or "iceberg"
    pub analysis_timestamp: String,
    pub metrics: HealthMetrics,
    pub health_score: f64, // 0.0 to 1.0
    pub timed_metrics: TimedLikeMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimedLikeMetrics {
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
        writeln!(f, " {:<41} {}", "Key Metrics", "File Size Distribution")?;
        writeln!(f, "{}", "━".repeat(80))?;

        let dist = &report.metrics.file_size_distribution;
        let total_files =
            (dist.small_files + dist.medium_files + dist.large_files + dist.very_large_files)
                as f64;

        let size_gb = report.metrics.total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let size_str = if size_gb >= 1.0 {
            format!("{:.2} GB", size_gb)
        } else {
            let size_mb = report.metrics.total_size_bytes as f64 / (1024.0 * 1024.0);
            format!("{:.2} MB", size_mb)
        };

        let avg_mb = report.metrics.avg_file_size_bytes / (1024.0 * 1024.0);

        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}  {:>5.1}%",
            "Total Files",
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
            "Total Size",
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
            format!("{:.2} MB", avg_mb),
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
        writeln!(f, " {:<41} {}", "Data Skew Analysis", "Metadata Health")?;
        writeln!(f, "{}", "━".repeat(80))?;

        let skew = &report.metrics.data_skew;
        let meta = &report.metrics.metadata_health;

        let largest_mb = skew.largest_partition_size as f64 / (1024.0 * 1024.0);
        let smallest_mb = skew.smallest_partition_size as f64 / (1024.0 * 1024.0);
        let avg_partition_mb = skew.avg_partition_size as f64 / (1024.0 * 1024.0);
        let meta_size_mb = meta.metadata_total_size_bytes as f64 / (1024.0 * 1024.0);
        let avg_meta_mb = meta.avg_metadata_file_size / (1024.0 * 1024.0);

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
            format!("{:.2} MB", meta_size_mb)
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "Largest Partition",
            if skew.avg_partition_size > 0 {
                format!("{:.2} MB", largest_mb)
            } else {
                "N/A".to_string()
            },
            "Avg Size",
            if meta.metadata_file_count > 0 {
                format!("{:.2} MB", avg_meta_mb)
            } else {
                "N/A".to_string()
            }
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "Smallest Partition",
            if skew.avg_partition_size > 0 {
                format!("{:.2} MB", smallest_mb)
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
            " {:<19} {:>8}              {:<19} {:>8}",
            "Avg Partition Size",
            if skew.avg_partition_size > 0 {
                format!("{:.2} MB", avg_partition_mb)
            } else {
                "N/A".to_string()
            },
            "First File",
            first_file_display
        )?;
        writeln!(
            f,
            " {:<19} {:>8}              {:<19} {:>8}",
            "", "", "Last File", last_file_display
        )?;

        // Snapshot Health and Unreferenced Files (side by side)
        writeln!(f)?;
        writeln!(f, " {:<41} {}", "Snapshot Health", "Unreferenced Files")?;
        writeln!(f, "{}", "━".repeat(80))?;

        let snap = &report.metrics.snapshot_health;
        let has_unreferenced = !report.metrics.unreferenced_files.is_empty();
        let wasted_gb = report.metrics.unreferenced_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let wasted_str = if wasted_gb >= 1.0 {
            format!("{:.2} GB", wasted_gb)
        } else {
            let wasted_mb = report.metrics.unreferenced_size_bytes as f64 / (1024.0 * 1024.0);
            format!("{:.2} MB", wasted_mb)
        };

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
            if has_unreferenced {
                wasted_str
            } else {
                "0 MB".to_string()
            }
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
        let has_clustering = report.metrics.clustering.is_some()
            && !report
                .metrics
                .clustering
                .as_ref()
                .unwrap()
                .clustering_columns
                .is_empty();
        let has_deletion_vectors = report.metrics.deletion_vector_metrics.is_some();

        if has_clustering || has_deletion_vectors {
            writeln!(f)?;
            writeln!(
                f,
                " {:<41} {}",
                if has_clustering {
                    "Clustering (Iceberg)"
                } else {
                    ""
                },
                if has_deletion_vectors {
                    "Deletion Vectors (Delta)"
                } else {
                    ""
                }
            )?;
            writeln!(f, "{}", "━".repeat(80))?;

            let max_rows = if has_clustering && has_deletion_vectors {
                4
            } else if has_clustering {
                4
            } else {
                5
            };

            for i in 0..max_rows {
                let left = if has_clustering {
                    if let Some(ref clustering) = report.metrics.clustering {
                        match i {
                            0 => format!(
                                " {:<19} {:>8}",
                                "Columns",
                                clustering.clustering_columns.join(", ")
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
                            3 => format!(
                                " {:<19} {:>8}",
                                "Avg Cluster Size",
                                format!(
                                    "{:.2} MB",
                                    clustering.avg_cluster_size_bytes / (1024.0 * 1024.0)
                                )
                            ),
                            _ => format!("{:<40}", ""),
                        }
                    } else {
                        format!("{:<40}", "")
                    }
                } else {
                    format!("{:<40}", "")
                };

                let right = if let Some(ref dv_metrics) = report.metrics.deletion_vector_metrics {
                    let dv_size_mb =
                        dv_metrics.total_deletion_vector_size_bytes as f64 / (1024.0 * 1024.0);
                    let dv_size_str = if dv_size_mb >= 1.0 {
                        format!("{:.2} MB", dv_size_mb)
                    } else {
                        let dv_size_kb =
                            dv_metrics.total_deletion_vector_size_bytes as f64 / 1024.0;
                        format!("{:.2} KB", dv_size_kb)
                    };

                    match i {
                        0 => format!(
                            " {:<19} {:>8}",
                            "Vectors",
                            format!("{}", dv_metrics.deletion_vector_count)
                        ),
                        1 => format!(" {:<20} {:>8}", "DV Size", dv_size_str),
                        2 => format!(
                            " {:<19} {:>8}",
                            "Deleted Rows",
                            format!("{}", dv_metrics.deleted_rows_count)
                        ),
                        3 => format!(
                            " {:<19} {:>8}",
                            "Oldest DV Age",
                            format!("{:.1} days", dv_metrics.deletion_vector_age_days)
                        ),
                        4 => format!(
                            " {:<19} {:>8}",
                            "Impact",
                            format!("{:.2} (0-1)", dv_metrics.deletion_vector_impact_score)
                        ),
                        _ => format!("{:<40}", ""),
                    }
                } else {
                    format!("{:<40}", "")
                };

                if has_clustering && has_deletion_vectors {
                    writeln!(f, "{}              {}", left, right)?;
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
                    let historical_gb =
                        tt_metrics.total_historical_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                    let historical_str = if historical_gb >= 1.0 {
                        format!("{:.2} GB", historical_gb)
                    } else {
                        let historical_mb =
                            tt_metrics.total_historical_size_bytes as f64 / (1024.0 * 1024.0);
                        format!("{:.2} MB", historical_mb)
                    };

                    match i {
                        0 => format!(
                            " {:<19} {:>8}",
                            "Snapshots",
                            format!("{}", tt_metrics.total_snapshots)
                        ),
                        1 => format!(
                            " {:<19} {:>8}",
                            "Oldest",
                            format!("{:.1} days", tt_metrics.oldest_snapshot_age_days)
                        ),
                        2 => format!(
                            " {:<19} {:>8}",
                            "Newest",
                            format!("{:.1} days", tt_metrics.newest_snapshot_age_days)
                        ),
                        3 => format!(" {:<20} {:>7}", "Historical Size", historical_str),
                        4 => format!(
                            " {:<19} {:>8}",
                            "Cost Impact",
                            format!("{:.2} (0-1)", tt_metrics.storage_cost_impact_score)
                        ),
                        5 => format!(
                            " {:<19} {:>8}",
                            "Retention Eff",
                            format!("{:.2} (0-1)", tt_metrics.retention_efficiency_score)
                        ),
                        6 => format!(
                            " {:<19} {:>8}",
                            "Recommended",
                            format!("{} days", tt_metrics.recommended_retention_days)
                        ),
                        _ => format!("{:<40}", ""),
                    }
                } else {
                    format!("{:<40}", "")
                };

                if has_schema && has_time_travel {
                    writeln!(f, "{}              {}", left, right)?;
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
                            " {:<20} {:>6}",
                            "Total",
                            format!("{}", constraint_metrics.total_constraints)
                        ),
                        1 => format!(
                            " {:<20} {:>6}",
                            "Check",
                            format!("{}", constraint_metrics.check_constraints)
                        ),
                        2 => format!(
                            " {:<20} {:>6}",
                            "NOT NULL",
                            format!("{}", constraint_metrics.not_null_constraints)
                        ),
                        3 => format!(
                            " {:<20} {:>6}",
                            "Unique",
                            format!("{}", constraint_metrics.unique_constraints)
                        ),
                        4 => format!(
                            " {:<20} {:>6}",
                            "Foreign Key",
                            format!("{}", constraint_metrics.foreign_key_constraints)
                        ),
                        5 => format!(
                            " {:<20} {:>6}",
                            "Violation Risk (0-1)",
                            format!("{:.2}", constraint_metrics.constraint_violation_risk)
                        ),
                        6 => format!(
                            " {:<20} {:>6}",
                            "Quality Score (0-1)",
                            format!("{:.2}", constraint_metrics.data_quality_score)
                        ),
                        7 => format!(
                            " {:<20} {:>6}",
                            "Coverage (0-1)",
                            format!("{:.2}", constraint_metrics.constraint_coverage_score)
                        ),
                        _ => format!("{:<40}", ""),
                    }
                } else {
                    format!("{:<40}", "")
                };

                let right = if let Some(ref compaction_metrics) = report.metrics.file_compaction {
                    let small_files_mb =
                        compaction_metrics.small_files_size_bytes as f64 / (1024.0 * 1024.0);
                    let savings_mb = compaction_metrics.estimated_compaction_savings_bytes as f64
                        / (1024.0 * 1024.0);
                    let savings_str = if savings_mb >= 1.0 {
                        format!("{:.2} MB", savings_mb)
                    } else {
                        let savings_kb =
                            compaction_metrics.estimated_compaction_savings_bytes as f64 / 1024.0;
                        format!("{:.2} KB", savings_kb)
                    };
                    let target_mb = compaction_metrics.recommended_target_file_size_bytes as f64
                        / (1024.0 * 1024.0);

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
                        2 => format!(
                            " {:<19} {:>8}",
                            "Small Size",
                            format!("{:.2} MB", small_files_mb)
                        ),
                        3 => format!(
                            " {:<19} {:>8}",
                            "Potential",
                            format!("{} files", compaction_metrics.potential_compaction_files)
                        ),
                        4 => format!(" {:<19} {:>8}", "Savings", savings_str),
                        5 => format!(
                            " {:<19} {:>8}",
                            "Target Size",
                            format!("{:.0} MB", target_mb)
                        ),
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
                                format!(
                                    " {:<19} {:>8}",
                                    "Z-Columns",
                                    compaction_metrics.z_order_columns.join(", ")
                                )
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
                    writeln!(f, "{}              {}", left, right)?;
                } else if has_constraints {
                    writeln!(f, "{}", left)?;
                } else {
                    writeln!(f, "{:<40}  {}", "", right)?;
                }
            }
        }

        // Delta Specific Metrics (full width)
        if let Some(ref delta_metrics) = report.metrics.delta_table_specific_metrics {
            writeln!(f)?;
            writeln!(f, " Delta Specific Metrics")?;
            writeln!(f, "{}", "━".repeat(80))?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Version",
                format!("{}", delta_metrics.version)
            )?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Min Reader Version",
                format!("{}", delta_metrics.protocol.min_reader_version)
            )?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Min Writer Version",
                format!("{}", delta_metrics.protocol.min_writer_version)
            )?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Reader Features",
                format!("{:?}", delta_metrics.protocol.reader_features)
            )?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Writer Features",
                format!("{:?}", delta_metrics.protocol.writer_features)
            )?;
            writeln!(
                f,
                " {:<31}  {:>40}",
                "Table ID",
                format!("{}", delta_metrics.metadata.id)
            )?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Table Name",
                format!("{:?}", delta_metrics.metadata.name)
            )?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Table Description",
                format!("{:?}", delta_metrics.metadata.description)
            )?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Field Count",
                format!("{}", delta_metrics.metadata.field_count)
            )?;
            writeln!(
                f,
                " {:<40} {:>32}",
                "Partition Columns",
                format!("{:?}", delta_metrics.metadata.partition_columns)
            )?;
            if let Some(created_time) = delta_metrics.metadata.created_time {
                let created_datetime = DateTime::from_timestamp(created_time / 1000, 0).unwrap();
                writeln!(
                    f,
                    " {:<40} {:>32}",
                    "Created Time",
                    created_datetime.to_rfc3339()
                )?;
            }
            let tbl_props = delta_metrics.table_properties.clone();
            writeln!(
                f,
                " {:<40} {:>32}",
                "Table Properties",
                if !tbl_props.is_empty() {
                    tbl_props
                        .iter()
                        .map(|(k, v)| format!("{}: {}", k, v))
                        .collect::<Vec<String>>()
                        .join(", ")
                } else {
                    "None".to_string()
                }
            )?;
            // File Statistics - formatted nicely
            let file_stats = &delta_metrics.file_stats;
            let total_size_mb = file_stats.total_size_bytes as f64 / (1024.0 * 1024.0);
            let avg_size_mb = file_stats.avg_file_size_bytes / (1024.0 * 1024.0);
            let min_size_kb = file_stats.min_file_size_bytes as f64 / 1024.0;
            let max_size_mb = file_stats.max_file_size_bytes as f64 / (1024.0 * 1024.0);

            writeln!(f, " File Statistics")?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Files",
                format!("{}", file_stats.num_files)
            )?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Total Size",
                format!("{:.2} MB", total_size_mb)
            )?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Avg Size",
                format!("{:.2} MB", avg_size_mb)
            )?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Min Size",
                format!("{:.2} KB", min_size_kb)
            )?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Max Size",
                format!("{:.2} MB", max_size_mb)
            )?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Files with Deletion Vectors",
                format!("{}", file_stats.files_with_deletion_vectors)
            )?;

            // Partition Info - formatted nicely
            let partition_info = &delta_metrics.partition_info;
            writeln!(f, " Partition Info")?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Partition Columns",
                format!("{}", partition_info.num_partition_columns)
            )?;
            if !partition_info.partition_columns.is_empty() {
                writeln!(
                    f,
                    "   {:<38} {:>32}",
                    "Column Names",
                    partition_info.partition_columns.join(", ")
                )?;
            }
            if let Some(count) = partition_info.estimated_partition_count {
                writeln!(
                    f,
                    "   {:<38} {:>32}",
                    "Estimated Partitions",
                    format!("{}", count)
                )?;
            }
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
            hudi_table_specific_metrics: None,
            iceberg_table_specific_metrics: None,
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

    pub fn calculate_snapshot_health(&mut self, snapshot_count: usize) {
        self.snapshot_health.snapshot_count = snapshot_count;

        // Simplified snapshot age calculation (would need actual timestamps)
        self.snapshot_health.oldest_snapshot_age_days = 0.0;
        self.snapshot_health.newest_snapshot_age_days = 0.0;
        self.snapshot_health.avg_snapshot_age_days = 0.0;

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
                "Found {} unreferenced files ({} bytes). Consider cleaning up orphaned data files.",
                self.unreferenced_files.len(),
                self.unreferenced_size_bytes
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
                let savings_mb = compaction_metrics.estimated_compaction_savings_bytes as f64
                    / (1024.0 * 1024.0);
                self.recommendations.push(
                    format!("Significant compaction savings available: {:.1} MB. Consider running OPTIMIZE.", savings_mb).to_string()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
