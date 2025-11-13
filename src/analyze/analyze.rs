use crate::analyze::delta::DeltaAnalyzer;
use crate::analyze::hudi::HudiAnalyzer;
use crate::analyze::iceberg::IcebergAnalyzer;
use crate::analyze::lance::analyze::LanceAnalyzer;
use crate::analyze::metrics::{
    FileCompactionMetrics, FileInfo, HealthMetrics, HealthReport, PartitionInfo, TimedLikeMetrics,
};
use crate::analyze::table_analyzer::TableAnalyzer;
use crate::reader::delta::reader::DeltaReader;
use crate::reader::hudi::reader::HudiReader;
use crate::reader::iceberg::reader::IcebergReader;
use crate::storage::{FileMetadata, StorageConfig, StorageProvider, StorageProviderFactory};
use crate::util::util::{
    detect_table_type, measure_dur, measure_dur_async, measure_dur_with_error,
};
use std::collections::{HashMap, HashSet, LinkedList};
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// Builder for constructing an `Analyzer` instance.
///
/// This builder provides a fluent API for configuring and creating an `Analyzer`.
/// The builder pattern allows for easy extension with additional configuration options
/// in the future.
///
/// # Examples
///
/// ```no_run
/// use lake_pulse::analyze::Analyzer;
/// use lake_pulse::storage::StorageConfig;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let storage_config = StorageConfig::local()
///     .with_option("path", "/path/to/data");
///
/// // Simple case with defaults
/// let analyzer = Analyzer::builder(storage_config.clone())
///     .build()
///     .await?;
///
/// // With custom parallelism
/// let analyzer = Analyzer::builder(storage_config)
///     .with_parallelism(10)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct AnalyzerBuilder {
    config: StorageConfig,
    parallelism: Option<usize>,
}

impl AnalyzerBuilder {
    /// Creates a new `AnalyzerBuilder` with the given storage configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration to use for the analyzer
    ///
    /// # Returns
    ///
    /// A new `AnalyzerBuilder` instance with default parallelism settings.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lake_pulse::analyze::analyze::AnalyzerBuilder;
    /// use lake_pulse::storage::StorageConfig;
    ///
    /// let config = StorageConfig::local().with_option("path", "/data");
    /// let builder = AnalyzerBuilder::new(config);
    /// ```
    pub fn new(config: StorageConfig) -> Self {
        Self {
            config,
            parallelism: None,
        }
    }

    /// Sets the desired parallelism for metadata file processing.
    ///
    /// # Arguments
    ///
    /// * `parallelism` - The desired level of parallelism (number of concurrent tasks)
    ///
    /// # Returns
    ///
    /// The `AnalyzerBuilder` instance with updated parallelism setting (for method chaining).
    ///
    /// ```no_run
    /// use lake_pulse::analyze::analyze::AnalyzerBuilder;
    /// use lake_pulse::storage::StorageConfig;
    ///
    /// let config = StorageConfig::local().with_option("path", "/data");
    /// let builder = AnalyzerBuilder::new(config).with_parallelism(10);
    /// ```

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = Some(parallelism);
        self
    }

    /// Builds the `Analyzer` instance.
    ///
    /// This method performs the async initialization of the storage provider
    /// and constructs the final `Analyzer` instance.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Analyzer)` - A fully initialized analyzer ready to analyze tables
    /// * `Err(Box<dyn Error + Send + Sync>)` - If the analyzer cannot be built
    ///
    /// # Errors
    ///
    /// Returns an error if the storage provider cannot be initialized.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lake_pulse::analyze::Analyzer;
    /// use lake_pulse::storage::StorageConfig;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let config = StorageConfig::local().with_option("path", "/data");
    /// let analyzer = Analyzer::builder(config)
    ///     .with_parallelism(5)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> Result<Analyzer, Box<dyn Error + Send + Sync>> {
        let storage_provider = StorageProviderFactory::from_config(self.config).await?;
        Ok(Analyzer {
            storage_provider,
            parallelism: self.parallelism.unwrap_or(1),
        })
    }
}

/// Main analyzer for data lake table health analysis.
///
/// The `Analyzer` provides comprehensive health analysis for data lake tables
/// across multiple formats (Delta Lake, Apache Iceberg, Apache Hudi, Lance) and
/// storage providers (AWS S3, Azure Data Lake, GCS, Local filesystem).
///
/// # Features
///
/// - Automatic table format detection
/// - Parallel file processing for improved performance
/// - Comprehensive health metrics including:
///   - File size distribution
///   - Partition analysis
///   - Data skew detection
///   - Unreferenced file detection
///   - Schema evolution tracking
///   - Time travel metrics
///   - Deletion vector analysis
///   - Compaction opportunities
///
/// # Examples
///
/// ## Basic Usage
///
/// ```no_run
/// use lake_pulse::{Analyzer, StorageConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// // Configure storage for local filesystem
/// let config = StorageConfig::local()
///     .with_option("path", "./examples/data");
///
/// // Create analyzer
/// let analyzer = Analyzer::builder(config)
///     .build()
///     .await?;
///
/// // Analyze a table (auto-detects format)
/// let report = analyzer.analyze("delta_dataset").await?;
/// println!("{}", report);
/// # Ok(())
/// # }
/// ```
///
/// ## Using Parallelism for Large Tables
///
/// ```no_run
/// use lake_pulse::{Analyzer, StorageConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let config = StorageConfig::aws()
///     .with_option("bucket", "my-bucket")
///     .with_option("region", "us-east-1");
///
/// // Use parallelism for faster processing of large tables
/// let analyzer = Analyzer::builder(config)
///     .with_parallelism(10)
///     .build()
///     .await?;
///
/// let report = analyzer.analyze("large-table").await?;
/// # Ok(())
/// # }
/// ```
///
/// For complete examples, see the [`examples/`](https://github.com/aionescu_adobe/lake-pulse/tree/main/examples) directory.
pub struct Analyzer {
    storage_provider: Arc<dyn StorageProvider>,
    parallelism: usize,
}

impl Analyzer {
    /// Creates a new `AnalyzerBuilder` for constructing an `Analyzer` instance.
    ///
    /// This is the preferred way to create an `Analyzer`. Use the builder pattern
    /// to configure the analyzer before calling `.build().await`.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration to use
    ///
    /// # Returns
    ///
    /// A new `AnalyzerBuilder` instance for configuring and building an analyzer.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lake_pulse::analyze::Analyzer;
    /// use lake_pulse::storage::StorageConfig;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let config = StorageConfig::local().with_option("path", "/data");
    /// let analyzer = Analyzer::builder(config)
    ///     .with_parallelism(10)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(config: StorageConfig) -> AnalyzerBuilder {
        AnalyzerBuilder::new(config)
    }

    /// Analyze a table at the given location and generate a comprehensive health report.
    ///
    /// This method performs a complete analysis of the table including:
    /// - File listing and categorization
    /// - Metadata processing
    /// - Referenced file discovery
    /// - Unreferenced file detection
    /// - Table-specific metrics extraction (Delta, Iceberg, Hudi, Lance)
    /// - Health metrics calculation
    ///
    /// # Arguments
    ///
    /// * `location` - The path to the table relative to the storage provider's base path
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(HealthReport)` - A comprehensive health report with metrics and recommendations
    /// * `Err(Box<dyn Error + Send + Sync>)` - If the analysis fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The storage connection cannot be validated
    /// * Partitions cannot be discovered
    /// * Files cannot be listed
    /// * Metadata files cannot be processed
    /// * Table-specific readers fail to open or extract metrics
    pub async fn analyze(
        &self,
        location: &str,
    ) -> Result<HealthReport, Box<dyn Error + Send + Sync>> {
        let mut internal_metrics: LinkedList<(&str, SystemTime, Duration)> = LinkedList::new();
        let mut metrics = HealthMetrics::new();

        info!(
            "Analyzing, base_path={}/{}",
            self.storage_provider.base_path(),
            location
        );

        measure_dur_async(
            "validate_connection_dur",
            &mut internal_metrics,
            || async { self.storage_provider.validate_connection(location).await },
            Some(|_| "Successfully validated connection".to_string()),
        )
        .await?;

        let partitions = measure_dur_async(
            "discover_partitions",
            &mut internal_metrics,
            || async {
                self.storage_provider
                    .discover_partitions(location, vec![])
                    .await
            },
            Some(|c: &Vec<String>| format!("Discovered partitions count={}", c.len())),
        )
        .await?;

        let list_files_start = SystemTime::now();
        let all_objects = measure_dur_async(
            "list_files_parallel",
            &mut internal_metrics,
            || async {
                self.storage_provider
                    .list_files_parallel(location, partitions.clone(), self.parallelism)
                    .await
            },
            Some(|c: &Vec<FileMetadata>| format!("Listed files count={}", c.len())),
        )
        .await?;

        // Detect table type and instantiate appropriate analyzer
        let table_type: String = measure_dur(
            "detect_table_type",
            &mut internal_metrics,
            || detect_table_type(&all_objects),
            Some(|t: String| format!("Detected table type={}", t)),
        )
        .await;

        // Instantiate the appropriate table analyzer based on detected type
        let table_analyzer: Arc<dyn TableAnalyzer> = match table_type.as_str() {
            "delta" => Arc::new(DeltaAnalyzer::new(
                Arc::clone(&self.storage_provider),
                self.parallelism,
            )),
            "hudi" => Arc::new(HudiAnalyzer::new(
                Arc::clone(&self.storage_provider),
                self.parallelism,
            )),
            "iceberg" => Arc::new(IcebergAnalyzer::new(
                Arc::clone(&self.storage_provider),
                self.parallelism,
            )),
            "lance" => Arc::new(LanceAnalyzer::new(
                Arc::clone(&self.storage_provider),
                self.parallelism,
            )),
            _ => {
                return Err(format!("Unknown or unsupported table type={}", table_type).into());
            }
        };

        let (data_files, metadata_files) = measure_dur(
            "categorize_files",
            &mut internal_metrics,
            || table_analyzer.categorize_files(all_objects.clone()),
            Some(|(data, meta): (Vec<FileMetadata>, Vec<FileMetadata>)| {
                format!(
                    "Categorized data files count={}, metadata files count={}",
                    data.len(),
                    meta.len()
                )
            }),
        )
        .await;

        let referenced_files = measure_dur_async(
            "find_referenced_files",
            &mut internal_metrics,
            || async { table_analyzer.find_referenced_files(&metadata_files).await },
            Some(|c: &Vec<String>| format!("Found referenced files, count={}", c.len())),
        )
        .await?;

        metrics.total_files = data_files.len();
        metrics.total_size_bytes = data_files.iter().map(|f| f.size as u64).sum();

        let referenced_set: HashSet<String> = referenced_files.into_iter().collect();
        let unreferenced_files = measure_dur(
            "find_unreferenced_files",
            &mut internal_metrics,
            || {
                data_files
                    .iter()
                    .filter(|f| !referenced_set.contains(&f.path))
                    .map(|f| FileInfo {
                        path: format!("{}/{}", self.storage_provider.base_path(), f.path),
                        size_bytes: f.size as u64,
                        last_modified: f.last_modified.clone().map(|m| m.to_rfc3339()),
                        is_referenced: false,
                    })
                    .collect::<Vec<FileInfo>>()
            },
            Some(|c: Vec<FileInfo>| format!("Found unreferenced files, count={}", c.len())),
        )
        .await;

        metrics.unreferenced_files = unreferenced_files;
        metrics.unreferenced_size_bytes = metrics
            .unreferenced_files
            .iter()
            .map(|f| f.size_bytes)
            .sum();

        metrics.partitions = measure_dur_with_error(
            "analyze_partitioning",
            &mut internal_metrics,
            || self.analyze_partitioning(&data_files),
            Some(|c: &Vec<PartitionInfo>| format!("Analyzed partitions count={}", c.len())),
        )?;

        let data_files_total_size: u64 = data_files.iter().map(|f| f.size).sum();

        measure_dur_async(
            "update_metrics_from_metadata",
            &mut internal_metrics,
            || async {
                table_analyzer
                    .update_metrics_from_metadata(
                        &metadata_files,
                        data_files_total_size,
                        data_files.len(),
                        &mut metrics,
                    )
                    .await
            },
            Some(|_| "Updated metrics from metadata".to_string()),
        )
        .await?;

        let (small_files, medium_files, large_files, very_large_files) =
            measure_dur(
                "calculate_file_size_distribution",
                &mut internal_metrics,
                || { self.calculate_file_size_distribution(&data_files) },
                Some(|(small, medium, large, very_large): (usize, usize, usize, usize)| {
                    format!(
                        "Calculated file size distribution, small={}, medium={}, large={}, very_large={}",
                        small, medium, large, very_large
                    )
                }),
            ).await;

        metrics.file_size_distribution.small_files = small_files;
        metrics.file_size_distribution.medium_files = medium_files;
        metrics.file_size_distribution.large_files = large_files;
        metrics.file_size_distribution.very_large_files = very_large_files;

        if metrics.total_files > 0 {
            metrics.avg_file_size_bytes =
                metrics.total_size_bytes as f64 / metrics.total_files as f64;
        }

        let (
            metadata_file_count,
            metadata_total_size_bytes,
            avg_metadata_file_size,
            metadata_growth_rate,
        ) = measure_dur(
            "calculate_metadata_health",
            &mut internal_metrics,
            || { self.calculate_metadata_health(&metadata_files) },
            Some(|(count, size, avg, growth): (usize, u64, f64, f64)| {
                format!(
                    "Calculated metadata health, file_count={}, bytesize={}, avg_bytesize={}, growth_rate={}",
                    count, size, avg, growth
                )
            }),
        ).await;

        metrics.metadata_health.metadata_file_count = metadata_file_count;
        metrics.metadata_health.metadata_total_size_bytes = metadata_total_size_bytes;
        metrics.metadata_health.avg_metadata_file_size = avg_metadata_file_size;
        metrics.metadata_health.metadata_growth_rate = metadata_growth_rate;

        measure_dur(
            "calculate_data_skew",
            &mut internal_metrics,
            || metrics.calculate_data_skew(),
            None,
        )
        .await;

        measure_dur(
            "calculate_snapshot_health",
            &mut internal_metrics,
            || metrics.calculate_snapshot_health(metadata_files.len()),
            None,
        )
        .await;

        metrics.file_compaction = measure_dur_async(
            "analyze_file_compaction",
            &mut internal_metrics,
            || async {
                if let Some(file_compaction) = metrics.file_compaction.clone() {
                    let z_order_opportunity = file_compaction.z_order_opportunity.clone();
                    let z_order_columns = file_compaction.z_order_columns;
                    self.analyze_file_compaction(
                        &data_files,
                        z_order_opportunity,
                        z_order_columns.clone(),
                    )
                    .await
                } else {
                    Ok(None)
                }
            },
            Some(|_| "Calculated compaction metrics".to_string()),
        )
        .await?;

        measure_dur(
            "generate_recommendations",
            &mut internal_metrics,
            || metrics.generate_recommendations(),
            Some(|_| "Generated recommendations".to_string()),
        )
        .await;

        metrics.health_score = measure_dur(
            "calculate_health_score",
            &mut internal_metrics,
            || metrics.calculate_health_score(),
            Some(|score: f64| format!("Calculated health score={}", score)),
        )
        .await;

        let table_path = self.storage_provider.uri_from_path(location);

        let analyze_after_validation_dur = list_files_start.elapsed()?;
        internal_metrics.push_back((
            "analyze_after_validation_dur",
            list_files_start,
            analyze_after_validation_dur,
        ));

        info!("Analysis took={}", analyze_after_validation_dur.as_millis());

        // Only try to open Delta reader for Delta tables
        if table_type == "delta" {
            measure_dur_async(
                "delta_reader",
                &mut internal_metrics,
                || async {
                    let delta_reader = DeltaReader::open(
                        self.storage_provider.uri_from_path(location).as_str(),
                        &self.storage_provider.clean_options(),
                    )
                    .await;

                    match delta_reader {
                        Ok(reader) => {
                            metrics.delta_table_specific_metrics =
                                Some(reader.extract_metrics().await?);
                            Ok(())
                        }
                        Err(e) => {
                            warn!("Failed to open Delta reader: {}", e);
                            Err(e)
                        }
                    }
                },
                Some(|_| "Opened Delta reader".to_string()),
            )
            .await?;
        } else if table_type == "iceberg" {
            measure_dur_async(
                "iceberg_reader",
                &mut internal_metrics,
                || async {
                    let iceberg_reader = IcebergReader::open(
                        self.storage_provider.uri_from_path(location).as_str(),
                        &self.storage_provider.clean_options(),
                    )
                    .await;

                    match iceberg_reader {
                        Ok(reader) => {
                            metrics.iceberg_table_specific_metrics =
                                Some(reader.extract_metrics().await?);
                            Ok(())
                        }
                        Err(e) => {
                            warn!("Failed to open Iceberg reader: {}", e);
                            Err(e)
                        }
                    }
                },
                Some(|_| "Opened Iceberg reader".to_string()),
            )
            .await?;
        } else if table_type == "hudi" {
            measure_dur_async(
                "hudi_reader",
                &mut internal_metrics,
                || async {
                    let hudi_reader = HudiReader::new(
                        self.storage_provider.uri_from_path(location).as_str(),
                        "COPY_ON_WRITE", // Default to COW, could be detected from hoodie.properties
                    );

                    match hudi_reader {
                        Ok(reader) => {
                            metrics.hudi_table_specific_metrics =
                                Some(reader.extract_metrics().await?);
                            Ok(())
                        }
                        Err(e) => {
                            warn!("Failed to open Hudi reader: {}", e);
                            Err(e)
                        }
                    }
                },
                Some(|_| "Opened Hudi reader".to_string()),
            )
            .await?;
        }

        let timed_metrics: LinkedList<(String, u128, u128)> = internal_metrics
            .iter()
            .map(
                |(k, st, dur)| -> Result<(String, u128, u128), Box<dyn Error + Send + Sync>> {
                    Ok((
                        k.to_string(),
                        st.duration_since(UNIX_EPOCH)?.as_millis(),
                        dur.as_millis(),
                    ))
                },
            )
            .collect::<Result<LinkedList<(_, _, _)>, _>>()?;

        let report = HealthReport {
            table_path,
            table_type,
            analysis_timestamp: chrono::Utc::now().to_rfc3339(),
            metrics: metrics.clone(),
            health_score: metrics.health_score.clone(),
            timed_metrics: TimedLikeMetrics {
                duration_collection: timed_metrics,
            },
        };

        Ok(report)
    }

    fn analyze_partitioning(
        &self,
        data_files: &Vec<FileMetadata>,
    ) -> Result<Vec<PartitionInfo>, Box<dyn Error + Send + Sync>> {
        let mut partition_map: HashMap<String, PartitionInfo> = HashMap::new();

        for file in data_files {
            // Extract partition information from file path
            // Delta Lake typically uses partition columns in the path like: col1=value1/col2=value2/file.parquet
            let path_parts: Vec<&str> = file.path.split('/').collect();
            let mut partition_values = HashMap::new();
            let mut _file_name = "";

            for part in &path_parts {
                if part.contains('=') {
                    let kv: Vec<&str> = part.split('=').collect();
                    if kv.len() == 2 {
                        partition_values.insert(kv[0].to_string(), kv[1].to_string());
                    }
                } else if part.ends_with(".parquet") {
                    _file_name = part;
                }
            }

            let partition_key = serde_json::to_string(&partition_values).unwrap_or_default();

            let partition_info =
                partition_map
                    .entry(partition_key)
                    .or_insert_with(|| PartitionInfo {
                        partition_values: partition_values.clone(),
                        file_count: 0,
                        total_size_bytes: 0,
                        avg_file_size_bytes: 0.0,
                        files: Vec::new(),
                    });

            partition_info.file_count += 1;
            partition_info.total_size_bytes += file.size as u64;
            partition_info.files.push(FileInfo {
                path: format!("{}/{}", self.storage_provider.base_path(), file.path),
                size_bytes: file.size as u64,
                last_modified: file.last_modified.clone().map(|m| m.to_rfc3339()),
                is_referenced: true, // We'll update this later
            });
        }

        // Calculate averages for each partition
        for partition in partition_map.values_mut() {
            if partition.file_count > 0 {
                partition.avg_file_size_bytes =
                    partition.total_size_bytes as f64 / partition.file_count as f64;
            }
        }

        let partitions = partition_map.into_values().collect::<Vec<PartitionInfo>>();

        Ok(partitions)
    }

    fn calculate_file_size_distribution(
        &self,
        data_files: &Vec<FileMetadata>,
    ) -> (usize, usize, usize, usize) {
        let mut small_files = 0;
        let mut medium_files = 0;
        let mut large_files = 0;
        let mut very_large_files = 0;

        for file in data_files {
            let size_mb = file.size as f64 / (1024.0 * 1024.0);

            if size_mb < 16.0 {
                small_files += 1;
            } else if size_mb < 128.0 {
                medium_files += 1;
            } else if size_mb < 1024.0 {
                large_files += 1;
            } else {
                very_large_files += 1;
            }
        }
        (small_files, medium_files, large_files, very_large_files)
    }

    /// Calculate metadata health metrics.
    ///
    /// # Arguments
    ///
    /// * `metadata_files` - Vector of metadata file information
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * `usize` - Metadata file count
    /// * `u64` - Total metadata size in bytes
    /// * `f64` - Metadata health score (average file size)
    /// * `f64` - Metadata growth rate (currently placeholder, returns 0.0)
    pub fn calculate_metadata_health(
        &self,
        metadata_files: &Vec<FileMetadata>,
    ) -> (usize, u64, f64, f64) {
        let mut metadata_health: f64 = 0.0;
        let metadata_file_count = metadata_files.len();
        let metadata_total_size_bytes: u64 = metadata_files.iter().map(|f| f.size as u64).sum();

        if !metadata_files.is_empty() {
            metadata_health = metadata_total_size_bytes as f64 / metadata_files.len() as f64;
        }

        // Estimate growth rate (simplified - would need historical data for accuracy)
        let metadata_growth_rate = 0.0; // Placeholder

        (
            metadata_file_count,
            metadata_total_size_bytes,
            metadata_health,
            metadata_growth_rate,
        )
    }

    async fn analyze_file_compaction(
        &self,
        data_files: &Vec<FileMetadata>,
        z_order_opportunity: bool,
        z_order_columns: Vec<String>,
    ) -> Result<Option<FileCompactionMetrics>, Box<dyn Error + Send + Sync>> {
        let mut small_files_count = 0;
        let mut small_files_size = 0u64;
        let mut potential_compaction_files = 0;
        let mut estimated_savings = 0u64;

        // Analyze file sizes for compaction opportunities
        for file in data_files {
            let file_size = file.size as u64;
            if file_size < 16 * 1024 * 1024 {
                // < 16MB
                small_files_count += 1;
                small_files_size += file_size;
                potential_compaction_files += 1;
            }
        }

        // Calculate potential savings
        if small_files_count > 1 {
            let target_size = 128 * 1024 * 1024; // 128MB target
            let files_per_target = (target_size as f64
                / (small_files_size as f64 / small_files_count as f64))
                .ceil() as usize;
            let target_files = (small_files_count as f64 / files_per_target as f64).ceil() as usize;
            let estimated_target_size = target_files as u64 * target_size / 2; // Conservative estimate
            estimated_savings = small_files_size.saturating_sub(estimated_target_size);
        }

        let compaction_opportunity = self.calculate_compaction_opportunity(
            small_files_count,
            small_files_size,
            data_files.len(),
        );
        let recommended_target_size = self.calculate_recommended_target_size(data_files);
        let compaction_priority =
            self.calculate_compaction_priority(compaction_opportunity, small_files_count);

        Ok(Some(FileCompactionMetrics {
            compaction_opportunity_score: compaction_opportunity,
            small_files_count,
            small_files_size_bytes: small_files_size,
            potential_compaction_files,
            estimated_compaction_savings_bytes: estimated_savings,
            recommended_target_file_size_bytes: recommended_target_size,
            compaction_priority,
            z_order_opportunity,
            z_order_columns,
        }))
    }

    fn calculate_compaction_opportunity(
        &self,
        small_files: usize,
        small_files_size: u64,
        total_files: usize,
    ) -> f64 {
        if total_files == 0 {
            return 0.0;
        }

        let small_file_ratio = small_files as f64 / total_files as f64;
        let _size_ratio = small_files_size as f64 / (small_files_size as f64 + 1.0); // Avoid division by zero

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

    fn calculate_recommended_target_size(&self, data_files: &Vec<FileMetadata>) -> u64 {
        if data_files.is_empty() {
            return 128 * 1024 * 1024; // 128MB default
        }

        let total_size = data_files.iter().map(|f| f.size as u64).sum::<u64>();
        let avg_size = total_size as f64 / data_files.len() as f64;

        // Recommend target size based on current average
        if avg_size < 16.0 * 1024.0 * 1024.0 {
            128 * 1024 * 1024 // 128MB for small files
        } else if avg_size < 64.0 * 1024.0 * 1024.0 {
            256 * 1024 * 1024 // 256MB for medium files
        } else {
            512 * 1024 * 1024 // 512MB for large files
        }
    }

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
}
