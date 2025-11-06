use crate::analyze::metrics::HealthMetrics;
use crate::storage::FileMetadata;
use async_trait::async_trait;
use std::error::Error;

/// Trait for table format-specific analyzers.
///
/// This trait defines the interface that all table format analyzers (Delta, Iceberg, etc.)
/// must implement. It provides three core operations:
/// 1. Categorizing files into data files and metadata files
/// 2. Finding referenced files from metadata
/// 3. Updating health metrics from metadata
///
/// # Design Philosophy
///
/// This trait enables a strategy pattern where the main `Analyzer` can work with any
/// table format without knowing the specific implementation details. When adding support
/// for a new table format (e.g., Iceberg, Hudi), simply:
/// 1. Create a new analyzer struct (e.g., `IcebergAnalyzer`)
/// 2. Implement this trait for it
/// 3. Update the factory logic in `Analyzer::analyze()` to instantiate it
///
/// No changes to the main analysis logic are required.
#[async_trait]
pub trait TableAnalyzer: Send + Sync {
    /// Categorize files into data files and metadata files.
    ///
    /// This method separates the list of all files into two categories:
    /// - Data files: The actual table data (e.g., Parquet files)
    /// - Metadata files: Table metadata (e.g., Delta transaction logs, Iceberg metadata files)
    ///
    /// # Arguments
    ///
    /// * `objects` - All files discovered in the table location
    ///
    /// # Returns
    ///
    /// A tuple of (data_files, metadata_files)
    fn categorize_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>);

    /// Find all data files referenced in the table metadata.
    ///
    /// This method parses the metadata files to extract the list of data files that are
    /// currently part of the table. Files not in this list are considered unreferenced
    /// and may be candidates for cleanup.
    ///
    /// # Arguments
    ///
    /// * `metadata_files` - The metadata files to parse
    ///
    /// # Returns
    ///
    /// A list of file paths that are referenced in the table metadata
    async fn find_referenced_files(
        &self,
        metadata_files: &Vec<FileMetadata>,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>>;

    /// Update health metrics by analyzing metadata files.
    ///
    /// This method performs deep analysis of the table metadata to extract various
    /// health metrics such as:
    /// - Schema evolution metrics
    /// - Time travel/snapshot metrics
    /// - Deletion vectors (if supported)
    /// - Table constraints
    /// - Clustering information
    /// - File compaction opportunities
    ///
    /// # Arguments
    ///
    /// * `metadata_files` - The metadata files to analyze
    /// * `data_files_total_size` - Total size of all data files in bytes
    /// * `data_files_total_files` - Total number of data files
    /// * `metrics` - The metrics object to update (mutated in place)
    async fn update_metrics_from_metadata(
        &self,
        metadata_files: &Vec<FileMetadata>,
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}
