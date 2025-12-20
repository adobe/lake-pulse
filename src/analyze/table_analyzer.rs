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
    /// A `Result` containing:
    /// * `Ok(Vec<String>)` - List of file paths that are referenced in the table metadata
    /// * `Err` - If metadata reading or parsing fails
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// * Any metadata file cannot be read from storage
    /// * Metadata file content is not valid UTF-8
    /// * JSON/metadata parsing fails
    async fn find_referenced_files(
        &self,
        metadata_files: &[FileMetadata],
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
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the metrics extraction.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// * Any metadata file cannot be read or parsed
    /// * Metric calculations fail due to invalid data
    /// * Parallel processing encounters errors
    async fn update_metrics_from_metadata(
        &self,
        metadata_files: &[FileMetadata],
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}
