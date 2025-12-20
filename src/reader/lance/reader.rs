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

use super::metrics::{FileStatistics, FragmentMetrics, IndexMetrics, LanceMetrics, TableMetadata};
use lance::dataset::Dataset;
use std::collections::HashMap;
use std::error::Error;
use tracing::{info, warn};

/// Lance table reader for extracting metrics
pub struct LanceReader {
    dataset: Dataset,
}

impl LanceReader {
    /// Open a Lance table from the given location.
    ///
    /// # Arguments
    ///
    /// * `location` - The path to the Lance table (e.g., "/path/to/table", "s3://bucket/path")
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(LanceReader)` - A successfully opened Lance table reader
    /// * `Err(Box<dyn Error + Send + Sync>)` - If the table cannot be opened
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The location is invalid or cannot be accessed
    /// * The Lance dataset does not exist at the specified location
    /// * Storage credentials are invalid or expired
    /// * Network or storage access errors occur
    /// * The dataset metadata is corrupted or cannot be read
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lake_pulse::reader::lance::reader::LanceReader;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let reader = LanceReader::open("/path/to/lance/table").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(location: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        info!("Opening Lance table at location={}", location);

        // Open the Lance dataset
        let dataset = Dataset::open(location)
            .await
            .map_err(|e| format!("Failed to open Lance dataset at {}: {}", location, e))?;

        info!(
            "Successfully opened Lance table, version={}",
            dataset.version().version
        );

        Ok(Self { dataset })
    }

    /// Extract comprehensive metrics from the Lance table.
    ///
    /// This method reads the table's metadata and extracts various metrics including
    /// version information, schema, file statistics, fragment information, and indices.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(LanceMetrics)` - Comprehensive metrics including version, schema, file statistics, fragments, and indices
    /// * `Err(Box<dyn Error + Send + Sync>)` - If metrics cannot be extracted
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Table metadata cannot be read
    /// * Schema information is corrupted or invalid
    /// * Fragment information cannot be accessed
    /// * Index information cannot be retrieved
    pub async fn extract_metrics(&self) -> Result<LanceMetrics, Box<dyn Error + Send + Sync>> {
        info!("Extracting metrics from Lance table");

        let version = self.dataset.version().version;

        // Extract table metadata
        let metadata = self.extract_table_metadata().await?;

        // Extract table properties (if any)
        let table_properties = HashMap::new(); // Lance doesn't have explicit table properties like Delta/Iceberg

        // Extract file statistics
        let file_stats = self.extract_file_statistics().await?;

        // Extract fragment information
        let fragment_info = self.extract_fragment_metrics().await?;

        // Extract index information
        let index_info = self.extract_index_metrics().await?;

        let metrics = LanceMetrics {
            version,
            metadata,
            table_properties,
            file_stats,
            fragment_info,
            index_info,
        };

        info!("Successfully extracted Lance table metrics");
        Ok(metrics)
    }

    /// Extract table metadata from Lance dataset
    async fn extract_table_metadata(&self) -> Result<TableMetadata, Box<dyn Error + Send + Sync>> {
        info!("Extracting table metadata");

        let schema = self.dataset.schema();
        let field_count = schema.fields.len();

        // Serialize schema to JSON string - use Debug format since Schema doesn't implement Serialize
        let schema_string = format!("{:#?}", schema);

        // Get manifest for additional metadata
        let manifest = self.dataset.manifest();

        // Get version info
        let version = self.dataset.version().version;

        // Count rows
        let count_rows_result = self.dataset.count_rows(None).await;
        let num_rows = match count_rows_result {
            Ok(count) => Some(count as u64),
            Err(e) => {
                warn!("Failed to count rows: {}", e);
                None
            }
        };

        // Count deleted rows (if deletion files exist)
        let num_deleted_rows = manifest
            .fragments
            .iter()
            .filter_map(|f| f.deletion_file.as_ref())
            .count() as u64;

        let metadata = TableMetadata {
            uuid: version.to_string(), // Use version number as identifier
            schema_string,
            field_count,
            created_time: None, // Lance doesn't track creation time in manifest
            last_modified_time: None, // Could be derived from file timestamps
            num_rows,
            num_deleted_rows: if num_deleted_rows > 0 {
                Some(num_deleted_rows)
            } else {
                None
            },
        };

        Ok(metadata)
    }

    /// Extract file statistics from the Lance table
    async fn extract_file_statistics(
        &self,
    ) -> Result<FileStatistics, Box<dyn Error + Send + Sync>> {
        info!("Extracting file statistics");

        let manifest = self.dataset.manifest();
        let fragments = &manifest.fragments;

        if fragments.is_empty() {
            warn!("No fragments found in Lance table");
            return Ok(FileStatistics {
                num_data_files: 0,
                num_deletion_files: 0,
                total_data_size_bytes: 0,
                total_deletion_size_bytes: 0,
                avg_data_file_size_bytes: 0.0,
                min_data_file_size_bytes: 0,
                max_data_file_size_bytes: 0,
            });
        }

        let mut num_data_files = 0;
        let mut num_deletion_files = 0;
        let mut total_data_size: u64 = 0;
        let mut total_deletion_size: u64 = 0;
        let mut min_size: u64 = u64::MAX;
        let mut max_size: u64 = 0;

        for fragment in fragments.iter() {
            // Count data files in each fragment
            num_data_files += fragment.files.len();

            // Sum up physical sizes
            if let Some(physical_rows) = fragment.physical_rows {
                // Estimate size based on rows (rough approximation)
                // In reality, we'd need to read actual file sizes from storage
                let estimated_size = physical_rows as u64 * 100; // rough estimate
                total_data_size += estimated_size;
                min_size = min_size.min(estimated_size);
                max_size = max_size.max(estimated_size);
            }

            // Check for deletion files
            if fragment.deletion_file.is_some() {
                num_deletion_files += 1;
                // Deletion files are typically small
                total_deletion_size += 1024; // rough estimate
            }
        }

        let avg_size = if num_data_files > 0 {
            total_data_size as f64 / num_data_files as f64
        } else {
            0.0
        };

        if min_size == u64::MAX {
            min_size = 0;
        }

        let stats = FileStatistics {
            num_data_files,
            num_deletion_files,
            total_data_size_bytes: total_data_size,
            total_deletion_size_bytes: total_deletion_size,
            avg_data_file_size_bytes: avg_size,
            min_data_file_size_bytes: min_size,
            max_data_file_size_bytes: max_size,
        };

        info!(
            "File statistics: data_files={}, deletion_files={}, total_data_size={}",
            stats.num_data_files, stats.num_deletion_files, stats.total_data_size_bytes
        );

        Ok(stats)
    }

    /// Extract fragment-level metrics
    async fn extract_fragment_metrics(
        &self,
    ) -> Result<FragmentMetrics, Box<dyn Error + Send + Sync>> {
        info!("Extracting fragment metrics");

        let manifest = self.dataset.manifest();
        let fragments = &manifest.fragments;

        let num_fragments = fragments.len();
        let num_fragments_with_deletions = fragments
            .iter()
            .filter(|f| f.deletion_file.is_some())
            .count();

        let mut total_physical_rows: u64 = 0;
        let mut min_rows: u64 = u64::MAX;
        let mut max_rows: u64 = 0;

        for fragment in fragments.iter() {
            if let Some(physical_rows) = fragment.physical_rows {
                let rows = physical_rows as u64;
                total_physical_rows += rows;
                min_rows = min_rows.min(rows);
                max_rows = max_rows.max(rows);
            }
        }

        let avg_rows = if num_fragments > 0 {
            total_physical_rows as f64 / num_fragments as f64
        } else {
            0.0
        };

        if min_rows == u64::MAX {
            min_rows = 0;
        }

        let metrics = FragmentMetrics {
            num_fragments,
            num_fragments_with_deletions,
            avg_rows_per_fragment: avg_rows,
            min_rows_per_fragment: min_rows,
            max_rows_per_fragment: max_rows,
            total_physical_rows,
        };

        info!(
            "Fragment metrics: fragments={}, with_deletions={}, total_rows={}",
            metrics.num_fragments,
            metrics.num_fragments_with_deletions,
            metrics.total_physical_rows
        );

        Ok(metrics)
    }

    /// Extract index metrics
    async fn extract_index_metrics(&self) -> Result<IndexMetrics, Box<dyn Error + Send + Sync>> {
        info!("Extracting index metrics");

        // Lance indices - in v0.39, index_section is Option<usize> indicating offset
        // Index metadata is not directly available in the manifest structure
        // We would need to read index files separately to get detailed index info
        let manifest = self.dataset.manifest();

        // Check if indices exist (index_section is Some)
        let has_indices = manifest.index_section.is_some();

        let metrics = IndexMetrics {
            num_indices: if has_indices { 1 } else { 0 }, // Rough estimate
            indexed_columns: Vec::new(),                  // Would need to parse index files
            index_types: if has_indices {
                vec!["VECTOR".to_string()]
            } else {
                Vec::new()
            },
            total_index_size_bytes: 0, // Would need to read index file sizes
        };

        info!("Index metrics: num_indices={}", metrics.num_indices);

        Ok(metrics)
    }
}
