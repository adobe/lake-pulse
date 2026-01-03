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
use lance_index::traits::DatasetIndexExt;
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

        // Extract operation metrics from version history
        let operation_metrics = self.extract_operation_metrics().await.ok();

        let metrics = LanceMetrics {
            version,
            metadata,
            table_properties,
            file_stats,
            fragment_info,
            index_info,
            operation_metrics,
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

        // Use the DatasetIndexExt trait to load indices
        let indices = match self.dataset.load_indices().await {
            Ok(indices) => indices,
            Err(e) => {
                warn!("Failed to load indices: {}", e);
                // Fall back to manifest-based detection
                let manifest = self.dataset.manifest();
                let has_indices = manifest.index_section.is_some();
                return Ok(IndexMetrics {
                    num_indices: if has_indices { 1 } else { 0 },
                    indexed_columns: Vec::new(),
                    index_types: if has_indices {
                        vec!["VECTOR".to_string()]
                    } else {
                        Vec::new()
                    },
                    total_index_size_bytes: 0,
                });
            }
        };

        let num_indices = indices.len();

        // Extract indexed column names by looking up field IDs in the schema
        let schema = self.dataset.schema();
        let mut indexed_columns = Vec::new();
        let mut index_types = Vec::new();

        for index in indices.iter() {
            // Get column names from field IDs
            for field_id in &index.fields {
                if let Some(field) = schema.field_by_id(*field_id) {
                    indexed_columns.push(field.name.clone());
                }
            }

            // Determine index type from the index details
            // Lance supports vector indices (IVF_PQ, IVF_FLAT, etc.) and scalar indices (BTREE)
            let index_type = if let Some(ref details) = index.index_details {
                // Check the type_url to determine index type
                if details.type_url.contains("VectorIndex") {
                    "VECTOR".to_string()
                } else if details.type_url.contains("ScalarIndex") {
                    "SCALAR".to_string()
                } else if details.type_url.contains("FtsIndex") {
                    "FTS".to_string()
                } else {
                    format!("UNKNOWN({})", index.name)
                }
            } else {
                // Default to vector for older indices without details
                "VECTOR".to_string()
            };

            if !index_types.contains(&index_type) {
                index_types.push(index_type);
            }
        }

        let metrics = IndexMetrics {
            num_indices,
            indexed_columns,
            index_types,
            total_index_size_bytes: 0, // Index size not directly available from metadata
        };

        info!(
            "Index metrics: num_indices={}, indexed_columns={:?}, types={:?}",
            metrics.num_indices, metrics.indexed_columns, metrics.index_types
        );

        Ok(metrics)
    }

    /// Extract operation metrics from version history
    async fn extract_operation_metrics(
        &self,
    ) -> Result<super::metrics::OperationMetrics, Box<dyn Error + Send + Sync>> {
        use super::metrics::OperationMetrics;

        info!("Extracting operation metrics from version history");

        let versions = match self.dataset.versions().await {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to get dataset versions: {}", e);
                return Ok(OperationMetrics::default());
            }
        };

        if versions.is_empty() {
            return Ok(OperationMetrics::default());
        }

        // Extract version info (version number and timestamp) - Version doesn't implement Clone
        let mut version_info: Vec<(u64, i64)> = versions
            .iter()
            .map(|v| (v.version, v.timestamp.timestamp()))
            .collect();
        version_info.sort_by_key(|(v, _)| *v);

        // Get the dataset URI for checking out versions
        let uri = self.dataset.uri();

        // Collect version stats
        let mut stats: Vec<(u64, usize, usize, usize, u64)> = Vec::new();

        for (version_num, _) in &version_info {
            let versioned_ds = match Dataset::open(uri).await {
                Ok(ds) => match ds.checkout_version(*version_num).await {
                    Ok(vds) => vds,
                    Err(_) => continue,
                },
                Err(_) => continue,
            };

            let row_count = versioned_ds.count_rows(None).await.unwrap_or(0);
            let deleted_count = versioned_ds.count_deleted_rows().await.unwrap_or(0);
            let fragment_count = versioned_ds.manifest().fragments.len();
            // Estimate data size from physical rows since CachedFileSize doesn't have unwrap_or
            let data_size: u64 = versioned_ds
                .manifest()
                .fragments
                .iter()
                .filter_map(|f| f.physical_rows)
                .map(|rows| rows as u64 * 100) // rough estimate: 100 bytes per row
                .sum();

            stats.push((
                *version_num,
                row_count,
                deleted_count,
                fragment_count,
                data_size,
            ));
        }

        // Infer operation types by comparing consecutive versions
        let mut append_count = 0usize;
        let mut delete_count = 0usize;
        let mut overwrite_count = 0usize;
        let mut compaction_count = 0usize;
        let mut total_bytes_written = 0u64;

        for i in 1..stats.len() {
            let (_, prev_rows, prev_deleted, prev_frags, prev_size) = stats[i - 1];
            let (_, curr_rows, curr_deleted, curr_frags, curr_size) = stats[i];

            if curr_size > prev_size {
                total_bytes_written += curr_size - prev_size;
            }

            if curr_deleted > prev_deleted {
                delete_count += 1;
            } else if curr_rows > prev_rows && curr_frags >= prev_frags {
                append_count += 1;
            } else if curr_frags < prev_frags && curr_rows == prev_rows {
                compaction_count += 1;
            } else if curr_frags != prev_frags
                && (curr_rows as i64 - prev_rows as i64).abs() > (prev_rows as i64 / 2)
            {
                overwrite_count += 1;
            } else if curr_rows > prev_rows {
                append_count += 1;
            }
        }

        let total_operations = stats.len().saturating_sub(1);

        let operations_per_day = if version_info.len() >= 2 {
            let first_ts = version_info.first().map(|(_, ts)| *ts).unwrap_or(0);
            let last_ts = version_info.last().map(|(_, ts)| *ts).unwrap_or(0);
            let days = ((last_ts - first_ts) as f64 / 86400.0).max(1.0);
            total_operations as f64 / days
        } else {
            0.0
        };

        let mut operation_distribution = HashMap::new();
        if total_operations > 0 {
            operation_distribution.insert(
                "append".to_string(),
                append_count as f64 / total_operations as f64 * 100.0,
            );
            operation_distribution.insert(
                "delete".to_string(),
                delete_count as f64 / total_operations as f64 * 100.0,
            );
            operation_distribution.insert(
                "overwrite".to_string(),
                overwrite_count as f64 / total_operations as f64 * 100.0,
            );
            operation_distribution.insert(
                "compaction".to_string(),
                compaction_count as f64 / total_operations as f64 * 100.0,
            );
        }

        let metrics = OperationMetrics {
            total_operations,
            append_count,
            delete_count,
            overwrite_count,
            compaction_count,
            index_operation_count: 0,
            total_bytes_written,
            operations_per_day,
            operation_distribution,
        };

        info!(
            "Operation metrics: {} total, {} appends, {} deletes",
            metrics.total_operations, metrics.append_count, metrics.delete_count
        );

        Ok(metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LANCE_DATASET_PATH: &str = "examples/data/lance_dataset.lance";

    #[tokio::test]
    async fn test_lance_reader_open() {
        let reader = LanceReader::open(LANCE_DATASET_PATH).await;
        assert!(reader.is_ok(), "Failed to open Lance dataset");
    }

    #[tokio::test]
    async fn test_lance_reader_open_invalid_path() {
        let reader = LanceReader::open("/nonexistent/path/to/lance").await;
        assert!(reader.is_err());
    }

    #[tokio::test]
    async fn test_lance_reader_extract_metrics() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let metrics = reader.extract_metrics().await;
        assert!(metrics.is_ok(), "Failed to extract metrics: {:?}", metrics);

        let metrics = metrics.unwrap();

        // Verify basic metrics are populated
        assert!(metrics.version > 0, "Version should be > 0");
        assert!(
            metrics.metadata.field_count > 0,
            "Field count should be > 0"
        );
        assert!(
            !metrics.metadata.schema_string.is_empty(),
            "Schema string should not be empty"
        );
    }

    #[tokio::test]
    async fn test_lance_reader_extract_table_metadata() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let metadata = reader
            .extract_table_metadata()
            .await
            .expect("Failed to extract table metadata");

        // Verify metadata fields
        assert!(!metadata.uuid.is_empty(), "UUID should not be empty");
        assert!(
            !metadata.schema_string.is_empty(),
            "Schema string should not be empty"
        );
        assert!(metadata.field_count > 0, "Field count should be > 0");
    }

    #[tokio::test]
    async fn test_lance_reader_extract_file_statistics() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let file_stats = reader
            .extract_file_statistics()
            .await
            .expect("Failed to extract file statistics");

        // Verify file statistics
        assert!(
            file_stats.num_data_files > 0,
            "Should have at least one data file"
        );
    }

    #[tokio::test]
    async fn test_lance_reader_extract_fragment_metrics() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let fragment_metrics = reader
            .extract_fragment_metrics()
            .await
            .expect("Failed to extract fragment metrics");

        // Verify fragment metrics
        assert!(
            fragment_metrics.num_fragments > 0,
            "Should have at least one fragment"
        );
    }

    #[tokio::test]
    async fn test_lance_reader_extract_index_metrics() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let index_metrics = reader
            .extract_index_metrics()
            .await
            .expect("Failed to extract index metrics");

        // Index metrics should be valid (may or may not have indices)
        // num_indices is usize so always >= 0, just verify it's accessible
        let _ = index_metrics.num_indices;
    }

    #[tokio::test]
    async fn test_lance_reader_metrics_consistency() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify consistency between different metrics
        // If there are fragments, there should be data files
        if metrics.fragment_info.num_fragments > 0 {
            assert!(
                metrics.file_stats.num_data_files > 0,
                "Fragments exist but no data files"
            );
        }

        // If there are fragments with deletions, there should be deletion files
        if metrics.fragment_info.num_fragments_with_deletions > 0 {
            assert!(
                metrics.file_stats.num_deletion_files > 0,
                "Fragments with deletions exist but no deletion files"
            );
        }
    }

    #[tokio::test]
    async fn test_lance_reader_row_count() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // The test dataset should have rows
        if let Some(num_rows) = metrics.metadata.num_rows {
            assert!(num_rows > 0, "Row count should be > 0");
        }
    }

    #[tokio::test]
    async fn test_lance_reader_operation_metrics() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Operation metrics should be present (may be None if extraction fails)
        // If present, verify the structure is valid
        if let Some(ref op_metrics) = metrics.operation_metrics {
            // Operation counts should sum to total (or less if some ops weren't classified)
            let classified_ops = op_metrics.append_count
                + op_metrics.delete_count
                + op_metrics.overwrite_count
                + op_metrics.compaction_count;
            assert!(
                classified_ops <= op_metrics.total_operations,
                "Classified ops should not exceed total"
            );

            // Operations per day should be non-negative
            assert!(
                op_metrics.operations_per_day >= 0.0,
                "Operations per day should be >= 0"
            );

            // Verify operation distribution is populated
            if op_metrics.total_operations > 0 {
                // Distribution should have entries
                assert!(
                    !op_metrics.operation_distribution.is_empty(),
                    "Operation distribution should be populated"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_lance_reader_extract_operation_metrics_directly() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        // Call extract_operation_metrics directly
        let result = reader.extract_operation_metrics().await;
        assert!(
            result.is_ok(),
            "extract_operation_metrics should succeed: {:?}",
            result
        );

        let op_metrics = result.unwrap();

        // Verify the structure
        let classified = op_metrics.append_count
            + op_metrics.delete_count
            + op_metrics.overwrite_count
            + op_metrics.compaction_count;
        assert!(
            classified <= op_metrics.total_operations,
            "Classified ops ({}) should not exceed total ({})",
            classified,
            op_metrics.total_operations
        );
    }

    #[tokio::test]
    async fn test_lance_reader_dataset_access() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        // Test that we can access the dataset via extract_metrics
        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Version should be > 0
        assert!(metrics.version > 0, "Version should be > 0");
    }

    #[tokio::test]
    async fn test_lance_reader_schema_access() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Schema should have fields (check via field_count in metadata)
        assert!(
            metrics.metadata.field_count > 0,
            "Schema should have fields"
        );

        // Schema string should not be empty
        assert!(
            !metrics.metadata.schema_string.is_empty(),
            "Schema string should not be empty"
        );
    }

    #[tokio::test]
    async fn test_lance_reader_fragment_access() {
        let reader = LanceReader::open(LANCE_DATASET_PATH)
            .await
            .expect("Failed to open Lance dataset");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Fragment count should be > 0
        assert!(
            metrics.fragment_info.num_fragments > 0,
            "Fragment count should be > 0"
        );
    }
}
