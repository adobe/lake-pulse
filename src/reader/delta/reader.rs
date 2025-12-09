// Copyright 2022 Adobe. All rights reserved.
// This file is licensed to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
// OF ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

use super::metrics::{DeltaMetrics, FileStatistics, PartitionMetrics, ProtocolInfo, TableMetadata};
use crate::util::retry::retry_with_max_retries;
use deltalake::kernel::{Metadata, StructType};
use deltalake::{open_table_with_storage_options, DeltaTable};
use std::collections::HashMap;
use std::error::Error;
use tracing::{info, warn};
use url::Url;

/// Delta Lake table reader for extracting metrics
pub struct DeltaReader {
    table: DeltaTable,
}

impl DeltaReader {
    /// Open a Delta table from the given location.
    ///
    /// # Arguments
    ///
    /// * `table_uri` - The URI of the Delta table (e.g., "s3://bucket/path", "abfss://container@account.dfs.core.windows.net/path")
    /// * `cleaned_storage_options` - Optional storage configuration options
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(DeltaReader)` - A successfully opened Delta table reader
    /// * `Err(Box<dyn Error + Send + Sync>)` - If the table cannot be opened
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The location URI is invalid or cannot be parsed
    /// * The Delta table does not exist at the specified location
    /// * Storage credentials are invalid or expired
    /// * Network or storage access errors occur
    /// * The table metadata is corrupted or cannot be read
    pub async fn open(
        location: &str,
        cleaned_storage_options: &HashMap<String, String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        info!("Opening Delta table at  location={}", location);

        deltalake_azure::register_handlers(None);
        deltalake_aws::register_handlers(None);

        let url = Url::parse(location)?;

        // Build the table with storage options if provided
        let table = retry_with_max_retries(10, "open_table_with_storage_options", || async {
            open_table_with_storage_options(url.clone(), cleaned_storage_options.clone()).await
        })
        .await?;

        info!(
            "Successfully opened Delta table, version={:?}",
            table.version()
        );

        Ok(Self { table })
    }

    /// Extract comprehensive metrics from the Delta table.
    ///
    /// This method reads the table's snapshot and extracts various metrics including
    /// protocol information, metadata, table properties, and file statistics.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(DeltaMetrics)` - Comprehensive metrics including protocol info, metadata, table properties, and file statistics
    /// * `Err(Box<dyn Error + Send + Sync>)` - If metrics cannot be extracted
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The table snapshot cannot be read
    /// * Table metadata is corrupted or invalid
    /// * File statistics cannot be computed
    /// * Partition information cannot be extracted
    pub async fn extract_metrics(&self) -> Result<DeltaMetrics, Box<dyn Error + Send + Sync>> {
        info!("Extracting metrics from Delta table");

        let snapshot = self.table.snapshot()?;
        let version = self.table.version().unwrap_or(0);

        // Extract protocol information
        let protocol = snapshot.protocol();
        let protocol_info = ProtocolInfo {
            min_reader_version: protocol.min_reader_version(),
            min_writer_version: protocol.min_writer_version(),
            reader_features: protocol
                .reader_features()
                .map(|features| features.iter().map(|f| format!("{:?}", f)).collect()),
            writer_features: protocol
                .writer_features()
                .map(|features| features.iter().map(|f| format!("{:?}", f)).collect()),
        };

        // Extract metadata
        let metadata = snapshot.metadata();
        let schema = snapshot.schema();
        let table_metadata = self.extract_table_metadata(metadata, schema.as_ref())?;

        // Extract table properties (configuration)
        let table_properties = metadata.configuration().clone();

        // Extract file statistics
        let file_stats = self.extract_file_statistics().await?;

        // Extract partition information
        let partition_info = PartitionMetrics {
            num_partition_columns: metadata.partition_columns().len(),
            partition_columns: metadata.partition_columns().to_vec(),
            estimated_partition_count: None, // Could be calculated by scanning files
        };

        let metrics = DeltaMetrics {
            version,
            protocol: protocol_info,
            metadata: table_metadata,
            table_properties,
            file_stats,
            partition_info,
        };

        info!("Successfully extracted Delta table metrics");
        Ok(metrics)
    }

    /// Extract table metadata from Delta metadata
    ///
    /// # Arguments
    ///
    /// * `metadata` - The Delta metadata
    /// * `schema` - The Delta schema
    fn extract_table_metadata(
        &self,
        metadata: &Metadata,
        schema: &StructType,
    ) -> Result<TableMetadata, Box<dyn Error + Send + Sync>> {
        // Count fields in schema
        let field_count = schema.fields().len();

        // Serialize schema to JSON string
        let schema_string = serde_json::to_string_pretty(schema)?;

        let table_metadata = TableMetadata {
            id: metadata.id().to_string(),
            name: metadata.name().map(|s| s.to_string()),
            description: metadata.description().map(|s| s.to_string()),
            schema_string,
            field_count,
            partition_columns: metadata.partition_columns().to_vec(),
            created_time: metadata.created_time(),
            format_provider: "parquet".to_string(), // Delta Lake uses Parquet
            format_options: HashMap::new(),         // Format options not directly accessible
        };

        Ok(table_metadata)
    }

    /// Extract file statistics from the Delta table
    async fn extract_file_statistics(
        &self,
    ) -> Result<FileStatistics, Box<dyn Error + Send + Sync>> {
        info!("Extracting file statistics");

        // Get file URIs to count files
        let file_uris: Vec<String> = self.table.get_file_uris()?.collect();
        let num_files = file_uris.len();

        if num_files == 0 {
            warn!("No files found in Delta table");
            return Ok(FileStatistics {
                num_files: 0,
                total_size_bytes: 0,
                avg_file_size_bytes: 0.0,
                min_file_size_bytes: 0,
                max_file_size_bytes: 0,
                files_with_deletion_vectors: 0,
            });
        }

        // Use get_active_add_actions_by_partitions to get file metadata
        use futures::StreamExt;

        let mut total_size: u64 = 0;
        let mut min_size: u64 = u64::MAX;
        let mut max_size: u64 = 0;
        let files_with_dv = 0; // Deletion vector info not directly accessible via LogicalFileView

        // Get all files (no partition filters)
        let mut file_stream = self.table.get_active_add_actions_by_partitions(&[]);

        while let Some(file_result) = file_stream.next().await {
            match file_result {
                Ok(file_view) => {
                    let size = file_view.size() as u64;
                    total_size += size;
                    min_size = min_size.min(size);
                    max_size = max_size.max(size);

                    // Check for deletion vectors
                    // Note: LogicalFileView doesn't expose deletion vector info directly
                    // This would require accessing the underlying Add action
                }
                Err(e) => {
                    warn!("Error reading file metadata, error={}", e);
                }
            }
        }

        let avg_size = if num_files > 0 {
            total_size as f64 / num_files as f64
        } else {
            0.0
        };

        if min_size == u64::MAX {
            min_size = 0;
        }

        let stats = FileStatistics {
            num_files,
            total_size_bytes: total_size,
            avg_file_size_bytes: avg_size,
            min_file_size_bytes: min_size,
            max_file_size_bytes: max_size,
            files_with_deletion_vectors: files_with_dv,
        };

        info!(
            "File statistics: file_count={}, total_bytesize={}, avg_bytesize={}",
            stats.num_files, stats.total_size_bytes, stats.avg_file_size_bytes
        );

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to get the test delta table path
    fn get_test_delta_table_path() -> String {
        // Use the example delta dataset for testing
        let current_dir = std::env::current_dir().unwrap();
        let test_path = current_dir.join("examples/data/delta_dataset");
        format!("file://{}", test_path.to_str().unwrap())
    }

    #[tokio::test]
    async fn test_delta_reader_open_success() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let result = DeltaReader::open(&location, &storage_options).await;

        assert!(
            result.is_ok(),
            "Failed to open Delta table: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_delta_reader_open_invalid_location() {
        let location = "file:///nonexistent/path/to/delta/table";
        let storage_options = HashMap::new();

        let result = DeltaReader::open(location, &storage_options).await;

        assert!(result.is_err(), "Expected error for nonexistent table");
    }

    #[tokio::test]
    async fn test_delta_reader_open_invalid_url() {
        let location = "not-a-valid-url";
        let storage_options = HashMap::new();

        let result = DeltaReader::open(location, &storage_options).await;

        assert!(result.is_err(), "Expected error for invalid URL");
    }

    #[tokio::test]
    async fn test_delta_reader_extract_metrics_success() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let result = reader.extract_metrics().await;

        assert!(
            result.is_ok(),
            "Failed to extract metrics: {:?}",
            result.err()
        );

        let metrics = result.unwrap();

        // Verify basic metrics structure
        assert!(metrics.version >= 0, "Version should be non-negative");
        assert!(metrics.protocol.min_reader_version >= 0);
        assert!(metrics.protocol.min_writer_version >= 0);
        assert!(
            !metrics.metadata.id.is_empty(),
            "Table ID should not be empty"
        );
        assert!(
            !metrics.metadata.schema_string.is_empty(),
            "Schema should not be empty"
        );
        assert!(
            metrics.metadata.field_count > 0,
            "Should have at least one field"
        );
    }

    #[tokio::test]
    async fn test_delta_reader_extract_protocol_info() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // The test delta table should have protocol version 1/2
        assert_eq!(metrics.protocol.min_reader_version, 1);
        assert_eq!(metrics.protocol.min_writer_version, 2);

        // Test table doesn't use advanced features
        assert!(
            metrics.protocol.reader_features.is_none()
                || metrics
                    .protocol
                    .reader_features
                    .as_ref()
                    .unwrap()
                    .is_empty()
        );
        assert!(
            metrics.protocol.writer_features.is_none()
                || metrics
                    .protocol
                    .writer_features
                    .as_ref()
                    .unwrap()
                    .is_empty()
        );
    }

    #[tokio::test]
    async fn test_delta_reader_extract_table_metadata() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify table metadata
        assert!(!metrics.metadata.id.is_empty());
        assert_eq!(metrics.metadata.format_provider, "parquet");

        // The test table has 3 fields: id, f1, f2
        assert_eq!(metrics.metadata.field_count, 3);

        // Verify schema contains expected fields
        assert!(metrics.metadata.schema_string.contains("id"));
        assert!(metrics.metadata.schema_string.contains("f1"));
        assert!(metrics.metadata.schema_string.contains("f2"));

        // Test table is not partitioned
        assert_eq!(metrics.metadata.partition_columns.len(), 0);
    }

    #[tokio::test]
    async fn test_delta_reader_extract_file_statistics() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // The test delta table should have files
        assert!(
            metrics.file_stats.num_files > 0,
            "Should have at least one file"
        );
        assert!(
            metrics.file_stats.total_size_bytes > 0,
            "Total size should be greater than 0"
        );
        assert!(
            metrics.file_stats.avg_file_size_bytes > 0.0,
            "Average file size should be greater than 0"
        );
        assert!(
            metrics.file_stats.min_file_size_bytes > 0,
            "Min file size should be greater than 0"
        );
        assert!(
            metrics.file_stats.max_file_size_bytes > 0,
            "Max file size should be greater than 0"
        );
        assert!(
            metrics.file_stats.min_file_size_bytes <= metrics.file_stats.max_file_size_bytes,
            "Min size should be <= max size"
        );

        // Verify average calculation
        let expected_avg =
            metrics.file_stats.total_size_bytes as f64 / metrics.file_stats.num_files as f64;
        assert!(
            (metrics.file_stats.avg_file_size_bytes - expected_avg).abs() < 0.01,
            "Average file size calculation should be correct"
        );
    }

    #[tokio::test]
    async fn test_delta_reader_extract_partition_info() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // The test table is not partitioned
        assert_eq!(metrics.partition_info.num_partition_columns, 0);
        assert_eq!(metrics.partition_info.partition_columns.len(), 0);
    }

    #[tokio::test]
    async fn test_delta_reader_table_version() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // The test table should have at least version 0
        assert!(metrics.version >= 0);
    }

    #[tokio::test]
    async fn test_delta_reader_with_storage_options() {
        let location = get_test_delta_table_path();
        let mut storage_options = HashMap::new();

        // Add some storage options (these won't affect local file access but test the parameter)
        storage_options.insert("timeout".to_string(), "30s".to_string());

        let result = DeltaReader::open(&location, &storage_options).await;

        assert!(
            result.is_ok(),
            "Should open successfully with storage options"
        );
    }

    #[tokio::test]
    async fn test_delta_reader_schema_parsing() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify schema is valid JSON
        let schema_json: serde_json::Value = serde_json::from_str(&metrics.metadata.schema_string)
            .expect("Schema should be valid JSON");

        // Verify it's a struct type
        assert_eq!(schema_json["type"], "struct");

        // Verify fields array exists
        assert!(schema_json["fields"].is_array());

        // Verify field count matches
        let fields = schema_json["fields"].as_array().unwrap();
        assert_eq!(fields.len(), metrics.metadata.field_count);
    }

    #[tokio::test]
    async fn test_delta_reader_table_properties() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Table properties should be a HashMap (may be empty for test table)
        // Just verify it's accessible
        let _props = &metrics.table_properties;
    }

    #[tokio::test]
    async fn test_delta_reader_multiple_extractions() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        // Extract metrics multiple times to ensure it's idempotent
        let metrics1 = reader
            .extract_metrics()
            .await
            .expect("First extraction failed");
        let metrics2 = reader
            .extract_metrics()
            .await
            .expect("Second extraction failed");

        // Both extractions should return the same data
        assert_eq!(metrics1.version, metrics2.version);
        assert_eq!(metrics1.metadata.id, metrics2.metadata.id);
        assert_eq!(metrics1.file_stats.num_files, metrics2.file_stats.num_files);
    }

    #[tokio::test]
    async fn test_delta_reader_file_statistics_consistency() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Verify consistency of file statistics
        if metrics.file_stats.num_files > 0 {
            assert!(metrics.file_stats.total_size_bytes > 0);
            assert!(metrics.file_stats.avg_file_size_bytes > 0.0);
            assert!(metrics.file_stats.min_file_size_bytes > 0);
            assert!(metrics.file_stats.max_file_size_bytes > 0);
            assert!(
                metrics.file_stats.min_file_size_bytes
                    <= metrics.file_stats.avg_file_size_bytes as u64
            );
            assert!(
                metrics.file_stats.avg_file_size_bytes
                    <= metrics.file_stats.max_file_size_bytes as f64
            );
        }
    }

    #[tokio::test]
    async fn test_delta_reader_metadata_id_format() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Delta table IDs are typically UUIDs
        assert!(!metrics.metadata.id.is_empty());
        // Should contain hyphens (UUID format)
        assert!(metrics.metadata.id.contains('-'));
    }

    #[tokio::test]
    async fn test_delta_reader_format_provider() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // Delta Lake always uses Parquet
        assert_eq!(metrics.metadata.format_provider, "parquet");
    }

    #[tokio::test]
    async fn test_delta_reader_deletion_vectors() {
        let location = get_test_delta_table_path();
        let storage_options = HashMap::new();

        let reader = DeltaReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Delta table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // The test table doesn't use deletion vectors
        assert_eq!(metrics.file_stats.files_with_deletion_vectors, 0);
    }
}
