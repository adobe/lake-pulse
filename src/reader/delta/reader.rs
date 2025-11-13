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
