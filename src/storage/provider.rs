use async_trait::async_trait;
use object_store::path::Path as ObjectPath;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use super::error::StorageResult;

/// Metadata about a file in storage
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// Full path to the file
    pub path: String,

    /// File size in bytes
    pub size: u64,

    /// Last modified timestamp (if available)
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// Generic trait for cloud storage providers
///
/// This trait provides a unified interface for interacting with different
/// cloud storage providers (AWS S3, Azure Data Lake, GCS, Local filesystem).
#[allow(dead_code)]
#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Get the base path/prefix for this storage provider.
    ///
    /// # Returns
    ///
    /// A string slice containing the base path or URL prefix for this storage provider.
    fn base_path(&self) -> &str;

    /// Validate the connection to the storage provider.
    ///
    /// This performs a simple operation to ensure credentials and connectivity work.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to validate access to
    ///
    /// # Returns
    ///
    /// A `Result` indicating:
    /// * `Ok(())` - Connection is valid and accessible
    /// * `Err(StorageError)` - Connection validation failed
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Credentials are invalid or expired
    /// * Network connectivity issues occur
    /// * The specified path is not accessible
    async fn validate_connection(&self, path: &str) -> StorageResult<()>;

    /// List all files at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to list files from (relative to base_path)
    /// * `recursive` - Whether to list files recursively
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<FileMetadata>)` - Vector of file metadata for all files found
    /// * `Err(StorageError)` - If listing fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The path does not exist or is not accessible
    /// * Network or storage access errors occur
    /// * Permission denied
    async fn list_files(&self, path: &str, recursive: bool) -> StorageResult<Vec<FileMetadata>>;

    /// Discover partitions (directories) at the given path using non-recursive listing.
    ///
    /// This method uses `list_with_delimiter` to efficiently discover partition directories
    /// without recursively listing all files. This is much faster than recursive listing
    /// for tables with many files.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to discover partitions from (relative to base_path)
    /// * `exclude_prefixes` - List of prefixes to exclude from the results
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<String>)` - Vector of partition paths (directory paths)
    /// * `Err(StorageError)` - If partition discovery fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The path does not exist or is not accessible
    /// * Network or storage access errors occur
    /// * Directory listing fails
    async fn discover_partitions(
        &self,
        path: &str,
        exclude_prefixes: Vec<&str>,
    ) -> StorageResult<Vec<String>>;

    /// List files with automatic parallelization based on partition structure.
    ///
    /// This method automatically detects partitions and lists them in parallel
    /// for better performance on large tables. For tables with many partitions,
    /// this can provide 10-30x speedup compared to sequential listing.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to list files from (relative to base_path)
    /// * `partitions` - Vector of partition paths to list (obtained from `discover_partitions`)
    /// * `parallelism` - Desired level of parallelism (number of concurrent tasks)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<FileMetadata>)` - Vector of all file metadata from all partitions
    /// * `Err(StorageError)` - If any partition listing fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Any partition path is not accessible
    /// * Network or storage access errors occur
    /// * File metadata cannot be retrieved
    async fn list_files_parallel(
        &self,
        path: &str,
        partitions: Vec<String>,
        parallelism: usize,
    ) -> StorageResult<Vec<FileMetadata>>;

    /// Read the contents of a file.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file (relative to base_path)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<u8>)` - The file contents as bytes
    /// * `Err(StorageError)` - If the file cannot be read
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The file does not exist
    /// * Permission denied
    /// * Network or storage access errors occur
    async fn read_file(&self, path: &str) -> StorageResult<Vec<u8>>;

    /// Check if a file or directory exists.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to check (relative to base_path)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(true)` - The file or directory exists
    /// * `Ok(false)` - The file or directory does not exist
    /// * `Err(StorageError)` - If the existence check fails (not including NotFound)
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Network or storage access errors occur (excluding NotFound)
    /// * Permission denied
    async fn exists(&self, path: &str) -> StorageResult<bool>;

    /// Get metadata for a specific file.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file (relative to base_path)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(FileMetadata)` - The file metadata including path, size, and last modified time
    /// * `Err(StorageError)` - If metadata cannot be retrieved
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The file does not exist
    /// * Permission denied
    /// * Network or storage access errors occur
    async fn get_metadata(&self, path: &str) -> StorageResult<FileMetadata>;

    /// Get the provider-specific configuration options.
    ///
    /// # Returns
    ///
    /// A reference to the HashMap containing all configuration options.
    fn options(&self) -> &HashMap<String, String>;

    /// Get the provider-specific configuration without custom options.
    ///
    /// # Returns
    ///
    /// A HashMap containing configuration options with sensitive or custom options removed.
    fn clean_options(&self) -> HashMap<String, String>;

    /// Get a full provider-specific URL for a path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to convert to a full URL
    ///
    /// # Returns
    ///
    /// A String containing the full provider-specific URL (e.g., "s3://bucket/path", "file:///path").
    fn uri_from_path(&self, path: &str) -> String;
}

impl Debug for dyn StorageProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "StorageProvider(base_path={})", self.base_path())
    }
}

/// Helper function to create an ObjectPath from a string
pub(crate) fn string_to_path(s: &str) -> ObjectPath {
    ObjectPath::from(s)
}
