// Copyright 2022 Adobe. All rights reserved.
// This file is licensed to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
// OF ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_file_metadata_creation() {
        let metadata = FileMetadata {
            path: "/path/to/file.txt".to_string(),
            size: 1024,
            last_modified: None,
        };

        assert_eq!(metadata.path, "/path/to/file.txt");
        assert_eq!(metadata.size, 1024);
        assert!(metadata.last_modified.is_none());
    }

    #[test]
    fn test_file_metadata_with_timestamp() {
        let now = Utc::now();
        let metadata = FileMetadata {
            path: "/path/to/file.txt".to_string(),
            size: 2048,
            last_modified: Some(now),
        };

        assert_eq!(metadata.path, "/path/to/file.txt");
        assert_eq!(metadata.size, 2048);
        assert!(metadata.last_modified.is_some());
        assert_eq!(metadata.last_modified.unwrap(), now);
    }

    #[test]
    fn test_file_metadata_clone() {
        let metadata1 = FileMetadata {
            path: "/path/to/file.txt".to_string(),
            size: 512,
            last_modified: None,
        };

        let metadata2 = metadata1.clone();
        assert_eq!(metadata1.path, metadata2.path);
        assert_eq!(metadata1.size, metadata2.size);
        assert_eq!(metadata1.last_modified, metadata2.last_modified);
    }

    #[test]
    fn test_file_metadata_debug() {
        let metadata = FileMetadata {
            path: "/test/file.txt".to_string(),
            size: 100,
            last_modified: None,
        };

        let debug_str = format!("{:?}", metadata);
        assert!(debug_str.contains("FileMetadata"));
        assert!(debug_str.contains("/test/file.txt"));
        assert!(debug_str.contains("100"));
    }

    #[test]
    fn test_string_to_path() {
        let path_str = "path/to/file.txt";
        let object_path = string_to_path(path_str);

        // Verify it creates a valid ObjectPath
        assert_eq!(object_path.as_ref(), path_str);
    }

    #[test]
    fn test_string_to_path_empty() {
        let path_str = "";
        let object_path = string_to_path(path_str);
        assert_eq!(object_path.as_ref(), "");
    }

    #[test]
    fn test_string_to_path_with_slashes() {
        let path_str = "a/b/c/d/file.parquet";
        let object_path = string_to_path(path_str);
        assert_eq!(object_path.as_ref(), path_str);
    }

    #[test]
    fn test_file_metadata_large_size() {
        let metadata = FileMetadata {
            path: "/large/file.dat".to_string(),
            size: u64::MAX,
            last_modified: None,
        };

        assert_eq!(metadata.size, u64::MAX);
    }

    #[test]
    fn test_file_metadata_zero_size() {
        let metadata = FileMetadata {
            path: "/empty/file.txt".to_string(),
            size: 0,
            last_modified: None,
        };

        assert_eq!(metadata.size, 0);
    }

    #[test]
    fn test_storage_provider_debug() {
        // Create a mock storage provider to test the Debug trait
        use std::sync::OnceLock;

        struct MockProvider {
            options: OnceLock<HashMap<String, String>>,
        }

        impl MockProvider {
            fn new() -> Self {
                Self {
                    options: OnceLock::new(),
                }
            }
        }

        #[async_trait]
        impl StorageProvider for MockProvider {
            fn base_path(&self) -> &str {
                "/mock/base/path"
            }

            async fn validate_connection(&self, _path: &str) -> StorageResult<()> {
                Ok(())
            }

            async fn list_files(
                &self,
                _path: &str,
                _recursive: bool,
            ) -> StorageResult<Vec<FileMetadata>> {
                Ok(vec![])
            }

            async fn discover_partitions(
                &self,
                _path: &str,
                _exclude_prefixes: Vec<&str>,
            ) -> StorageResult<Vec<String>> {
                Ok(vec![])
            }

            async fn list_files_parallel(
                &self,
                _path: &str,
                _partitions: Vec<String>,
                _parallelism: usize,
            ) -> StorageResult<Vec<FileMetadata>> {
                Ok(vec![])
            }

            async fn read_file(&self, _path: &str) -> StorageResult<Vec<u8>> {
                Ok(vec![])
            }

            async fn exists(&self, _path: &str) -> StorageResult<bool> {
                Ok(true)
            }

            async fn get_metadata(&self, _path: &str) -> StorageResult<FileMetadata> {
                Ok(FileMetadata {
                    path: "test".to_string(),
                    size: 0,
                    last_modified: None,
                })
            }

            fn options(&self) -> &HashMap<String, String> {
                self.options.get_or_init(|| HashMap::new())
            }

            fn clean_options(&self) -> HashMap<String, String> {
                HashMap::new()
            }

            fn uri_from_path(&self, path: &str) -> String {
                path.to_string()
            }
        }

        let provider: &dyn StorageProvider = &MockProvider::new();
        let debug_str = format!("{:?}", provider);
        assert!(debug_str.contains("StorageProvider"));
        assert!(debug_str.contains("/mock/base/path"));
    }
}
