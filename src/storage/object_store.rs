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

use super::config::{StorageConfig, StorageType};
use super::error::{StorageError, StorageResult};
use super::provider::{string_to_path, FileMetadata, StorageProvider};
use crate::util::retry::retry_with_max_retries;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;
use hdfs_native_object_store::HdfsObjectStoreBuilder;
use object_store::{
    aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem, ClientOptions, ObjectStore, ObjectStoreExt, RetryConfig,
};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Generic storage provider that works with any object_store backend
pub struct ObjectStoreProvider {
    pub config: StorageConfig,
    pub store: Arc<dyn ObjectStore>,
    pub base_path: String,
}

impl ObjectStoreProvider {
    /// Create a new generic storage provider from configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration specifying the storage type and options
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(GenericStorageProvider)` - A configured storage provider ready to use
    /// * `Err(StorageError)` - If the storage backend cannot be initialized
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The storage configuration is invalid
    /// * Required configuration options are missing
    /// * The storage backend cannot be created (e.g., invalid credentials, network issues)
    pub async fn new(config: StorageConfig) -> StorageResult<Self> {
        let (store, base_path) = Self::build_store(&config).await?;

        Ok(Self {
            config,
            store: Arc::new(store),
            base_path,
        })
    }

    /// Build the appropriate object store based on configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration specifying the storage type and options
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok((Box<dyn ObjectStore>, String))` - A tuple of the object store and base path/URL
    /// * `Err(StorageError)` - If the object store cannot be built
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The storage type is not supported
    /// * Required configuration options are missing for the storage type
    /// * The object store backend cannot be initialized
    async fn build_store(config: &StorageConfig) -> StorageResult<(Box<dyn ObjectStore>, String)> {
        match config.storage_type {
            StorageType::Local => Self::build_local_store(config),
            StorageType::Aws => Self::build_aws_store(config),
            StorageType::Azure => Self::build_azure_store(config),
            StorageType::Gcs => Self::build_gcs_store(config),
            StorageType::Hdfs => Self::build_hdfs_store(config),
        }
    }

    /// Build a local filesystem store.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration with 'path' option specifying the local directory
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok((Box<dyn ObjectStore>, String))` - A tuple of the local filesystem store and canonical path
    /// * `Err(StorageError)` - If the local store cannot be created
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The 'path' option is missing from configuration
    /// * The path cannot be canonicalized (doesn't exist or permission denied)
    /// * The path is not a directory
    /// * The local filesystem store cannot be created
    fn build_local_store(config: &StorageConfig) -> StorageResult<(Box<dyn ObjectStore>, String)> {
        let path = config.options.get("path").ok_or_else(|| {
            StorageError::ConfigError("Local storage requires 'path' option".to_string())
        })?;
        let base_path = PathBuf::from(path);

        // Canonicalize the path (handles both relative and absolute paths, resolves symlinks)
        let canonical_path = base_path.canonicalize().map_err(|e| {
            StorageError::ConfigError(format!(
                "Failed to resolve path '{}': {} (path must exist)",
                path, e
            ))
        })?;

        if !canonical_path.is_dir() {
            return Err(StorageError::ConfigError(format!(
                "Base path is not a directory: {}",
                canonical_path.display()
            )));
        }

        let store = LocalFileSystem::new_with_prefix(&canonical_path).map_err(|e| {
            StorageError::ConfigError(format!("Failed to create local store: {}", e))
        })?;

        let base_path_str = canonical_path.to_string_lossy().to_string();
        Ok((Box::new(store), base_path_str))
    }

    /// Build connection options from configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration with optional timeout and connection settings
    ///
    /// # Returns
    ///
    /// A `ClientOptions` instance configured with timeout and connection settings from the config.
    fn build_connection_options(config: &StorageConfig) -> ClientOptions {
        let mut client_options = ClientOptions::default();
        if let Some(timeout_str) = config.options.get("timeout") {
            if timeout_str == "0" || timeout_str == "disabled" {
                client_options = client_options.with_timeout_disabled();
            } else if let Ok(sec) = timeout_str.parse::<u64>() {
                client_options = client_options.with_timeout(Duration::from_secs(sec))
            }
        };
        if let Some(connect_timeout_str) = config.options.get("connect_timeout") {
            if connect_timeout_str == "0" || connect_timeout_str == "disabled" {
                client_options = client_options.with_connect_timeout_disabled();
            } else if let Ok(ms) = connect_timeout_str.parse::<u64>() {
                client_options = client_options.with_connect_timeout(Duration::from_secs(ms))
            }
        }
        if let Some(pool_idle_timeout_str) = config.options.get("pool_idle_timeout") {
            if let Ok(sec) = pool_idle_timeout_str.parse::<u64>() {
                client_options = client_options.with_pool_idle_timeout(Duration::from_secs(sec))
            }
        }
        if let Some(pool_max_idle_per_host_str) = config.options.get("pool_max_idle_per_host") {
            if let Ok(max_idle) = pool_max_idle_per_host_str.parse::<usize>() {
                client_options = client_options.with_pool_max_idle_per_host(max_idle)
            }
        }
        client_options
    }

    /// Build retry options from configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration with optional max_retries and retry_timeout settings
    ///
    /// # Returns
    ///
    /// A `RetryConfig` instance configured with retry settings from the config.
    fn build_retry_options(config: &StorageConfig) -> RetryConfig {
        let default_retry_config = RetryConfig::default();
        let max_retries = config
            .options
            .get("max_retries")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(default_retry_config.max_retries);
        let retry_timeout = config
            .options
            .get("retry_timeout")
            .and_then(|s| Some(Duration::from_secs(s.parse::<u64>().ok()?)))
            .unwrap_or(default_retry_config.retry_timeout);
        RetryConfig {
            backoff: Default::default(),
            max_retries,
            retry_timeout,
        }
    }

    /// Get max retries from config.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration with optional max_retries setting
    ///
    /// # Returns
    ///
    /// The maximum number of retries (defaults to 10 if not specified).
    fn get_max_retries(config: &StorageConfig) -> usize {
        config
            .options
            .get("max_retries")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10)
    }

    /// Retry wrapper for operations that may fail due to transient network errors.
    ///
    /// # Arguments
    ///
    /// * `operation_name` - Name of the operation for logging purposes
    /// * `operation` - The async operation to retry
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(T)` - The successful result from the operation
    /// * `Err(StorageError)` - If all retry attempts fail
    async fn retry_operation<F, Fut, T>(
        &self,
        operation_name: &str,
        operation: F,
    ) -> StorageResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = StorageResult<T>>,
    {
        let max_retries = Self::get_max_retries(&self.config);
        retry_with_max_retries(max_retries, operation_name, operation).await
    }

    /// Build an AWS S3 store.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration with AWS S3 options (bucket, region, credentials, etc.)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok((Box<dyn ObjectStore>, String))` - A tuple of the S3 store and base S3 URL
    /// * `Err(StorageError)` - If the S3 store cannot be created
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Required S3 configuration options are missing
    /// * AWS credentials are invalid
    /// * The S3 store cannot be initialized
    fn build_aws_store(config: &StorageConfig) -> StorageResult<(Box<dyn ObjectStore>, String)> {
        let mut builder = AmazonS3Builder::new()
            .with_client_options(Self::build_connection_options(config))
            .with_retry(Self::build_retry_options(config));
        let mut bucket: Option<&String> = None;
        let mut endpoint: Option<&String> = None;

        // Apply configuration options
        for (key, value) in &config.options {
            match key.as_str() {
                "bucket" => {
                    bucket = Some(value);
                    builder = builder.with_bucket_name(value);
                }
                "region" => builder = builder.with_region(value),
                "access_key_id" => builder = builder.with_access_key_id(value),
                "secret_access_key" => builder = builder.with_secret_access_key(value),
                "session_token" | "token" => builder = builder.with_token(value),
                "endpoint" => {
                    endpoint = Some(value);
                    builder = builder.with_endpoint(value);
                }
                "allow_http" => {
                    if value.to_lowercase() == "true" {
                        builder = builder.with_allow_http(true);
                    }
                }
                // Already handled by `build_connection_options` and `build_retry_options`
                "timeout"
                | "connect_timeout"
                | "max_retries"
                | "retry_timeout"
                | "pool_idle_timeout"
                | "pool_max_idle_per_host" => (),
                _ => {
                    // Ignore unknown options or log a warning
                    tracing::warn!("Unknown AWS S3 option: {}", key);
                }
            }
        }

        let store = builder
            .build()
            .map_err(|e| StorageError::ConfigError(format!("Failed to create S3 store: {}", e)))?;

        // Construct base URL
        let base_url = if let Some(endpoint_url) = endpoint {
            // If custom endpoint is provided, use it
            endpoint_url.trim_end_matches('/').to_string()
        } else if let Some(bucket_name) = bucket {
            // Standard S3 URL format
            format!("s3://{}", bucket_name)
        } else {
            // No bucket specified (unusual but possible)
            "s3://".to_string()
        };

        Ok((Box::new(store), base_url))
    }

    /// Build an Azure store.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration with Azure options (account, container, credentials, etc.)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok((Box<dyn ObjectStore>, String))` - A tuple of the Azure store and base Azure URL
    /// * `Err(StorageError)` - If the Azure store cannot be created
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Required Azure configuration options are missing
    /// * Azure credentials are invalid
    /// * The Azure store cannot be initialized
    fn build_azure_store(config: &StorageConfig) -> StorageResult<(Box<dyn ObjectStore>, String)> {
        let mut builder = MicrosoftAzureBuilder::new()
            .with_client_options(Self::build_connection_options(config))
            .with_retry(Self::build_retry_options(config));

        // Account name is required for Azure
        let mut account_name = config.get_option("account_name").ok_or_else(|| {
            StorageError::ConfigError("Azure requires 'account_name' option".to_string())
        })?;
        let mut container = config.get_option("container").ok_or_else(|| {
            StorageError::ConfigError("Azure requires 'container' option".to_string())
        })?;

        builder = builder.with_account(account_name);

        // Track if we should use fabric endpoint
        let mut use_fabric_endpoint = false;
        let mut custom_endpoint: Option<&String> = None;

        // Apply configuration options
        for (key, value) in &config.options {
            match key.as_str() {
                "container" => {
                    container = value;
                    builder = builder.with_container_name(value)
                }
                "account_name" => {
                    account_name = value;
                    builder = builder.with_account(value)
                }
                "access_key" | "account_key" => builder = builder.with_access_key(value),
                "sas_token" => {
                    // Parse SAS token query parameters
                    let pairs: Vec<(String, String)> = value
                        .trim_start_matches('?')
                        .split('&')
                        .filter_map(|pair| {
                            let mut parts = pair.split('=');
                            match (parts.next(), parts.next()) {
                                (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                                _ => None,
                            }
                        })
                        .collect();
                    builder = builder.with_sas_authorization(pairs);
                }
                "tenant_id" => builder = builder.with_tenant_id(value),
                "client_id" => builder = builder.with_client_id(value),
                "client_secret" => builder = builder.with_client_secret(value),
                "use_fabric_endpoint" => {
                    use_fabric_endpoint = value.to_lowercase() == "true";
                    builder = builder.with_use_fabric_endpoint(use_fabric_endpoint);
                }
                "endpoint" => {
                    custom_endpoint = Some(value);
                    builder = builder.with_endpoint(value.clone());
                }
                // Already handled by `build_connection_options` and `build_retry_options`
                "timeout"
                | "connect_timeout"
                | "max_retries"
                | "retry_timeout"
                | "pool_idle_timeout"
                | "pool_max_idle_per_host" => (),
                _ => {
                    // Ignore unknown options or log a warning
                    tracing::info!("Unknown Azure option: {}", key);
                }
            }
        }

        let store = builder.build().map_err(|e| {
            StorageError::ConfigError(format!("Failed to create Azure store: {}", e))
        })?;

        // Construct base URL
        // Format: abfss://<container>@<account>.<endpoint>/
        let base_url = if let Some(endpoint) = custom_endpoint {
            // If custom endpoint is provided, use it
            endpoint.trim_end_matches('/').to_string()
        } else {
            // Determine the endpoint based on configuration
            let endpoint_domain = if use_fabric_endpoint {
                "dfs.fabric.microsoft.com"
            } else {
                "dfs.core.windows.net"
            };

            // Use abfss:// (secure) protocol with fully qualified domain
            format!("abfss://{}@{}.{}", container, account_name, endpoint_domain)
        };

        Ok((Box::new(store), base_url))
    }

    /// Build a GCS store.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration with GCS options (bucket, credentials, etc.)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok((Box<dyn ObjectStore>, String))` - A tuple of the GCS store and base GCS URL
    /// * `Err(StorageError)` - If the GCS store cannot be created
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Required GCS configuration options are missing
    /// * GCS credentials are invalid
    /// * The GCS store cannot be initialized
    fn build_gcs_store(config: &StorageConfig) -> StorageResult<(Box<dyn ObjectStore>, String)> {
        let mut builder = GoogleCloudStorageBuilder::new()
            .with_client_options(Self::build_connection_options(config))
            .with_retry(Self::build_retry_options(config));
        let mut bucket: Option<&String> = None;

        // Apply configuration options
        for (key, value) in &config.options {
            match key.as_str() {
                "bucket" => {
                    bucket = Some(value);
                    builder = builder.with_bucket_name(value);
                }
                "service_account_key_path" => builder = builder.with_service_account_path(value),
                "service_account_key" => builder = builder.with_service_account_key(value),
                // Already handled by `build_connection_options` and `build_retry_options`
                "timeout"
                | "connect_timeout"
                | "max_retries"
                | "retry_timeout"
                | "pool_idle_timeout"
                | "pool_max_idle_per_host" => (),
                _ => {
                    // Ignore unknown options or log a warning
                    tracing::warn!("Unknown GCS option: {}", key);
                }
            }
        }

        let store = builder
            .build()
            .map_err(|e| StorageError::ConfigError(format!("Failed to create GCS store: {}", e)))?;

        // Construct base URL
        let base_url = if let Some(bucket_name) = bucket {
            format!("gs://{}", bucket_name)
        } else {
            // No bucket specified (unusual but possible)
            "gs://".to_string()
        };

        Ok((Box::new(store), base_url))
    }

    /// Build an HDFS store.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration with HDFS options (url)
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok((Box<dyn ObjectStore>, String))` - A tuple of the HDFS store and base HDFS URL
    /// * `Err(StorageError)` - If the HDFS store cannot be created
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The 'url' option is missing from configuration
    /// * The HDFS store cannot be initialized (e.g., invalid URL, connection issues)
    fn build_hdfs_store(config: &StorageConfig) -> StorageResult<(Box<dyn ObjectStore>, String)> {
        let url = config.options.get("url").ok_or_else(|| {
            StorageError::ConfigError("HDFS storage requires 'url' option".to_string())
        })?;

        let store = HdfsObjectStoreBuilder::new()
            .with_url(url)
            .build()
            .map_err(|e| {
                StorageError::ConfigError(format!("Failed to create HDFS store: {}", e))
            })?;

        Ok((Box::new(store), url.clone()))
    }

    /// List multiple partitions in parallel with bounded concurrency.
    ///
    /// This method lists each partition concurrently, which can provide significant
    /// speedup for tables with many partitions.
    ///
    /// # Arguments
    ///
    /// * `partitions` - Vector of partition paths to list
    /// * `parallelism` - Maximum number of concurrent partition listing operations
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Vec<FileMetadata>)` - All files from all partitions combined
    /// * `Err(StorageError)` - If any partition listing fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Any partition cannot be listed
    /// * Network or storage access errors occur
    /// * File metadata cannot be retrieved
    async fn list_partitions_parallel(
        &self,
        partitions: Vec<String>,
        parallelism: usize,
    ) -> StorageResult<Vec<FileMetadata>> {
        use futures::stream;

        let parallelism = parallelism.max(1);

        let store = Arc::clone(&self.store);
        let config = self.config.clone();

        // List each partition in parallel
        let results: Vec<StorageResult<Vec<FileMetadata>>> = stream::iter(partitions)
            .map(|partition_path| {
                info!("Listing partition={}", &partition_path);
                let store_clone = Arc::clone(&store);
                let max_retries = Self::get_max_retries(&config);
                async move {
                    // Use the retry helper function
                    retry_with_max_retries(
                        max_retries,
                        &format!("list_partition({})", partition_path),
                        || async {
                            let mut files = Vec::new();
                            let path = string_to_path(&partition_path);
                            let mut stream = store_clone.list(Some(&path));

                            while let Some(meta) = stream.next().await {
                                let meta = meta?;
                                files.push(FileMetadata {
                                    path: meta.location.to_string(),
                                    size: meta.size,
                                    last_modified: Some(meta.last_modified),
                                });
                            }

                            info!(
                                "Listed partition={}, found count={} files",
                                string_to_path(&partition_path),
                                files.len()
                            );

                            Ok(files)
                        },
                    )
                    .await
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        info!(
            "Collected count={} first level partitions, parallelism={}",
            results.len(),
            parallelism
        );

        // Flatten results and handle errors
        let mut all_files = Vec::new();
        for result in results {
            all_files.extend(result?);
        }

        info!(
            "Found count={} total files across all partitions, parallelism={}",
            all_files.len(),
            parallelism,
        );

        Ok(all_files)
    }
}

#[async_trait]
impl StorageProvider for ObjectStoreProvider {
    fn base_path(&self) -> &str {
        &self.base_path
    }

    async fn validate_connection(&self, path: &str) -> StorageResult<()> {
        // For local filesystem, check if the base path is accessible
        if self.config.storage_type == StorageType::Local {
            let mut path_url = PathBuf::from(&self.base_path);
            path_url.push(path);
            return if path_url.exists() && path_url.is_dir() {
                Ok(())
            } else {
                Err(StorageError::ConnectionError(format!(
                    "Base path is not accessible: {}",
                    path
                )))
            };
        }

        // For cloud providers, try to list objects at the root to validate connection
        self.store.list_with_delimiter(None).await?;
        Ok(())
    }

    async fn list_files(&self, path: &str, recursive: bool) -> StorageResult<Vec<FileMetadata>> {
        let path_str = path.to_string();
        let store = Arc::clone(&self.store);

        self.retry_operation(&format!("list_files({})", path), || async {
            let object_path = if path_str.is_empty() {
                None
            } else {
                Some(string_to_path(&path_str))
            };

            let mut files = Vec::new();

            if recursive {
                let mut stream = store.list(object_path.as_ref());

                while let Some(meta) = stream.next().await {
                    let meta = meta?;
                    files.push(FileMetadata {
                        path: meta.location.to_string(),
                        size: meta.size,
                        last_modified: Some(meta.last_modified),
                    });
                }
            } else {
                let list_result = store.list_with_delimiter(object_path.as_ref()).await?;

                for meta in list_result.objects {
                    files.push(FileMetadata {
                        path: meta.location.to_string(),
                        size: meta.size,
                        last_modified: Some(meta.last_modified),
                    });
                }
            }

            Ok(files)
        })
        .await
    }

    async fn discover_partitions(
        &self,
        path: &str,
        exclude_prefixes: Vec<&str>,
    ) -> StorageResult<Vec<String>> {
        let path_str = path.to_string();
        let exclude_prefixes_owned: Vec<String> =
            exclude_prefixes.iter().map(|s| s.to_string()).collect();
        let store = Arc::clone(&self.store);

        self.retry_operation(&format!("discover_partitions({})", path), || async {
            let object_path = if path_str.is_empty() {
                None
            } else {
                Some(string_to_path(&path_str))
            };

            let list_result = store.list_with_delimiter(object_path.as_ref()).await?;

            // Extract directory paths from common_prefixes
            let partitions: Vec<String> = list_result
                .common_prefixes
                .iter()
                .map(|prefix| prefix.to_string())
                .filter(|prefix| !exclude_prefixes_owned.iter().any(|p| prefix.contains(p)))
                .collect();

            Ok(partitions)
        })
        .await
    }

    async fn list_files_parallel(
        &self,
        path: &str,
        partitions: Vec<String>,
        parallelism: usize,
    ) -> StorageResult<Vec<FileMetadata>> {
        let path_str = path.to_string();
        let store = Arc::clone(&self.store);

        // List files at the root path with retry
        let mut all_files: Vec<FileMetadata> = self
            .retry_operation(&format!("list_files_parallel_root({})", path), || async {
                let object_path = if path_str.is_empty() {
                    None
                } else {
                    Some(string_to_path(&path_str))
                };

                let list_result = store.list_with_delimiter(object_path.as_ref()).await?;

                let files: Vec<FileMetadata> = list_result
                    .objects
                    .into_iter()
                    .map(|meta| FileMetadata {
                        path: meta.location.to_string(),
                        size: meta.size,
                        last_modified: Some(meta.last_modified),
                    })
                    .collect();

                Ok(files)
            })
            .await?;

        if !partitions.is_empty() {
            info!(
                "Found count={} first level partitions at location={}, now listing with parallelism={}",
                partitions.len(),
                path,
                parallelism
            );

            // Parallel listing of partitions (already has retry logic)
            let partition_files = self
                .list_partitions_parallel(partitions.clone(), parallelism)
                .await?;
            all_files.extend(partition_files);
        } else {
            // No partitions found, use standard recursive list with retry
            tracing::debug!(
                "No partitions found at location={}, using standard recursive listing",
                path
            );

            let recursive_files = self
                .retry_operation(
                    &format!("list_files_parallel_recursive({})", path),
                    || async {
                        let object_path = if path_str.is_empty() {
                            None
                        } else {
                            Some(string_to_path(&path_str))
                        };

                        let mut files = Vec::new();
                        let mut stream = store.list(object_path.as_ref());
                        while let Some(meta) = stream.next().await {
                            let meta = meta?;
                            files.push(FileMetadata {
                                path: meta.location.to_string(),
                                size: meta.size,
                                last_modified: Some(meta.last_modified),
                            });
                        }
                        Ok(files)
                    },
                )
                .await?;

            all_files.extend(recursive_files);
        }

        info!(
            "Found count={} partitions and file_count={} at location={}",
            partitions.len(),
            all_files.len(),
            path
        );

        Ok(all_files)
    }

    async fn read_file(&self, path: &str) -> StorageResult<Vec<u8>> {
        let object_path = string_to_path(path);
        let result = self.store.get(&object_path).await?;
        let bytes: Bytes = result.bytes().await?;
        Ok(bytes.to_vec())
    }

    async fn exists(&self, path: &str) -> StorageResult<bool> {
        let object_path = string_to_path(path);
        match self.store.head(&object_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_metadata(&self, path: &str) -> StorageResult<FileMetadata> {
        let object_path = string_to_path(path);
        let meta = self.store.head(&object_path).await?;

        Ok(FileMetadata {
            path: meta.location.to_string(),
            size: meta.size as u64,
            last_modified: Some(meta.last_modified),
        })
    }

    fn options(&self) -> &HashMap<String, String> {
        &self.config.options
    }

    fn clean_options(&self) -> HashMap<String, String> {
        self.config
            .options
            .clone()
            .into_iter()
            .filter(|(k, _)| {
                ![
                    "retry_timeout",
                    "timeout",
                    "connect_timeout",
                    "max_retries",
                    "pool_idle_timeout",
                    "pool_max_idle_per_host",
                ]
                .contains(&k.as_str())
            })
            .collect::<HashMap<_, _>>()
    }

    fn uri_from_path(&self, path: &str) -> String {
        fn fix_uri(storage_type: &StorageType, path: &str) -> String {
            if storage_type == &StorageType::Local {
                // Normalize file:// URIs to canonical format
                // Handle paths like "file:///path", "file://path", "file:/path", or "/path"
                // Also convert backslashes to forward slashes for Windows compatibility
                let path = path.replace('\\', "/");

                // Remove Windows extended-length path prefix (\\?\ or //?/)
                // This prefix is added by canonicalize() on Windows
                let path = path.strip_prefix("//?/").unwrap_or(&path).to_string();

                let path_without_scheme = if let Some(without_scheme) = path.strip_prefix("file:") {
                    // Had file: prefix - strip it and any leading slashes
                    without_scheme.trim_start_matches('/').to_string()
                } else if path.starts_with('/') {
                    // Unix absolute path - remove leading slash, we'll add it back
                    path.trim_start_matches('/').to_string()
                } else {
                    // Windows absolute path (C:/...) or relative path
                    path.to_string()
                };

                // file:// URIs require three slashes before the path:
                // - Unix: file:///home/user/... (file:// + / + home/user/...)
                // - Windows: file:///C:/Users/... (file:// + / + C:/Users/...)
                format!("file:///{}", path_without_scheme)
            } else {
                path.to_string()
            }
        }

        let fp = if path.contains(&self.base_path) {
            path.to_string()
        } else {
            format!("{}/{}", self.base_path, path.trim_start_matches('/'))
        };

        fix_uri(&self.config.storage_type, fp.as_str())
    }
}

impl Debug for ObjectStoreProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageProvider(type=generic, cloud_provider={}, config={:?})",
            self.config.storage_type_str(),
            self.config
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_build_connection_options_default() {
        let config = StorageConfig::local();
        let _options = ObjectStoreProvider::build_connection_options(&config);
        // No assertion, just make sure is does not panic
    }

    #[test]
    fn test_build_connection_options_with_timeout() {
        let config = StorageConfig::local()
            .with_option("timeout", "60")
            .with_option("connect_timeout", "10");

        let _options = ObjectStoreProvider::build_connection_options(&config);
        // No assertion, just make sure is does not panic
    }

    #[test]
    fn test_build_connection_options_disabled_timeout() {
        let config = StorageConfig::local()
            .with_option("timeout", "disabled")
            .with_option("connect_timeout", "0");

        let _options = ObjectStoreProvider::build_connection_options(&config);
        // No assertion, just make sure is does not panic
    }

    #[test]
    fn test_build_connection_options_with_pool_settings() {
        let config = StorageConfig::local()
            .with_option("pool_idle_timeout", "30")
            .with_option("pool_max_idle_per_host", "10");

        let _options = ObjectStoreProvider::build_connection_options(&config);
        // No assertion, just make sure is does not panic
    }

    #[test]
    fn test_build_connection_options_invalid_values() {
        let config = StorageConfig::local()
            .with_option("timeout", "invalid")
            .with_option("pool_max_idle_per_host", "not_a_number");

        // Should handle invalid values gracefully
        let _options = ObjectStoreProvider::build_connection_options(&config);
        // No assertion, just make sure is does not panic
    }

    #[test]
    fn test_build_retry_options_default() {
        let config = StorageConfig::local();
        let retry_config = ObjectStoreProvider::build_retry_options(&config);

        // Should use default values from config
        assert!(retry_config.max_retries > 0);
    }

    #[test]
    fn test_build_retry_options_custom() {
        let config = StorageConfig::local()
            .with_option("max_retries", "5")
            .with_option("retry_timeout", "300");

        let retry_config = ObjectStoreProvider::build_retry_options(&config);
        assert_eq!(retry_config.max_retries, 5);
        assert_eq!(retry_config.retry_timeout, Duration::from_secs(300));
    }

    #[test]
    fn test_build_retry_options_invalid_values() {
        let config = StorageConfig::local()
            .with_option("max_retries", "invalid")
            .with_option("retry_timeout", "not_a_number");

        let retry_config = ObjectStoreProvider::build_retry_options(&config);
        // Should fall back to defaults
        assert!(retry_config.max_retries > 0);
    }

    #[test]
    fn test_get_max_retries_default() {
        let config = StorageConfig::local();
        let max_retries = ObjectStoreProvider::get_max_retries(&config);

        // Should return default value from config (20) or fallback (10)
        // StorageConfig::local() sets default_options which includes max_retries=20
        assert_eq!(max_retries, 20);
    }

    #[test]
    fn test_get_max_retries_custom() {
        let config = StorageConfig::local().with_option("max_retries", "15");
        let max_retries = ObjectStoreProvider::get_max_retries(&config);

        assert_eq!(max_retries, 15);
    }

    #[test]
    fn test_get_max_retries_invalid() {
        let config = StorageConfig::local().with_option("max_retries", "invalid");
        let max_retries = ObjectStoreProvider::get_max_retries(&config);

        // Should fall back to default
        assert_eq!(max_retries, 10);
    }

    #[tokio::test]
    async fn test_new_local_provider() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await;

        assert!(provider.is_ok());
        let provider = provider.unwrap();
        // On Windows, canonicalize() adds \\?\ prefix and converts 8.3 short names to long names
        // (e.g., RUNNER~1 -> runneradmin), so we need to canonicalize both paths for comparison
        let base_path = provider.base_path.replace("\\\\?\\", "").replace('\\', "/");
        let canonical_temp = temp_dir
            .path()
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
            .replace("\\\\?\\", "")
            .replace('\\', "/");
        assert!(
            base_path.contains(&canonical_temp),
            "base_path '{}' should contain '{}'",
            base_path,
            canonical_temp
        );
        assert_eq!(provider.config.storage_type, StorageType::Local);
    }

    #[tokio::test]
    async fn test_new_local_provider_invalid_path() {
        let config = StorageConfig::local().with_option("path", "/nonexistent/invalid/path");
        let provider = ObjectStoreProvider::new(config).await;

        assert!(provider.is_err());
        match provider {
            Err(StorageError::ConfigError(msg)) => {
                assert!(msg.contains("Failed to resolve path"));
            }
            _ => panic!("Expected ConfigError"),
        }
    }

    #[tokio::test]
    async fn test_new_local_provider_missing_path() {
        let config = StorageConfig::local();
        let provider = ObjectStoreProvider::new(config).await;

        assert!(provider.is_err());
        match provider {
            Err(StorageError::ConfigError(msg)) => {
                assert!(msg.contains("path"));
            }
            _ => panic!("Expected ConfigError for missing path"),
        }
    }

    #[tokio::test]
    async fn test_new_local_provider_file_not_directory() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        fs::write(&file_path, "test content").unwrap();

        let config = StorageConfig::local().with_option("path", file_path.to_str().unwrap());
        let provider = ObjectStoreProvider::new(config).await;

        assert!(provider.is_err());
        match provider {
            Err(StorageError::ConfigError(msg)) => {
                assert!(msg.contains("not a directory"));
            }
            _ => panic!("Expected ConfigError for file instead of directory"),
        }
    }

    #[tokio::test]
    async fn test_provider_base_path() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let base_path = provider.base_path();
        assert!(!base_path.is_empty());
        // On Windows, canonicalize() adds \\?\ prefix and converts 8.3 short names to long names
        // (e.g., RUNNER~1 -> runneradmin), so we need to canonicalize both paths for comparison
        let normalized_base_path = base_path.replace("\\\\?\\", "").replace('\\', "/");
        let canonical_temp = temp_dir
            .path()
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
            .replace("\\\\?\\", "")
            .replace('\\', "/");
        assert!(
            normalized_base_path.contains(&canonical_temp),
            "base_path '{}' should contain '{}'",
            normalized_base_path,
            canonical_temp
        );
    }

    #[tokio::test]
    async fn test_provider_options() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local()
            .with_option("path", temp_path)
            .with_option("custom_option", "custom_value");
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let options = provider.options();
        assert!(options.contains_key("path"));
        assert!(options.contains_key("custom_option"));
        assert_eq!(options.get("custom_option").unwrap(), "custom_value");
    }

    #[tokio::test]
    async fn test_provider_clean_options() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local()
            .with_option("path", temp_path)
            .with_option("timeout", "60")
            .with_option("max_retries", "10")
            .with_option("custom_option", "custom_value");
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let clean_options = provider.clean_options();

        // Clean options should exclude internal retry/timeout options
        assert!(!clean_options.contains_key("timeout"));
        assert!(!clean_options.contains_key("max_retries"));
        assert!(!clean_options.contains_key("retry_timeout"));
        assert!(!clean_options.contains_key("connect_timeout"));

        // But should include custom options
        assert!(clean_options.contains_key("path"));
        assert!(clean_options.contains_key("custom_option"));
    }

    #[tokio::test]
    async fn test_uri_from_path_local() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let uri = provider.uri_from_path("test/file.txt");
        assert!(uri.starts_with("file://"));
        assert!(uri.contains("test"));
        assert!(uri.contains("file.txt"));

        // Verify the URI format is valid for delta-rs
        // On Unix: file:///tmp/... (file:// + /tmp/...)
        // On Windows: file:///C:/... (file:// + / + C:/...)
        #[cfg(unix)]
        {
            // Unix absolute paths start with /, so URI should have file:// followed by /
            assert!(
                uri.starts_with("file:///"),
                "Unix absolute path URI should start with file:/// but got: {}",
                uri
            );
        }

        // URIs should never contain backslashes (even on Windows)
        assert!(
            !uri.contains('\\'),
            "URI should use forward slashes only, but got: {}",
            uri
        );
    }

    #[tokio::test]
    async fn test_uri_from_path_with_base_path() {
        use std::path::MAIN_SEPARATOR;

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        // Test with path that already contains base_path (using platform-native separator)
        let full_path = format!(
            "{}{}test{}file.txt",
            provider.base_path, MAIN_SEPARATOR, MAIN_SEPARATOR
        );
        let uri = provider.uri_from_path(&full_path);

        assert!(
            uri.starts_with("file://"),
            "URI should start with file:// but got: {}",
            uri
        );

        // URIs should never contain backslashes (even on Windows)
        assert!(
            !uri.contains('\\'),
            "URI should use forward slashes only, but got: {}",
            uri
        );

        #[cfg(unix)]
        assert!(
            uri.starts_with("file:///"),
            "Unix absolute path URI should start with file:/// but got: {}",
            uri
        );
    }

    #[tokio::test]
    async fn test_uri_from_path_with_leading_slash() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let uri = provider.uri_from_path("/test/file.txt");
        assert!(uri.starts_with("file://"));
        assert!(uri.contains("test"));
        assert!(uri.contains("file.txt"));

        // URIs should never contain backslashes
        assert!(
            !uri.contains('\\'),
            "URI should use forward slashes only, but got: {}",
            uri
        );

        #[cfg(unix)]
        assert!(
            uri.starts_with("file:///"),
            "Unix absolute path URI should start with file:/// but got: {}",
            uri
        );
    }

    /// Test that fix_uri produces valid URIs for various input formats.
    /// This test specifically catches the bug where absolute paths without
    /// file: prefix would produce file://path instead of file:///path.
    #[tokio::test]
    async fn test_uri_from_path_format_validation() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        // Test various input formats
        let test_cases = vec![
            ("relative/path.txt", "relative path"),
            ("/absolute/path.txt", "absolute path with leading slash"),
        ];

        for (input, description) in test_cases {
            let uri = provider.uri_from_path(input);

            // All URIs should start with file://
            assert!(
                uri.starts_with("file://"),
                "{}: URI should start with file:// but got: {}",
                description,
                uri
            );

            // The path component should be present
            let path_part = input.trim_start_matches('/');
            assert!(
                uri.contains(path_part),
                "{}: URI should contain path '{}' but got: {}",
                description,
                path_part,
                uri
            );

            // URIs should never contain backslashes (even on Windows)
            assert!(
                !uri.contains('\\'),
                "{}: URI should use forward slashes only, but got: {}",
                description,
                uri
            );

            #[cfg(unix)]
            {
                // On Unix, the base_path is absolute (starts with /), so all URIs
                // should have file:// followed by an absolute path (starting with /)
                assert!(
                    uri.starts_with("file:///"),
                    "{}: Unix URI should start with file:/// but got: {}",
                    description,
                    uri
                );

                // Verify no double slashes in the path (except after file:)
                let after_scheme = &uri[7..]; // Skip "file://"
                assert!(
                    !after_scheme.contains("//"),
                    "{}: URI should not have double slashes in path: {}",
                    description,
                    uri
                );
            }
        }
    }

    /// Test that fix_uri correctly handles Windows extended-length path prefix.
    /// On Windows, canonicalize() adds \\?\ prefix to paths, which must be
    /// stripped when creating file:// URIs.
    #[test]
    fn test_fix_uri_windows_extended_path_prefix() {
        // Simulate what fix_uri does internally for testing the Windows prefix handling
        fn fix_uri_for_test(path: &str) -> String {
            let path = path.replace('\\', "/");
            let path = path.strip_prefix("//?/").unwrap_or(&path).to_string();

            let path_without_scheme = if let Some(without_scheme) = path.strip_prefix("file:") {
                without_scheme.trim_start_matches('/').to_string()
            } else if path.starts_with('/') {
                path.trim_start_matches('/').to_string()
            } else {
                path.to_string()
            };

            format!("file:///{}", path_without_scheme)
        }

        // Test Windows extended-length path prefix (\\?\C:\...)
        let windows_path = "\\\\?\\C:\\Users\\test\\data";
        let uri = fix_uri_for_test(windows_path);
        assert_eq!(uri, "file:///C:/Users/test/data");
        assert!(!uri.contains("?"), "URI should not contain ? from prefix");

        // Test already forward-slash version (//?/C:/...)
        let windows_path_fwd = "//?/C:/Users/test/data";
        let uri = fix_uri_for_test(windows_path_fwd);
        assert_eq!(uri, "file:///C:/Users/test/data");

        // Test normal Windows path without prefix
        let normal_windows = "C:\\Users\\test\\data";
        let uri = fix_uri_for_test(normal_windows);
        assert_eq!(uri, "file:///C:/Users/test/data");

        // Test Unix path (should not be affected)
        let unix_path = "/home/user/data";
        let uri = fix_uri_for_test(unix_path);
        assert_eq!(uri, "file:///home/user/data");
    }

    #[tokio::test]
    async fn test_validate_connection_local_valid() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        // Create a subdirectory for validation
        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir).unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let result = provider.validate_connection("subdir").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_connection_local_invalid() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let result = provider.validate_connection("nonexistent").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_debug_implementation() {
        let config = StorageConfig::local()
            .with_option("path", "/tmp")
            .with_option("timeout", "60");

        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("StorageConfig"));
    }

    #[tokio::test]
    async fn test_read_file() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        // Create a test file
        let file_path = temp_dir.path().join("test.txt");
        let test_content = b"Hello, World!";
        fs::write(&file_path, test_content).unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let content = provider.read_file("test.txt").await.unwrap();
        assert_eq!(content, test_content);
    }

    #[tokio::test]
    async fn test_read_file_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let result = provider.read_file("nonexistent.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_exists_file() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        // Create a test file
        let file_path = temp_dir.path().join("exists.txt");
        fs::write(&file_path, "content").unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let exists = provider.exists("exists.txt").await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_exists_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let exists = provider.exists("nonexistent.txt").await.unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_get_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        // Create a test file
        let file_path = temp_dir.path().join("metadata.txt");
        let test_content = b"Test content for metadata";
        fs::write(&file_path, test_content).unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let metadata = provider.get_metadata("metadata.txt").await.unwrap();
        assert_eq!(metadata.path, "metadata.txt");
        assert_eq!(metadata.size, test_content.len() as u64);
        assert!(metadata.last_modified.is_some());
    }

    #[tokio::test]
    async fn test_list_files_non_recursive() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        // Create test files
        fs::write(temp_dir.path().join("file1.txt"), "content1").unwrap();
        fs::write(temp_dir.path().join("file2.txt"), "content2").unwrap();

        // Create subdirectory with file (should not be listed in non-recursive mode)
        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir).unwrap();
        fs::write(sub_dir.join("file3.txt"), "content3").unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let files = provider.list_files("", false).await.unwrap();

        // Should only list files in root directory
        assert_eq!(files.len(), 2);
        let file_names: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
        assert!(file_names.iter().any(|name| name.contains("file1.txt")));
        assert!(file_names.iter().any(|name| name.contains("file2.txt")));
    }

    #[tokio::test]
    async fn test_list_files_recursive() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        // Create test files
        fs::write(temp_dir.path().join("file1.txt"), "content1").unwrap();

        // Create subdirectory with file
        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir).unwrap();
        fs::write(sub_dir.join("file2.txt"), "content2").unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let files = provider.list_files("", true).await.unwrap();

        // Should list all files recursively
        assert!(files.len() >= 2);
        let file_names: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
        assert!(file_names.iter().any(|name| name.contains("file1.txt")));
        assert!(file_names.iter().any(|name| name.contains("file2.txt")));
    }

    #[tokio::test]
    async fn test_discover_partitions() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        // Create partition directories
        fs::create_dir(temp_dir.path().join("partition1")).unwrap();
        fs::create_dir(temp_dir.path().join("partition2")).unwrap();
        fs::create_dir(temp_dir.path().join("_metadata")).unwrap(); // Should be excluded

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let partitions = provider
            .discover_partitions("", vec!["_metadata"])
            .await
            .unwrap();

        // Should find partitions but exclude _metadata
        assert!(partitions.len() >= 2);
        assert!(partitions.iter().any(|p| p.contains("partition1")));
        assert!(partitions.iter().any(|p| p.contains("partition2")));
        assert!(!partitions.iter().any(|p| p.contains("_metadata")));
    }

    #[tokio::test]
    async fn test_provider_debug_format() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = ObjectStoreProvider::new(config).await.unwrap();

        let debug_str = format!("{:?}", provider);
        assert!(debug_str.contains("StorageProvider"));
        assert!(debug_str.contains("local"));
    }

    #[tokio::test]
    async fn test_hdfs_provider_missing_url() {
        // HDFS requires a 'url' option - test that missing URL returns appropriate error
        let config = StorageConfig::hdfs();
        let provider = ObjectStoreProvider::new(config).await;

        assert!(provider.is_err());
        match provider {
            Err(StorageError::ConfigError(msg)) => {
                assert!(
                    msg.contains("HDFS storage requires 'url' option"),
                    "Expected error about missing URL, got: {}",
                    msg
                );
            }
            _ => panic!("Expected ConfigError for missing HDFS URL"),
        }
    }

    #[tokio::test]
    async fn test_hdfs_provider_invalid_url() {
        // Test with an invalid HDFS URL format
        let config = StorageConfig::hdfs().with_option("url", "not-a-valid-hdfs-url");
        let provider = ObjectStoreProvider::new(config).await;

        assert!(provider.is_err());
        match provider {
            Err(StorageError::ConfigError(msg)) => {
                assert!(
                    msg.contains("Failed to create HDFS store"),
                    "Expected error about HDFS store creation failure, got: {}",
                    msg
                );
            }
            _ => panic!("Expected ConfigError for invalid HDFS URL"),
        }
    }

    #[tokio::test]
    async fn test_hdfs_provider_unreachable_namenode() {
        // Test with a valid URL format but unreachable namenode
        // This tests the error handling when HDFS connection fails
        let config = StorageConfig::hdfs().with_option("url", "hdfs://nonexistent-namenode:8020");
        let provider = ObjectStoreProvider::new(config).await;

        // The provider creation may succeed (lazy connection) or fail immediately
        // depending on the hdfs-native-object-store implementation
        // Either outcome is acceptable - we're testing that it doesn't panic
        match provider {
            Ok(_) => {
                // Lazy connection - provider created but will fail on first operation
                // This is acceptable behavior
            }
            Err(StorageError::ConfigError(msg)) => {
                // Eager connection failure - also acceptable
                assert!(
                    msg.contains("Failed to create HDFS store"),
                    "Expected HDFS store creation error, got: {}",
                    msg
                );
            }
            Err(e) => panic!("Unexpected error type: {:?}", e),
        }
    }

    #[test]
    fn test_build_hdfs_store_missing_url() {
        // Direct test of build_hdfs_store with missing URL
        let config = StorageConfig::hdfs();
        let result = ObjectStoreProvider::build_hdfs_store(&config);

        assert!(result.is_err());
        match result {
            Err(StorageError::ConfigError(msg)) => {
                assert!(msg.contains("HDFS storage requires 'url' option"));
            }
            _ => panic!("Expected ConfigError for missing URL"),
        }
    }

    #[test]
    fn test_build_hdfs_store_invalid_url() {
        // Direct test of build_hdfs_store with invalid URL
        let config = StorageConfig::hdfs().with_option("url", "invalid://not-hdfs");
        let result = ObjectStoreProvider::build_hdfs_store(&config);

        assert!(result.is_err());
        match result {
            Err(StorageError::ConfigError(msg)) => {
                assert!(
                    msg.contains("Failed to create HDFS store"),
                    "Expected HDFS creation error, got: {}",
                    msg
                );
            }
            _ => panic!("Expected ConfigError for invalid URL"),
        }
    }

    #[test]
    fn test_build_hdfs_store_empty_url() {
        // Test with empty URL string
        let config = StorageConfig::hdfs().with_option("url", "");
        let result = ObjectStoreProvider::build_hdfs_store(&config);

        // Empty URL should fail during HDFS store creation
        assert!(result.is_err());
        match result {
            Err(StorageError::ConfigError(msg)) => {
                assert!(
                    msg.contains("Failed to create HDFS store"),
                    "Expected HDFS creation error for empty URL, got: {}",
                    msg
                );
            }
            _ => panic!("Expected ConfigError for empty URL"),
        }
    }
}
