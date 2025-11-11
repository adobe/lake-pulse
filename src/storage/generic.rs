use super::config::{StorageConfig, StorageType};
use super::error::{StorageError, StorageResult};
use super::provider::{FileMetadata, StorageProvider, string_to_path};
use crate::util::retry::retry_with_max_retries;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;
use object_store::{
    ClientOptions, ObjectStore, RetryConfig, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem,
};
use regex::Regex;
use std::collections::HashMap;
use std::future::Future;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Generic storage provider that works with any object_store backend
pub struct GenericStorageProvider {
    pub config: StorageConfig,
    pub store: Arc<dyn ObjectStore>,
    pub base_path: String,
}

impl GenericStorageProvider {
    /// Create a new generic storage provider from configuration
    pub async fn new(config: StorageConfig) -> StorageResult<Self> {
        let (store, base_path) = Self::build_store(&config).await?;

        Ok(Self {
            config,
            store: Arc::new(store),
            base_path,
        })
    }

    /// Build the appropriate object store based on configuration
    async fn build_store(config: &StorageConfig) -> StorageResult<(Box<dyn ObjectStore>, String)> {
        match config.storage_type {
            StorageType::Local => Self::build_local_store(config),
            StorageType::Aws => Self::build_aws_store(config),
            StorageType::Azure => Self::build_azure_store(config),
            StorageType::Gcs => Self::build_gcs_store(config),
        }
    }

    /// Build a local filesystem store
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

    fn build_connection_options(config: &StorageConfig) -> ClientOptions {
        let mut client_options = ClientOptions::default();
        if let Some(timeout_str) = config.options.get("timeout") {
            if timeout_str == "0" || timeout_str == "disabled" {
                client_options = client_options.with_timeout_disabled();
            } else {
                match timeout_str.parse::<u64>() {
                    Ok(sec) => {
                        client_options = client_options.with_timeout(Duration::from_secs(sec))
                    }
                    Err(_) => (),
                }
            }
        };
        if let Some(connect_timeout_str) = config.options.get("connect_timeout") {
            if connect_timeout_str == "0" || connect_timeout_str == "disabled" {
                client_options = client_options.with_connect_timeout_disabled();
            } else {
                match connect_timeout_str.parse::<u64>() {
                    Ok(ms) => {
                        client_options =
                            client_options.with_connect_timeout(Duration::from_secs(ms))
                    }
                    Err(_) => (),
                }
            }
        }
        if let Some(pool_idle_timeout_str) = config.options.get("pool_idle_timeout") {
            match pool_idle_timeout_str.parse::<u64>() {
                Ok(sec) => {
                    client_options = client_options.with_pool_idle_timeout(Duration::from_secs(sec))
                }
                Err(_) => (),
            }
        }
        if let Some(pool_max_idle_per_host_str) = config.options.get("pool_max_idle_per_host") {
            match pool_max_idle_per_host_str.parse::<usize>() {
                Ok(max_idle) => {
                    client_options = client_options.with_pool_max_idle_per_host(max_idle)
                }
                Err(_) => (),
            }
        }
        client_options
    }

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

    /// Get max retries from config
    fn get_max_retries(config: &StorageConfig) -> usize {
        config
            .options
            .get("max_retries")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10)
    }

    /// Retry wrapper for operations that may fail due to transient network errors
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

    /// Build an AWS S3 store
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
            format!("{}", endpoint_url.trim_end_matches('/'))
        } else if let Some(bucket_name) = bucket {
            // Standard S3 URL format
            format!("s3://{}", bucket_name)
        } else {
            // No bucket specified (unusual but possible)
            "s3://".to_string()
        };

        Ok((Box::new(store), base_url))
    }

    /// Build an Azure store
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
            format!("{}", endpoint.trim_end_matches('/'))
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

    /// Build a GCS store
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

    /// List multiple partitions in parallel with bounded concurrency
    ///
    /// This method lists each partition concurrently, which can provide significant
    /// speedup for tables with many partitions.
    ///
    /// # Arguments
    /// * `partitions` - Vector of partition paths to list
    ///
    /// # Returns
    /// All files from all partitions combined
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
                                    size: meta.size as u64,
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
impl StorageProvider for GenericStorageProvider {
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
                        size: meta.size as u64,
                        last_modified: Some(meta.last_modified),
                    });
                }
            } else {
                let list_result = store.list_with_delimiter(object_path.as_ref()).await?;

                for meta in list_result.objects {
                    files.push(FileMetadata {
                        path: meta.location.to_string(),
                        size: meta.size as u64,
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
                        size: meta.size as u64,
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
                                size: meta.size as u64,
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
                !vec![
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
                let re = Regex::new(r"^file:/+").unwrap();
                format!("file://{}", re.replace(path, ""))
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

impl Debug for GenericStorageProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageProvider(type=generic, cloud_provider={}, config={:?})",
            self.config.storage_type_str(),
            self.config
        )
    }
}
