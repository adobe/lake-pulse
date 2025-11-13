use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Storage provider type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    /// Local filesystem storage
    Local,
    /// AWS S3 storage
    Aws,
    /// Azure Data Lake Storage
    Azure,
    /// Google Cloud Storage
    Gcs,
}

/// Generic configuration for storage providers using object_store
///
/// This configuration uses a HashMap to store provider-specific options,
/// which are passed directly to the object_store builders. This approach
/// leverages object_store's built-in configuration system and reduces
/// the need for custom configuration structs.
///
/// # Examples
///
/// ## Local filesystem
/// ```
/// use lake_health::storage::StorageConfig;
///
/// let config = StorageConfig::local()
///     .with_option("path", "/tmp/data");
/// ```
///
/// ## AWS S3
/// ```
/// use lake_health::storage::StorageConfig;
///
/// let config = StorageConfig::new("s3")
///     .with_option("bucket", "my-bucket")
///     .with_option("region", "us-east-1")
///     .with_option("access_key_id", "AKIAIOSFODNN7EXAMPLE")
///     .with_option("secret_access_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
/// ```
///
/// ## Azure
/// ```
/// use lake_health::storage::StorageConfig;
///
/// let config = StorageConfig::new("azure")
///     .with_option("container", "mycontainer")
///     .with_option("account_name", "myaccount")
///     .with_option("access_key", "ACCOUNT_KEY");
/// ```
///
/// ## GCS
/// ```
/// use lake_health::storage::StorageConfig;
///
/// let config = StorageConfig::new("gcs")
///     .with_option("bucket", "my-bucket")
///     .with_option("service_account_key_path", "/path/to/key.json");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage provider type
    #[serde(rename = "type")]
    pub storage_type: StorageType,

    /// Provider-specific configuration options
    ///
    /// These options are passed directly to the object_store builders.
    /// Common options include:
    ///
    /// AWS S3:
    /// - bucket: Bucket name
    /// - region: AWS region (e.g., "us-east-1")
    /// - access_key_id: AWS access key ID
    /// - secret_access_key: AWS secret access key
    /// - session_token: AWS session token (for temporary credentials)
    /// - endpoint: Custom endpoint URL (for S3-compatible services)
    /// - allow_http: "true" to allow HTTP connections
    ///
    /// Azure:
    /// - container: Container name
    /// - account_name: Storage account name
    /// - access_key: Account key
    /// - sas_token: SAS token
    /// - tenant_id: Azure AD tenant ID
    /// - client_id: Azure AD client ID
    /// - client_secret: Azure AD client secret
    ///
    /// GCS:
    /// - bucket: Bucket name
    /// - service_account_key_path: Path to service account JSON key file
    /// - service_account_key: Service account key as JSON string
    ///
    /// Local:
    /// - path: Base path
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl StorageConfig {
    /// Create a new storage configuration.
    ///
    /// # Arguments
    ///
    /// * `storage_type` - The type of storage provider ("local", "aws", "azure", "gcs")
    ///
    /// # Returns
    ///
    /// A new `StorageConfig` instance with default options for the specified storage type.
    pub fn new(storage_type: impl Into<String>) -> Self {
        let storage_type_str = storage_type.into();
        let storage_type = match storage_type_str.to_lowercase().as_str() {
            "local" => StorageType::Local,
            "aws" | "s3" => StorageType::Aws,
            "azure" => StorageType::Azure,
            "gcs" | "gcp" => StorageType::Gcs,
            _ => panic!("Unknown storage type: {}", storage_type_str),
        };

        Self {
            storage_type,
            options: Self::default_options(),
        }
    }

    /// Create a local filesystem storage configuration.
    ///
    /// # Returns
    ///
    /// A new `StorageConfig` instance configured for local filesystem access with default options.
    pub fn local() -> Self {
        Self {
            storage_type: StorageType::Local,
            options: Self::default_options(),
        }
    }

    /// Create an AWS S3 storage configuration.
    ///
    /// # Returns
    ///
    /// A new `StorageConfig` instance configured for AWS S3 access with default options.
    pub fn aws() -> Self {
        Self {
            storage_type: StorageType::Aws,
            options: Self::default_options(),
        }
    }

    /// Create an Azure storage configuration.
    ///
    /// # Returns
    ///
    /// A new `StorageConfig` instance configured for Azure Blob Storage access with default options.
    pub fn azure() -> Self {
        Self {
            storage_type: StorageType::Azure,
            options: Self::default_options(),
        }
    }

    /// Create a GCS storage configuration.
    ///
    /// # Returns
    ///
    /// A new `StorageConfig` instance configured for Google Cloud Storage access.
    pub fn gcs() -> Self {
        Self {
            storage_type: StorageType::Gcs,
            options: HashMap::new(),
        }
    }

    /// Get default options for all storage types.
    ///
    /// # Returns
    ///
    /// A HashMap containing default timeout, retry, and connection pool settings.
    pub fn default_options() -> HashMap<String, String> {
        [
            ("timeout", "1200"),
            ("connect_timeout", "30"),
            ("max_retries", "20"),
            ("retry_timeout", "1200"),
            ("pool_idle_timeout", "15"),
            ("pool_max_idle_per_host", "5"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    /// Add a configuration option.
    ///
    /// # Arguments
    ///
    /// * `key` - The option key
    /// * `value` - The option value
    ///
    /// # Returns
    ///
    /// The `StorageConfig` instance with the added option (for method chaining).
    pub fn with_option(
        mut self,
        key: impl Into<String> + Clone,
        value: impl Into<String> + Clone,
    ) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Add multiple configuration options.
    ///
    /// # Arguments
    ///
    /// * `options` - HashMap of options to add
    ///
    /// # Returns
    ///
    /// The `StorageConfig` instance with the added options (for method chaining).
    pub fn with_options(mut self, options: HashMap<String, String>) -> Self {
        self.options.extend(options);
        self
    }

    /// Get a configuration option.
    ///
    /// # Arguments
    ///
    /// * `key` - The option key to retrieve
    ///
    /// # Returns
    ///
    /// `Some(&String)` if the option exists, `None` otherwise.
    pub fn get_option(&self, key: &str) -> Option<&String> {
        self.options.get(key)
    }

    /// Get the storage type as a string.
    ///
    /// # Returns
    ///
    /// A string slice representing the storage type ("local", "aws", "azure", or "gcs").
    pub fn storage_type_str(&self) -> &str {
        match self.storage_type {
            StorageType::Local => "local",
            StorageType::Aws => "aws",
            StorageType::Azure => "azure",
            StorageType::Gcs => "gcs",
        }
    }
}

impl From<StorageConfig> for String {
    fn from(config: StorageConfig) -> Self {
        config.storage_type_str().to_string()
    }
}
