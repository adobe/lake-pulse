// Copyright 2022 Adobe. All rights reserved.
// This file is licensed to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
// OF ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

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
/// use lake_pulse::storage::StorageConfig;
///
/// let config = StorageConfig::local()
///     .with_option("path", "/tmp/data");
/// ```
///
/// ## AWS S3
/// ```
/// use lake_pulse::storage::StorageConfig;
///
/// let config = StorageConfig::new("s3")
///     .with_option("bucket", "my-bucket")
///     .with_option("region", "us-east-1")
///     .with_option("access_key_id", "ACCESS_KEY")
///     .with_option("secret_access_key", "SECRET_ACCESS_KEY");
/// ```
///
/// ## Azure
/// ```
/// use lake_pulse::storage::StorageConfig;
///
/// let config = StorageConfig::new("azure")
///     .with_option("container", "mycontainer")
///     .with_option("account_name", "myaccount")
///     .with_option("access_key", "ACCOUNT_KEY");
/// ```
///
/// ## GCS
/// ```
/// use lake_pulse::storage::StorageConfig;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_type_serialization() {
        // Test that StorageType serializes correctly
        let local = StorageType::Local;
        let aws = StorageType::Aws;
        let azure = StorageType::Azure;
        let gcs = StorageType::Gcs;

        assert_eq!(serde_json::to_string(&local).unwrap(), "\"local\"");
        assert_eq!(serde_json::to_string(&aws).unwrap(), "\"aws\"");
        assert_eq!(serde_json::to_string(&azure).unwrap(), "\"azure\"");
        assert_eq!(serde_json::to_string(&gcs).unwrap(), "\"gcs\"");
    }

    #[test]
    fn test_storage_type_deserialization() {
        // Test that StorageType deserializes correctly
        let local: StorageType = serde_json::from_str("\"local\"").unwrap();
        let aws: StorageType = serde_json::from_str("\"aws\"").unwrap();
        let azure: StorageType = serde_json::from_str("\"azure\"").unwrap();
        let gcs: StorageType = serde_json::from_str("\"gcs\"").unwrap();

        assert_eq!(local, StorageType::Local);
        assert_eq!(aws, StorageType::Aws);
        assert_eq!(azure, StorageType::Azure);
        assert_eq!(gcs, StorageType::Gcs);
    }

    #[test]
    fn test_storage_config_new_local() {
        let config = StorageConfig::new("local");
        assert_eq!(config.storage_type, StorageType::Local);
        assert!(!config.options.is_empty());
        assert_eq!(config.storage_type_str(), "local");
    }

    #[test]
    fn test_storage_config_new_aws() {
        let config1 = StorageConfig::new("aws");
        let config2 = StorageConfig::new("s3");
        let config3 = StorageConfig::new("AWS");

        assert_eq!(config1.storage_type, StorageType::Aws);
        assert_eq!(config2.storage_type, StorageType::Aws);
        assert_eq!(config3.storage_type, StorageType::Aws);
        assert_eq!(config1.storage_type_str(), "aws");
    }

    #[test]
    fn test_storage_config_new_azure() {
        let config = StorageConfig::new("azure");
        assert_eq!(config.storage_type, StorageType::Azure);
        assert_eq!(config.storage_type_str(), "azure");
    }

    #[test]
    fn test_storage_config_new_gcs() {
        let config1 = StorageConfig::new("gcs");
        let config2 = StorageConfig::new("gcp");

        assert_eq!(config1.storage_type, StorageType::Gcs);
        assert_eq!(config2.storage_type, StorageType::Gcs);
        assert_eq!(config1.storage_type_str(), "gcs");
    }

    #[test]
    #[should_panic(expected = "Unknown storage type")]
    fn test_storage_config_new_invalid() {
        StorageConfig::new("invalid");
    }

    #[test]
    fn test_storage_config_local() {
        let config = StorageConfig::local();
        assert_eq!(config.storage_type, StorageType::Local);
        assert!(!config.options.is_empty());
    }

    #[test]
    fn test_storage_config_aws() {
        let config = StorageConfig::aws();
        assert_eq!(config.storage_type, StorageType::Aws);
        assert!(!config.options.is_empty());
    }

    #[test]
    fn test_storage_config_azure() {
        let config = StorageConfig::azure();
        assert_eq!(config.storage_type, StorageType::Azure);
        assert!(!config.options.is_empty());
    }

    #[test]
    fn test_storage_config_gcs() {
        let config = StorageConfig::gcs();
        assert_eq!(config.storage_type, StorageType::Gcs);
        // GCS has empty options by default
        assert!(config.options.is_empty());
    }

    #[test]
    fn test_default_options() {
        let options = StorageConfig::default_options();
        assert_eq!(options.get("timeout"), Some(&"1200".to_string()));
        assert_eq!(options.get("connect_timeout"), Some(&"30".to_string()));
        assert_eq!(options.get("max_retries"), Some(&"20".to_string()));
        assert_eq!(options.get("retry_timeout"), Some(&"1200".to_string()));
        assert_eq!(options.get("pool_idle_timeout"), Some(&"15".to_string()));
        assert_eq!(
            options.get("pool_max_idle_per_host"),
            Some(&"5".to_string())
        );
    }

    #[test]
    fn test_with_option() {
        let config = StorageConfig::local()
            .with_option("path", "/tmp/data")
            .with_option("custom_key", "custom_value");

        assert_eq!(config.get_option("path"), Some(&"/tmp/data".to_string()));
        assert_eq!(
            config.get_option("custom_key"),
            Some(&"custom_value".to_string())
        );
    }

    #[test]
    fn test_with_options() {
        let mut custom_options = HashMap::new();
        custom_options.insert("bucket".to_string(), "my-bucket".to_string());
        custom_options.insert("region".to_string(), "us-east-1".to_string());

        let config = StorageConfig::aws().with_options(custom_options);

        assert_eq!(config.get_option("bucket"), Some(&"my-bucket".to_string()));
        assert_eq!(config.get_option("region"), Some(&"us-east-1".to_string()));
        // Default options should still be present
        assert_eq!(config.get_option("timeout"), Some(&"1200".to_string()));
    }

    #[test]
    fn test_get_option() {
        let config = StorageConfig::local().with_option("path", "/tmp/data");

        assert_eq!(config.get_option("path"), Some(&"/tmp/data".to_string()));
        assert_eq!(config.get_option("nonexistent"), None);
    }

    #[test]
    fn test_storage_type_str() {
        assert_eq!(StorageConfig::local().storage_type_str(), "local");
        assert_eq!(StorageConfig::aws().storage_type_str(), "aws");
        assert_eq!(StorageConfig::azure().storage_type_str(), "azure");
        assert_eq!(StorageConfig::gcs().storage_type_str(), "gcs");
    }

    #[test]
    fn test_from_storage_config_to_string() {
        let local_str: String = StorageConfig::local().into();
        let aws_str: String = StorageConfig::aws().into();
        let azure_str: String = StorageConfig::azure().into();
        let gcs_str: String = StorageConfig::gcs().into();

        assert_eq!(local_str, "local");
        assert_eq!(aws_str, "aws");
        assert_eq!(azure_str, "azure");
        assert_eq!(gcs_str, "gcs");
    }

    #[test]
    fn test_method_chaining() {
        let config = StorageConfig::aws()
            .with_option("bucket", "my-bucket")
            .with_option("region", "us-west-2")
            .with_option("access_key_id", "AxxxxxxxxxNN7EXAMPLE");

        assert_eq!(config.storage_type, StorageType::Aws);
        assert_eq!(config.get_option("bucket"), Some(&"my-bucket".to_string()));
        assert_eq!(config.get_option("region"), Some(&"us-west-2".to_string()));
        assert_eq!(
            config.get_option("access_key_id"),
            Some(&"AxxxxxxxxxNN7EXAMPLE".to_string())
        );
    }

    #[test]
    fn test_config_serialization() {
        let config = StorageConfig::aws()
            .with_option("bucket", "test-bucket")
            .with_option("region", "us-east-1");

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"type\":\"aws\""));
        assert!(json.contains("\"bucket\""));
        assert!(json.contains("\"region\""));
    }

    #[test]
    fn test_config_deserialization() {
        let json = r#"{"type":"aws","options":{"bucket":"test-bucket","region":"us-east-1"}}"#;
        let config: StorageConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.storage_type, StorageType::Aws);
        assert_eq!(
            config.get_option("bucket"),
            Some(&"test-bucket".to_string())
        );
        assert_eq!(config.get_option("region"), Some(&"us-east-1".to_string()));
    }

    #[test]
    fn test_option_override() {
        let config = StorageConfig::local()
            .with_option("timeout", "600")
            .with_option("timeout", "900"); // Override previous value

        assert_eq!(config.get_option("timeout"), Some(&"900".to_string()));
    }

    #[test]
    fn test_clone() {
        let config1 = StorageConfig::aws().with_option("bucket", "my-bucket");
        let config2 = config1.clone();

        assert_eq!(config1.storage_type, config2.storage_type);
        assert_eq!(config1.get_option("bucket"), config2.get_option("bucket"));
    }
}
