// Copyright 2022 Adobe. All rights reserved.
// This file is licensed to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
// OF ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

use std::sync::Arc;

use super::config::StorageConfig;
use super::error::StorageResult;
use super::object_store::ObjectStoreProvider;
use super::provider::StorageProvider;

/// Factory for creating storage providers
pub struct StorageProviderFactory;

impl StorageProviderFactory {
    /// Create a storage provider from a configuration.
    ///
    /// This factory creates a generic storage provider that works with any
    /// object_store backend (AWS S3, Azure, GCS, or local filesystem).
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration specifying the provider type and options
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(Arc<dyn StorageProvider>)` - A thread-safe reference to the initialized storage provider
    /// * `Err(StorageError)` - If the provider cannot be created
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The storage configuration is invalid
    /// * Required configuration options are missing
    /// * The storage provider cannot be initialized
    /// * Credentials are invalid or expired
    pub async fn from_config(config: StorageConfig) -> StorageResult<Arc<dyn StorageProvider>> {
        let provider = ObjectStoreProvider::new(config).await?;
        Ok(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_factory_create_local_provider() {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let result = StorageProviderFactory::from_config(config).await;

        assert!(result.is_ok());
        let provider = result.unwrap();
        assert!(provider.base_path().contains(temp_path));
    }

    #[tokio::test]
    async fn test_factory_local_provider_invalid_path() {
        let config =
            StorageConfig::local().with_option("path", "/nonexistent/path/that/does/not/exist");
        let result = StorageProviderFactory::from_config(config).await;

        assert!(result.is_err());
        match result {
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("Failed to resolve path")
                        || error_msg.contains("Configuration error")
                );
            }
            Ok(_) => panic!("Expected error for invalid path"),
        }
    }

    #[tokio::test]
    async fn test_factory_local_provider_missing_path() {
        let config = StorageConfig::local();
        let result = StorageProviderFactory::from_config(config).await;

        assert!(result.is_err());
        match result {
            Err(e) => {
                assert!(e.to_string().contains("path"));
            }
            Ok(_) => panic!("Expected error for missing path option"),
        }
    }

    #[tokio::test]
    async fn test_factory_returns_arc() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = StorageProviderFactory::from_config(config).await.unwrap();

        // Test that we can clone the Arc
        let provider_clone = Arc::clone(&provider);
        assert_eq!(provider.base_path(), provider_clone.base_path());
    }

    #[tokio::test]
    async fn test_factory_provider_implements_trait() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        let config = StorageConfig::local().with_option("path", temp_path);
        let provider = StorageProviderFactory::from_config(config).await.unwrap();

        // Test that the provider implements StorageProvider trait methods
        let base_path = provider.base_path();
        assert!(!base_path.is_empty());

        let options = provider.options();
        assert!(!options.is_empty());
    }

    #[tokio::test]
    async fn test_factory_with_different_storage_types() {
        // Test that factory can be called with different storage types
        // (even if they fail due to missing credentials, the factory should handle them)

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();

        // Local should work
        let local_config = StorageConfig::local().with_option("path", temp_path);
        let local_result = StorageProviderFactory::from_config(local_config).await;
        assert!(local_result.is_ok());

        // AWS without credentials should fail gracefully
        let aws_config = StorageConfig::aws();
        let aws_result = StorageProviderFactory::from_config(aws_config).await;
        assert!(aws_result.is_err());

        // Azure without credentials should fail gracefully
        let azure_config = StorageConfig::azure();
        let azure_result = StorageProviderFactory::from_config(azure_config).await;
        assert!(azure_result.is_err());

        // GCS without credentials should fail gracefully
        let gcs_config = StorageConfig::gcs();
        let gcs_result = StorageProviderFactory::from_config(gcs_config).await;
        assert!(gcs_result.is_err());
    }
}
