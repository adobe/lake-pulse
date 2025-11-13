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
