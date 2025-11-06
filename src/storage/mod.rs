//! Cloud storage abstraction layer
//!
//! This module provides a unified interface for interacting with different
//! cloud storage providers (AWS S3, Azure Data Lake, GCS, Local filesystem).
//!
//! The implementation uses a generic approach leveraging the `object_store` crate's
//! built-in configuration system, which reduces code duplication and makes it easy
//! to add support for new storage providers.

pub mod config;
pub mod error;
pub mod factory;
pub mod generic;
pub mod provider;

// Public exports
pub use config::StorageConfig;
pub use factory::StorageProviderFactory;
pub use provider::{FileMetadata, StorageProvider};
