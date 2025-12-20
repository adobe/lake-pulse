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
pub mod object_store;
pub mod provider;

// Public exports
pub use config::StorageConfig;
pub use factory::StorageProviderFactory;
pub use provider::{FileMetadata, StorageProvider};
