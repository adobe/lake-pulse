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

use thiserror::Error;

/// Errors that can occur during storage operations
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_config_error() {
        let error = StorageError::ConfigError("Invalid configuration".to_string());
        assert_eq!(
            error.to_string(),
            "Configuration error: Invalid configuration"
        );
    }

    #[test]
    fn test_connection_error() {
        let error = StorageError::ConnectionError("Failed to connect".to_string());
        assert_eq!(error.to_string(), "Connection error: Failed to connect");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
        let storage_error: StorageError = io_error.into();

        match storage_error {
            StorageError::IoError(_) => {
                assert!(storage_error.to_string().contains("IO error"));
            }
            _ => panic!("Expected IoError variant"),
        }
    }

    #[test]
    fn test_url_parse_error_conversion() {
        let url_error = url::ParseError::EmptyHost;
        let storage_error: StorageError = url_error.into();

        match storage_error {
            StorageError::UrlParseError(_) => {
                assert!(storage_error.to_string().contains("URL parse error"));
            }
            _ => panic!("Expected UrlParseError variant"),
        }
    }

    #[test]
    fn test_error_debug() {
        let error = StorageError::ConfigError("test".to_string());
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("ConfigError"));
    }

    #[test]
    fn test_storage_result_ok() {
        let result: StorageResult<i32> = Ok(42);
        assert!(result.is_ok());
        if let Ok(value) = result {
            assert_eq!(value, 42);
        }
    }

    #[test]
    fn test_storage_result_err() {
        let result: StorageResult<i32> = Err(StorageError::ConfigError("error".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_error_source() {
        let io_error = io::Error::new(io::ErrorKind::PermissionDenied, "Permission denied");
        let storage_error: StorageError = io_error.into();

        // Test that the error can be used with error trait methods
        let error_msg = storage_error.to_string();
        assert!(error_msg.contains("IO error"));
    }

    #[test]
    fn test_multiple_error_types() {
        let errors = vec![
            StorageError::ConfigError("config".to_string()),
            StorageError::ConnectionError("connection".to_string()),
            StorageError::IoError(io::Error::other("io")),
        ];

        assert_eq!(errors.len(), 3);
        for error in errors {
            // Verify all errors implement Display
            let _ = error.to_string();
        }
    }
}
