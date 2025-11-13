//! Table format readers
//!
//! This module contains readers for different data lake table formats.
//! Each submodule provides format-specific functionality for reading
//! table metadata and extracting metrics.
//!
//! ## Supported Formats
//!
//! - [`delta`] - Delta Lake format reader
//! - [`iceberg`] - Apache Iceberg format reader
//! - [`hudi`] - Apache Hudi format reader
//! - [`lance`] - Lance format reader

pub mod delta;
pub mod hudi;
pub mod iceberg;
pub mod lance;
