//! Table analysis functionality
//!
//! This module provides the core analysis capabilities for data lake tables.
//! It includes the main [`Analyzer`] struct and format-specific analyzers for
//! Delta Lake, Apache Iceberg, Apache Hudi, and Lance.
//!
//! ## Main Components
//!
//! - [`Analyzer`] - Main entry point for analyzing tables
//! - [`metrics`] - Health metrics and reporting structures
//! - [`table_analyzer`] - Trait for table-specific analyzers
//!
//! ## Format-Specific Analyzers
//!
//! - [`delta`] - Delta Lake table analysis
//! - [`iceberg`] - Apache Iceberg table analysis
//! - [`hudi`] - Apache Hudi table analysis
//! - [`lance`] - Lance table analysis

pub mod analyze;
pub mod delta;
pub mod hudi;
pub mod iceberg;
pub mod lance;
pub mod metrics;
pub mod table_analyzer;

pub use analyze::Analyzer;
