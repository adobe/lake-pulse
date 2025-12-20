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

pub mod analyzer;
pub mod delta;
#[cfg(feature = "hudi")]
pub mod hudi;
pub mod iceberg;
#[cfg(feature = "lance")]
pub mod lance;
pub mod metrics;
pub mod table_analyzer;

pub use analyzer::Analyzer;
