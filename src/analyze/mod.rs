// Copyright 2022 Adobe. All rights reserved.
// This file is licensed to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
// OF ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

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
