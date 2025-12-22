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

//! Common utilities shared across table format analyzers.
//!
//! This module provides shared structures, constants, and functions used by
//! Delta, Iceberg, Hudi, and Lance analyzers. Centralizing this code reduces
//! duplication and ensures consistent behavior across all table formats.
//!
//! # Modules
//!
//! - [`constants`] - File size thresholds, scoring parameters, and other constants
//! - [`schema_change`] - Schema change tracking and breaking change detection
//! - [`scoring`] - Health scoring functions for schema stability and storage cost

pub mod constants;
pub mod schema_change;
pub mod scoring;

// Re-export commonly used items for convenience
pub use constants::*;
pub use schema_change::{detect_breaking_schema_changes, is_breaking_change, SchemaChange};
pub use scoring::{
    calculate_recommended_retention, calculate_retention_efficiency,
    calculate_schema_stability_score, calculate_storage_cost_impact,
};
