// Copyright 2025 Adobe. All rights reserved.
// This file is licensed to you under the Apache License,
// Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
// or the MIT license (http://opensource.org/licenses/MIT),
// at your option.

// Unless required by applicable law or agreed to in writing,
// this software is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR REPRESENTATIONS OF ANY KIND, either express or
// implied. See the LICENSE-MIT and LICENSE-APACHE files for the
// specific language governing permissions and limitations under
// each license.

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
#[cfg(feature = "hudi")]
pub mod hudi;
pub mod iceberg;
#[cfg(feature = "lance")]
pub mod lance;
