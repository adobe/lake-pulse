// Copyright 2022 Adobe. All rights reserved.
// This file is licensed to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
// OF ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

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
