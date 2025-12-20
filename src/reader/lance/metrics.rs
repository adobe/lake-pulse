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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Lance table specific metrics extracted from table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceMetrics {
    /// Current version of the table
    pub version: u64,

    /// Table metadata
    pub metadata: TableMetadata,

    /// Table configuration/properties
    pub table_properties: HashMap<String, String>,

    /// File statistics
    pub file_stats: FileStatistics,

    /// Fragment information
    pub fragment_info: FragmentMetrics,

    /// Index information
    pub index_info: IndexMetrics,
}

/// Lance table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table UUID
    pub uuid: String,

    /// Schema as JSON string
    pub schema_string: String,

    /// Number of fields in the schema
    pub field_count: usize,

    /// Table creation time (milliseconds since epoch)
    pub created_time: Option<i64>,

    /// Last modified time (milliseconds since epoch)
    pub last_modified_time: Option<i64>,

    /// Number of rows in the table
    pub num_rows: Option<u64>,

    /// Number of deleted rows
    pub num_deleted_rows: Option<u64>,
}

/// File-level statistics from Lance table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatistics {
    /// Total number of data files
    pub num_data_files: usize,

    /// Total number of deletion files
    pub num_deletion_files: usize,

    /// Total size of data files in bytes
    pub total_data_size_bytes: u64,

    /// Total size of deletion files in bytes
    pub total_deletion_size_bytes: u64,

    /// Average data file size in bytes
    pub avg_data_file_size_bytes: f64,

    /// Minimum data file size in bytes
    pub min_data_file_size_bytes: u64,

    /// Maximum data file size in bytes
    pub max_data_file_size_bytes: u64,
}

/// Fragment-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMetrics {
    /// Total number of fragments
    pub num_fragments: usize,

    /// Number of fragments with deletion files
    pub num_fragments_with_deletions: usize,

    /// Average rows per fragment
    pub avg_rows_per_fragment: f64,

    /// Minimum rows in a fragment
    pub min_rows_per_fragment: u64,

    /// Maximum rows in a fragment
    pub max_rows_per_fragment: u64,

    /// Fragment physical rows (including deleted)
    pub total_physical_rows: u64,
}

/// Index-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetrics {
    /// Number of indices on the table
    pub num_indices: usize,

    /// List of indexed columns
    pub indexed_columns: Vec<String>,

    /// Index types (e.g., "IVF_PQ", "BTREE")
    pub index_types: Vec<String>,

    /// Total size of indices in bytes
    pub total_index_size_bytes: u64,
}

impl Default for LanceMetrics {
    fn default() -> Self {
        Self {
            version: 0,
            metadata: TableMetadata {
                uuid: String::new(),
                schema_string: String::new(),
                field_count: 0,
                created_time: None,
                last_modified_time: None,
                num_rows: None,
                num_deleted_rows: None,
            },
            table_properties: HashMap::new(),
            file_stats: FileStatistics {
                num_data_files: 0,
                num_deletion_files: 0,
                total_data_size_bytes: 0,
                total_deletion_size_bytes: 0,
                avg_data_file_size_bytes: 0.0,
                min_data_file_size_bytes: 0,
                max_data_file_size_bytes: 0,
            },
            fragment_info: FragmentMetrics {
                num_fragments: 0,
                num_fragments_with_deletions: 0,
                avg_rows_per_fragment: 0.0,
                min_rows_per_fragment: 0,
                max_rows_per_fragment: 0,
                total_physical_rows: 0,
            },
            index_info: IndexMetrics {
                num_indices: 0,
                indexed_columns: Vec::new(),
                index_types: Vec::new(),
                total_index_size_bytes: 0,
            },
        }
    }
}
