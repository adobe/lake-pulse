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

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};

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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

impl Display for LanceMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        writeln!(f)?;
        writeln!(f, " Lance Specific Metrics")?;
        writeln!(f, "{}", "━".repeat(80))?;

        // Basic info
        writeln!(f, " {:<40} {:>32}", "Version", self.version)?;
        if !self.metadata.uuid.is_empty() {
            writeln!(f, " {:<31}  {:>40}", "Table UUID", &self.metadata.uuid)?;
        }
        writeln!(
            f,
            " {:<40} {:>32}",
            "Field Count", self.metadata.field_count
        )?;
        if let Some(rows) = self.metadata.num_rows {
            writeln!(f, " {:<40} {:>32}", "Total Rows", rows)?;
        }
        if let Some(deleted) = self.metadata.num_deleted_rows {
            if deleted > 0 {
                writeln!(f, " {:<40} {:>32}", "Deleted Rows", deleted)?;
            }
        }
        if let Some(created) = self.metadata.created_time {
            if let Some(dt) = DateTime::from_timestamp_millis(created) {
                writeln!(f, " {:<40} {:>32}", "Created Time", dt.to_rfc3339())?;
            }
        }
        if let Some(modified) = self.metadata.last_modified_time {
            if let Some(dt) = DateTime::from_timestamp_millis(modified) {
                writeln!(f, " {:<40} {:>32}", "Last Modified", dt.to_rfc3339())?;
            }
        }

        // Table Properties
        if !self.table_properties.is_empty() {
            self.table_properties
                .iter()
                .enumerate()
                .try_for_each(|(i, (k, v))| {
                    if i == 0 {
                        writeln!(
                            f,
                            " {:<20} {:>52}",
                            "Table Properties",
                            format!("{}: {}", k, v)
                        )
                    } else {
                        writeln!(f, " {:<20} {:>52}", "", format!("{}: {}", k, v))
                    }
                })?;
        }

        // File Statistics
        let file_stats = &self.file_stats;
        let total_size_mb = file_stats.total_data_size_bytes as f64 / (1024.0 * 1024.0);
        let avg_size_mb = file_stats.avg_data_file_size_bytes / (1024.0 * 1024.0);
        let min_size_kb = file_stats.min_data_file_size_bytes as f64 / 1024.0;
        let max_size_mb = file_stats.max_data_file_size_bytes as f64 / (1024.0 * 1024.0);

        writeln!(f, " File Statistics")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Data Files", file_stats.num_data_files
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Data Size",
            format!("{:.2} MB", total_size_mb)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Avg Data File Size",
            format!("{:.2} MB", avg_size_mb)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Min Data File Size",
            format!("{:.2} KB", min_size_kb)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Max Data File Size",
            format!("{:.2} MB", max_size_mb)
        )?;
        if file_stats.num_deletion_files > 0 {
            let del_size_mb = file_stats.total_deletion_size_bytes as f64 / (1024.0 * 1024.0);
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Deletion Files", file_stats.num_deletion_files
            )?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Total Deletion Size",
                format!("{:.2} MB", del_size_mb)
            )?;
        }

        // Fragment Info
        let fragment_info = &self.fragment_info;
        writeln!(f, " Fragment Info")?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Fragments", fragment_info.num_fragments
        )?;
        if fragment_info.num_fragments_with_deletions > 0 {
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Fragments with Deletions", fragment_info.num_fragments_with_deletions
            )?;
        }
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Avg Rows per Fragment",
            format!("{:.0}", fragment_info.avg_rows_per_fragment)
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Min Rows per Fragment", fragment_info.min_rows_per_fragment
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Max Rows per Fragment", fragment_info.max_rows_per_fragment
        )?;
        writeln!(
            f,
            "   {:<38} {:>32}",
            "Total Physical Rows", fragment_info.total_physical_rows
        )?;

        // Index Info
        let index_info = &self.index_info;
        if index_info.num_indices > 0 {
            let index_size_mb = index_info.total_index_size_bytes as f64 / (1024.0 * 1024.0);
            writeln!(f, " Index Info")?;
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Number of Indices", index_info.num_indices
            )?;
            if !index_info.indexed_columns.is_empty() {
                writeln!(
                    f,
                    "   {:<38} {:>32}",
                    "Indexed Columns",
                    index_info.indexed_columns.join(", ")
                )?;
            }
            if !index_info.index_types.is_empty() {
                writeln!(
                    f,
                    "   {:<38} {:>32}",
                    "Index Types",
                    index_info.index_types.join(", ")
                )?;
            }
            writeln!(
                f,
                "   {:<38} {:>32}",
                "Total Index Size",
                format!("{:.2} MB", index_size_mb)
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lance_metrics_default() {
        let metrics = LanceMetrics::default();

        assert_eq!(metrics.version, 0);
        assert_eq!(metrics.metadata.uuid, "");
        assert!(metrics.metadata.num_rows.is_none());
        assert!(metrics.metadata.num_deleted_rows.is_none());
        assert!(metrics.metadata.created_time.is_none());
        assert!(metrics.metadata.last_modified_time.is_none());
        assert!(metrics.table_properties.is_empty());
        assert_eq!(metrics.file_stats.num_data_files, 0);
        assert_eq!(metrics.fragment_info.num_fragments, 0);
        assert_eq!(metrics.index_info.num_indices, 0);
    }

    #[test]
    fn test_lance_metrics_display() {
        let mut table_properties = HashMap::new();
        table_properties.insert("lance.version".to_string(), "0.10.0".to_string());

        let metrics = LanceMetrics {
            version: 5,
            metadata: TableMetadata {
                uuid: "test-lance-uuid".to_string(),
                schema_string: String::new(),
                field_count: 0,
                num_rows: Some(1000000),
                num_deleted_rows: Some(100),
                created_time: Some(1700000000000),
                last_modified_time: Some(1700001000000),
            },
            table_properties,
            file_stats: FileStatistics {
                num_data_files: 100,
                num_deletion_files: 5,
                total_data_size_bytes: 1024 * 1024 * 500,
                total_deletion_size_bytes: 1024 * 100,
                avg_data_file_size_bytes: 1024.0 * 1024.0 * 5.0,
                min_data_file_size_bytes: 1024,
                max_data_file_size_bytes: 1024 * 1024 * 10,
            },
            fragment_info: FragmentMetrics {
                num_fragments: 50,
                num_fragments_with_deletions: 5,
                avg_rows_per_fragment: 20000.0,
                min_rows_per_fragment: 1000,
                max_rows_per_fragment: 50000,
                total_physical_rows: 1000100,
            },
            index_info: IndexMetrics {
                num_indices: 2,
                indexed_columns: vec!["id".to_string(), "embedding".to_string()],
                index_types: vec!["IVF_PQ".to_string()],
                total_index_size_bytes: 1024 * 1024 * 50,
            },
        };

        let display = format!("{}", metrics);

        assert!(display.contains("Lance Specific Metrics"));
        assert!(display.contains("Version"));
        assert!(display.contains("5"));
        assert!(display.contains("test-lance-uuid"));
        assert!(display.contains("Total Rows"));
        assert!(display.contains("1000000"));
        assert!(display.contains("Deleted Rows"));
        assert!(display.contains("File Statistics"));
        assert!(display.contains("Fragment Info"));
        assert!(display.contains("Index Info"));
        assert!(display.contains("Table Properties"));
    }

    #[test]
    fn test_lance_metrics_display_minimal() {
        let metrics = LanceMetrics::default();
        let display = format!("{}", metrics);

        assert!(display.contains("Lance Specific Metrics"));
        assert!(display.contains("Version"));
        assert!(display.contains("File Statistics"));
        assert!(display.contains("Fragment Info"));
    }

    #[test]
    fn test_lance_metrics_display_no_indices() {
        let metrics = LanceMetrics::default();
        let display = format!("{}", metrics);

        // Index Info section should not appear when there are no indices
        assert!(!display.contains("Index Info"));
    }

    #[test]
    fn test_lance_metrics_display_with_deletions() {
        let mut metrics = LanceMetrics::default();
        metrics.metadata.num_deleted_rows = Some(500);
        metrics.fragment_info.num_fragments_with_deletions = 10;

        let display = format!("{}", metrics);

        assert!(display.contains("Deleted Rows"));
        assert!(display.contains("500"));
        assert!(display.contains("Fragments with Deletions"));
        assert!(display.contains("10"));
    }

    #[test]
    fn test_lance_metrics_default_has_empty_table_properties() {
        let metrics = LanceMetrics::default();

        // Verify table_properties is empty by default
        assert!(metrics.table_properties.is_empty());
    }
}
