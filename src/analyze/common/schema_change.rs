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

//! Schema change tracking and analysis utilities.
//!
//! This module provides common structures and functions for tracking schema
//! evolution across different table formats (Delta, Iceberg, Hudi, Lance).

use serde_json::Value;
use std::collections::HashSet;

/// Represents a schema change event in a table's history.
///
/// This structure captures information about schema modifications, including
/// the version, timestamp, the actual schema definition, and whether the change
/// is backward-compatible or breaking.
///
/// # Fields
///
/// * `version` - The table version when this schema change occurred
/// * `timestamp` - Unix timestamp (in milliseconds) when the change was made
/// * `schema` - The complete schema definition as a JSON value
/// * `is_breaking` - Whether this change breaks backward compatibility
#[derive(Debug, Clone)]
pub struct SchemaChange {
    /// The table version when this schema change occurred.
    /// Note: This field is tracked but not always used in calculations.
    #[allow(dead_code)]
    pub version: u64,

    /// Unix timestamp (in milliseconds) when the change was made.
    pub timestamp: u64,

    /// The complete schema definition as a JSON value.
    pub schema: Value,

    /// Whether this change breaks backward compatibility.
    /// Breaking changes include column removal, type changes, and
    /// nullability changes (non-nullable to nullable).
    pub is_breaking: bool,
}

impl SchemaChange {
    /// Create a new SchemaChange.
    ///
    /// # Arguments
    ///
    /// * `version` - The table version when this schema change occurred
    /// * `timestamp` - Unix timestamp (in milliseconds) when the change was made
    /// * `schema` - The complete schema definition as a JSON value
    /// * `is_breaking` - Whether this change breaks backward compatibility
    pub fn new(version: u64, timestamp: u64, schema: Value, is_breaking: bool) -> Self {
        Self {
            version,
            timestamp,
            schema,
            is_breaking,
        }
    }
}

/// Check if a schema change is breaking compared to previous changes.
///
/// A breaking change is one that would cause existing readers to fail,
/// such as column removal or type changes.
///
/// # Arguments
///
/// * `previous_changes` - List of previous schema changes
/// * `new_schema` - The new schema to compare against
///
/// # Returns
///
/// `true` if the schema change is breaking, `false` otherwise.
/// Returns `false` if there are no previous changes to compare against.
pub fn is_breaking_change(previous_changes: &[SchemaChange], new_schema: &Value) -> bool {
    if previous_changes.is_empty() {
        return false;
    }

    // Safe access - we've already checked that previous_changes is not empty
    let last_schema = match previous_changes.last() {
        Some(change) => &change.schema,
        None => return false,
    };

    detect_breaking_schema_changes(last_schema, new_schema)
}

/// Detect breaking schema changes between two schemas.
///
/// Performs detailed comparison of schema fields to identify breaking changes:
/// - Column removal (fields present in old schema but missing in new)
/// - Type changes (field type modified)
/// - Nullability/required changes
///
/// # Arguments
///
/// * `old_schema` - The old schema
/// * `new_schema` - The new schema
///
/// # Returns
///
/// `true` if any breaking changes are detected, `false` otherwise.
pub fn detect_breaking_schema_changes(old_schema: &Value, new_schema: &Value) -> bool {
    if let (Some(old_fields), Some(new_fields)) =
        (old_schema.get("fields"), new_schema.get("fields"))
    {
        if let (Some(old_fields_array), Some(new_fields_array)) =
            (old_fields.as_array(), new_fields.as_array())
        {
            // Check if any fields were removed
            let old_field_names: HashSet<String> = old_fields_array
                .iter()
                .filter_map(|f| {
                    f.get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            let new_field_names: HashSet<String> = new_fields_array
                .iter()
                .filter_map(|f| {
                    f.get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();

            // If any old fields are missing, it's a breaking change
            if !old_field_names.is_subset(&new_field_names) {
                return true;
            }

            // Check for type changes in existing fields
            if has_type_or_nullability_changes(old_fields_array, new_fields_array) {
                return true;
            }
        }
    }

    false
}

/// Check for type or nullability changes between field arrays.
///
/// # Arguments
///
/// * `old_fields` - Array of old field definitions
/// * `new_fields` - Array of new field definitions
///
/// # Returns
///
/// `true` if any type or nullability breaking changes are detected.
fn has_type_or_nullability_changes(old_fields: &[Value], new_fields: &[Value]) -> bool {
    for old_field in old_fields {
        if let Some(field_name) = old_field.get("name").and_then(|n| n.as_str()) {
            if let Some(new_field) = new_fields
                .iter()
                .find(|f| f.get("name").and_then(|n| n.as_str()) == Some(field_name))
            {
                let old_type = old_field.get("type").and_then(|t| t.as_str());
                let new_type = new_field.get("type").and_then(|t| t.as_str());

                // If types changed, it's a breaking change
                if old_type != new_type {
                    return true;
                }

                // Check nullability changes (Delta uses "nullable", Iceberg uses "required")
                // Non-nullable becoming nullable is breaking for Delta
                // Not-required becoming required is breaking for Iceberg
                if has_nullability_breaking_change(old_field, new_field) {
                    return true;
                }
            }
        }
    }
    false
}

/// Check for nullability breaking changes between two fields.
///
/// Handles both Delta's "nullable" field and Iceberg's "required" field.
fn has_nullability_breaking_change(old_field: &Value, new_field: &Value) -> bool {
    // Delta style: nullable field
    if let (Some(old_nullable), Some(new_nullable)) = (
        old_field.get("nullable").and_then(|n| n.as_bool()),
        new_field.get("nullable").and_then(|n| n.as_bool()),
    ) {
        // Non-nullable becoming nullable is breaking
        if !old_nullable && new_nullable {
            return true;
        }
    }

    // Iceberg style: required field
    if let (Some(old_required), Some(new_required)) = (
        old_field.get("required").and_then(|r| r.as_bool()),
        new_field.get("required").and_then(|r| r.as_bool()),
    ) {
        // Not-required becoming required is breaking
        if !old_required && new_required {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_schema_change_new() {
        let schema = json!({"fields": []});
        let change = SchemaChange::new(1, 1000, schema.clone(), false);
        assert_eq!(change.version, 1);
        assert_eq!(change.timestamp, 1000);
        assert!(!change.is_breaking);
    }

    #[test]
    fn test_is_breaking_change_empty_previous() {
        let new_schema = json!({"fields": [{"name": "id", "type": "int"}]});
        assert!(!is_breaking_change(&[], &new_schema));
    }

    #[test]
    fn test_detect_breaking_change_column_removal() {
        let old_schema = json!({
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        });
        let new_schema = json!({
            "fields": [
                {"name": "id", "type": "int"}
            ]
        });
        assert!(detect_breaking_schema_changes(&old_schema, &new_schema));
    }

    #[test]
    fn test_detect_breaking_change_type_change() {
        let old_schema = json!({
            "fields": [{"name": "id", "type": "int"}]
        });
        let new_schema = json!({
            "fields": [{"name": "id", "type": "string"}]
        });
        assert!(detect_breaking_schema_changes(&old_schema, &new_schema));
    }

    #[test]
    fn test_detect_no_breaking_change_add_column() {
        let old_schema = json!({
            "fields": [{"name": "id", "type": "int"}]
        });
        let new_schema = json!({
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        });
        assert!(!detect_breaking_schema_changes(&old_schema, &new_schema));
    }
}
