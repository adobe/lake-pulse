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

use super::metrics::{
    FileStatistics, PaimonMetrics, PartitionMetrics, SchemaMetrics, SnapshotMetrics, TableMetadata,
};
use apache_avro::from_value;
use apache_avro::Reader as AvroReader;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tracing::{debug, info, warn};
use url::Url;

/// Paimon snapshot JSON structure
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaimonSnapshot {
    pub version: Option<i32>,
    pub id: i64,
    pub schema_id: i64,
    pub base_manifest_list: Option<String>,
    pub delta_manifest_list: Option<String>,
    pub changelog_manifest_list: Option<String>,
    pub commit_kind: Option<String>,
    pub time_millis: Option<i64>,
    pub log_offsets: Option<HashMap<String, i64>>,
    pub total_record_count: Option<i64>,
    pub delta_record_count: Option<i64>,
    pub changelog_record_count: Option<i64>,
    pub watermark: Option<i64>,
}

/// Paimon schema JSON structure
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaimonSchema {
    pub id: i64,
    pub fields: Vec<PaimonField>,
    pub highest_field_id: Option<i32>,
    pub partition_keys: Vec<String>,
    pub primary_keys: Vec<String>,
    pub options: Option<HashMap<String, String>>,
    pub comment: Option<String>,
    pub time_millis: Option<i64>,
}

/// Paimon field structure
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PaimonField {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: serde_json::Value,
    pub description: Option<String>,
}

/// Manifest list entry from Avro manifest-list file
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ManifestListEntry {
    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i64,
    #[serde(rename = "_NUM_ADDED_FILES")]
    pub num_added_files: i64,
    #[serde(rename = "_NUM_DELETED_FILES")]
    pub num_deleted_files: i64,
    #[serde(rename = "_SCHEMA_ID")]
    pub schema_id: i64,
}

/// Data file meta from manifest entry
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DataFileMeta {
    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i64,
    #[serde(rename = "_ROW_COUNT")]
    pub row_count: i64,
    #[serde(rename = "_MIN_SEQUENCE_NUMBER")]
    pub min_sequence_number: Option<i64>,
    #[serde(rename = "_MAX_SEQUENCE_NUMBER")]
    pub max_sequence_number: Option<i64>,
    #[serde(rename = "_SCHEMA_ID")]
    pub schema_id: i64,
    #[serde(rename = "_LEVEL")]
    pub level: i32,
    #[serde(rename = "_CREATION_TIME")]
    pub creation_time: Option<i64>,
    #[serde(rename = "_DELETE_ROW_COUNT")]
    pub delete_row_count: Option<i64>,
}

/// Manifest entry from Avro manifest file
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ManifestEntry {
    #[serde(rename = "_KIND")]
    pub kind: i8, // 0 = ADD, 1 = DELETE
    #[serde(rename = "_BUCKET")]
    pub bucket: i32,
    #[serde(rename = "_TOTAL_BUCKETS")]
    pub total_buckets: i32,
    #[serde(rename = "_FILE")]
    pub file: DataFileMeta,
}

/// Paimon table reader for extracting metrics.
pub struct PaimonReader {
    store: Arc<dyn ObjectStore>,
    base_path: Path,
    current_snapshot: Option<PaimonSnapshot>,
    current_schema: Option<PaimonSchema>,
}

impl PaimonReader {
    /// Open a Paimon table from the given location.
    pub async fn open(
        location: &str,
        storage_options: &HashMap<String, String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        info!("Opening Paimon table at location={}", location);

        let url = Url::parse(location)?;
        let (store, base_path) = object_store::parse_url_opts(&url, storage_options.iter())?;
        let store = Arc::new(store);

        // Read the LATEST file to get current snapshot ID
        let latest_path = base_path.child("snapshot").child("LATEST");
        let current_snapshot_id = match store.get(&latest_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8_lossy(&bytes);
                content.trim().parse::<i64>().unwrap_or(1)
            }
            Err(e) => {
                warn!("Failed to read LATEST file: {}, trying snapshot-1", e);
                1
            }
        };

        // Read current snapshot
        let snapshot_path = base_path
            .child("snapshot")
            .child(format!("snapshot-{}", current_snapshot_id));
        let current_snapshot = match store.get(&snapshot_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                match serde_json::from_slice::<PaimonSnapshot>(&bytes) {
                    Ok(snapshot) => Some(snapshot),
                    Err(e) => {
                        warn!("Failed to parse snapshot JSON: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read snapshot file: {}", e);
                None
            }
        };

        // Read current schema
        let schema_id = current_snapshot.as_ref().map(|s| s.schema_id).unwrap_or(0);
        let schema_path = base_path
            .child("schema")
            .child(format!("schema-{}", schema_id));
        let current_schema = match store.get(&schema_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                match serde_json::from_slice::<PaimonSchema>(&bytes) {
                    Ok(schema) => Some(schema),
                    Err(e) => {
                        warn!("Failed to parse schema JSON: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read schema file: {}", e);
                None
            }
        };

        info!(
            "Opened Paimon table: snapshot_id={}, schema_id={}",
            current_snapshot_id, schema_id
        );

        Ok(Self {
            store,
            base_path,
            current_snapshot,
            current_schema,
        })
    }

    /// Extract comprehensive metrics from the Paimon table.
    pub async fn extract_metrics(&self) -> Result<PaimonMetrics, Box<dyn Error + Send + Sync>> {
        info!("Extracting metrics from Paimon table");

        let snapshot_info = self.extract_snapshot_metrics().await?;
        let schema_info = self.extract_schema_metrics().await?;
        let file_stats = self.extract_file_statistics().await?;
        let partition_info = self.extract_partition_metrics().await?;
        let metadata = self.extract_table_metadata().await?;

        let table_name = self
            .base_path
            .filename()
            .map(|s| s.to_string())
            .unwrap_or_default();

        Ok(PaimonMetrics {
            table_name,
            metadata,
            snapshot_info,
            schema_info,
            file_stats,
            partition_info,
        })
    }

    async fn extract_snapshot_metrics(
        &self,
    ) -> Result<SnapshotMetrics, Box<dyn Error + Send + Sync>> {
        let snapshot_dir = self.base_path.child("snapshot");
        let mut total_snapshots = 0usize;
        let mut earliest_snapshot_id: Option<i64> = None;

        let mut stream = self.store.list(Some(&snapshot_dir));
        while let Some(item) = stream.next().await {
            if let Ok(meta) = item {
                let filename = meta.location.filename().unwrap_or("");
                if filename.starts_with("snapshot-") {
                    total_snapshots += 1;
                    if let Some(id_str) = filename.strip_prefix("snapshot-") {
                        if let Ok(id) = id_str.parse::<i64>() {
                            earliest_snapshot_id =
                                Some(earliest_snapshot_id.map_or(id, |c| c.min(id)));
                        }
                    }
                }
            }
        }

        let (current_snapshot_id, latest_commit_time, latest_commit_kind, total_record_count) =
            if let Some(ref snapshot) = self.current_snapshot {
                (
                    snapshot.id,
                    snapshot.time_millis,
                    snapshot.commit_kind.clone(),
                    snapshot.total_record_count.unwrap_or(0),
                )
            } else {
                (0, None, None, 0)
            };

        Ok(SnapshotMetrics {
            current_snapshot_id,
            total_snapshots,
            earliest_snapshot_id,
            latest_commit_time,
            earliest_commit_time: None,
            latest_commit_kind,
            total_record_count,
        })
    }

    async fn extract_schema_metrics(&self) -> Result<SchemaMetrics, Box<dyn Error + Send + Sync>> {
        let schema_dir = self.base_path.child("schema");
        let mut total_schema_versions = 0usize;

        let mut stream = self.store.list(Some(&schema_dir));
        while let Some(item) = stream.next().await {
            if let Ok(meta) = item {
                let filename = meta.location.filename().unwrap_or("");
                if filename.starts_with("schema-") {
                    total_schema_versions += 1;
                }
            }
        }

        let (current_schema_id, schema_string) = if let Some(ref schema) = self.current_schema {
            (schema.id, serde_json::to_string(schema).unwrap_or_default())
        } else {
            (0, String::new())
        };

        Ok(SchemaMetrics {
            current_schema_id,
            total_schema_versions,
            schema_string,
        })
    }

    async fn extract_file_statistics(
        &self,
    ) -> Result<FileStatistics, Box<dyn Error + Send + Sync>> {
        // Try to get accurate stats from manifest parsing first
        let data_files = self.get_data_files().await.unwrap_or_default();

        let mut num_manifest_files = 0usize;
        let mut total_manifest_size_bytes = 0u64;

        // Count manifest files by listing
        let manifest_dir = self.base_path.child("manifest");
        let mut stream = self.store.list(Some(&manifest_dir));
        while let Some(item) = stream.next().await {
            if let Ok(meta) = item {
                let filename = meta.location.filename().unwrap_or("");
                if filename.starts_with("manifest-") && filename.ends_with(".avro") {
                    num_manifest_files += 1;
                    total_manifest_size_bytes += meta.size;
                }
            }
        }

        if !data_files.is_empty() {
            // Use manifest-derived statistics
            let num_data_files = data_files.len();
            let total_data_size_bytes: u64 = data_files.iter().map(|f| f.file_size as u64).sum();
            let min_size = data_files
                .iter()
                .map(|f| f.file_size as u64)
                .min()
                .unwrap_or(0);
            let max_size = data_files
                .iter()
                .map(|f| f.file_size as u64)
                .max()
                .unwrap_or(0);
            let avg_size = if num_data_files > 0 {
                total_data_size_bytes as f64 / num_data_files as f64
            } else {
                0.0
            };

            Ok(FileStatistics {
                num_data_files,
                total_data_size_bytes,
                avg_data_file_size_bytes: avg_size,
                min_data_file_size_bytes: min_size,
                max_data_file_size_bytes: max_size,
                num_manifest_files,
                total_manifest_size_bytes,
            })
        } else {
            // Fallback to listing files
            let mut num_data_files = 0usize;
            let mut total_data_size_bytes = 0u64;
            let mut min_size = u64::MAX;
            let mut max_size = 0u64;

            let mut stream = self.store.list(Some(&self.base_path));
            while let Some(item) = stream.next().await {
                if let Ok(meta) = item {
                    let path = meta.location.to_string();
                    let size = meta.size;

                    if path.ends_with(".orc") || path.ends_with(".parquet") {
                        num_data_files += 1;
                        total_data_size_bytes += size;
                        min_size = min_size.min(size);
                        max_size = max_size.max(size);
                    }
                }
            }

            let avg_size = if num_data_files > 0 {
                total_data_size_bytes as f64 / num_data_files as f64
            } else {
                0.0
            };

            Ok(FileStatistics {
                num_data_files,
                total_data_size_bytes,
                avg_data_file_size_bytes: avg_size,
                min_data_file_size_bytes: if min_size == u64::MAX { 0 } else { min_size },
                max_data_file_size_bytes: max_size,
                num_manifest_files,
                total_manifest_size_bytes,
            })
        }
    }

    async fn extract_partition_metrics(
        &self,
    ) -> Result<PartitionMetrics, Box<dyn Error + Send + Sync>> {
        let num_partition_keys = self
            .current_schema
            .as_ref()
            .map(|s| s.partition_keys.len())
            .unwrap_or(0);

        // Count unique partitions and buckets by listing data directories
        let mut partitions = std::collections::HashSet::new();
        let mut buckets = std::collections::HashSet::new();

        let mut stream = self.store.list(Some(&self.base_path));
        while let Some(item) = stream.next().await {
            if let Ok(meta) = item {
                let path = meta.location.to_string();
                // Extract partition path (e.g., "dt=2024-01-01")
                for part in path.split('/') {
                    if part.contains('=') && !part.starts_with("bucket-") {
                        partitions.insert(part.to_string());
                    }
                    if part.starts_with("bucket-") {
                        buckets.insert(part.to_string());
                    }
                }
            }
        }

        Ok(PartitionMetrics {
            num_partition_keys,
            num_partitions: partitions.len(),
            num_buckets: buckets.len(),
            largest_partition_size_bytes: 0,
            smallest_partition_size_bytes: 0,
            avg_partition_size_bytes: 0.0,
        })
    }

    async fn extract_table_metadata(&self) -> Result<TableMetadata, Box<dyn Error + Send + Sync>> {
        let (field_count, primary_keys, partition_keys, comment) =
            if let Some(ref schema) = self.current_schema {
                (
                    schema.fields.len(),
                    schema.primary_keys.clone(),
                    schema.partition_keys.clone(),
                    schema.comment.clone(),
                )
            } else {
                (0, Vec::new(), Vec::new(), None)
            };

        // Get bucket count from schema options
        let bucket_count = self
            .current_schema
            .as_ref()
            .and_then(|s| s.options.as_ref())
            .and_then(|opts| opts.get("bucket"))
            .and_then(|b| b.parse::<i32>().ok())
            .unwrap_or(-1);

        Ok(TableMetadata {
            uuid: String::new(),
            format_version: self
                .current_snapshot
                .as_ref()
                .and_then(|s| s.version)
                .unwrap_or(0),
            field_count,
            primary_keys,
            partition_keys,
            bucket_count,
            comment,
        })
    }

    /// Parse a manifest-list Avro file and return the list of manifest entries.
    async fn parse_manifest_list(
        &self,
        manifest_list_name: &str,
    ) -> Result<Vec<ManifestListEntry>, Box<dyn Error + Send + Sync>> {
        let manifest_list_path = self.base_path.child("manifest").child(manifest_list_name);
        debug!("Parsing manifest-list: {}", manifest_list_path);

        let result = self.store.get(&manifest_list_path).await?;
        let bytes = result.bytes().await?;

        let mut entries = Vec::new();
        let reader = AvroReader::new(bytes.as_ref())?;

        for value in reader {
            match value {
                Ok(v) => {
                    if let Ok(entry) = from_value::<ManifestListEntry>(&v) {
                        entries.push(entry);
                    }
                }
                Err(e) => {
                    warn!("Failed to read manifest-list entry: {}", e);
                }
            }
        }

        debug!("Parsed {} manifest-list entries", entries.len());
        Ok(entries)
    }

    /// Parse a manifest Avro file and return the list of manifest entries.
    async fn parse_manifest(
        &self,
        manifest_name: &str,
    ) -> Result<Vec<ManifestEntry>, Box<dyn Error + Send + Sync>> {
        let manifest_path = self.base_path.child("manifest").child(manifest_name);
        debug!("Parsing manifest: {}", manifest_path);

        let result = self.store.get(&manifest_path).await?;
        let bytes = result.bytes().await?;

        let mut entries = Vec::new();
        let reader = AvroReader::new(bytes.as_ref())?;

        for value in reader {
            match value {
                Ok(v) => {
                    if let Ok(entry) = from_value::<ManifestEntry>(&v) {
                        entries.push(entry);
                    }
                }
                Err(e) => {
                    warn!("Failed to read manifest entry: {}", e);
                }
            }
        }

        debug!("Parsed {} manifest entries", entries.len());
        Ok(entries)
    }

    /// Get all data file entries from the current snapshot's manifests.
    pub async fn get_data_files(&self) -> Result<Vec<DataFileMeta>, Box<dyn Error + Send + Sync>> {
        let snapshot = match &self.current_snapshot {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let mut all_files: HashMap<String, DataFileMeta> = HashMap::new();

        // Process base manifest list (all changes from previous snapshots)
        if let Some(ref base_manifest_list) = snapshot.base_manifest_list {
            if let Ok(manifest_entries) = self.parse_manifest_list(base_manifest_list).await {
                for manifest_entry in manifest_entries {
                    if let Ok(entries) = self.parse_manifest(&manifest_entry.file_name).await {
                        for entry in entries {
                            if entry.kind == 0 {
                                // ADD
                                all_files.insert(entry.file.file_name.clone(), entry.file);
                            } else {
                                // DELETE
                                all_files.remove(&entry.file.file_name);
                            }
                        }
                    }
                }
            }
        }

        // Process delta manifest list (new changes in this snapshot)
        if let Some(ref delta_manifest_list) = snapshot.delta_manifest_list {
            if let Ok(manifest_entries) = self.parse_manifest_list(delta_manifest_list).await {
                for manifest_entry in manifest_entries {
                    if let Ok(entries) = self.parse_manifest(&manifest_entry.file_name).await {
                        for entry in entries {
                            if entry.kind == 0 {
                                // ADD
                                all_files.insert(entry.file.file_name.clone(), entry.file);
                            } else {
                                // DELETE
                                all_files.remove(&entry.file.file_name);
                            }
                        }
                    }
                }
            }
        }

        Ok(all_files.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // Helper to create a minimal Paimon table structure for testing
    fn create_paimon_table(temp_dir: &TempDir) -> String {
        let base_path = temp_dir.path();

        // Create directories
        fs::create_dir_all(base_path.join("snapshot")).unwrap();
        fs::create_dir_all(base_path.join("schema")).unwrap();
        fs::create_dir_all(base_path.join("manifest")).unwrap();
        fs::create_dir_all(base_path.join("bucket-0")).unwrap();

        // Create LATEST file
        fs::write(base_path.join("snapshot/LATEST"), "3").unwrap();

        // Create snapshot files
        let snapshot1 = r#"{"id": 1, "schemaId": 0, "timeMillis": 1700000000000, "commitKind": "APPEND", "totalRecordCount": 100}"#;
        let snapshot2 = r#"{"id": 2, "schemaId": 0, "timeMillis": 1705000000000, "commitKind": "APPEND", "totalRecordCount": 500}"#;
        let snapshot3 = r#"{"id": 3, "schemaId": 1, "timeMillis": 1710000000000, "commitKind": "COMPACT", "totalRecordCount": 1000, "deltaRecordCount": 200}"#;
        fs::write(base_path.join("snapshot/snapshot-1"), snapshot1).unwrap();
        fs::write(base_path.join("snapshot/snapshot-2"), snapshot2).unwrap();
        fs::write(base_path.join("snapshot/snapshot-3"), snapshot3).unwrap();

        // Create schema files
        let schema0 = r#"{
            "id": 0,
            "fields": [
                {"id": 0, "name": "id", "type": "BIGINT"},
                {"id": 1, "name": "name", "type": "STRING"}
            ],
            "highestFieldId": 1,
            "partitionKeys": [],
            "primaryKeys": ["id"],
            "options": {"bucket": "4"}
        }"#;
        let schema1 = r#"{
            "id": 1,
            "fields": [
                {"id": 0, "name": "id", "type": "BIGINT"},
                {"id": 1, "name": "name", "type": "STRING"},
                {"id": 2, "name": "created_at", "type": "TIMESTAMP"}
            ],
            "highestFieldId": 2,
            "partitionKeys": ["dt"],
            "primaryKeys": ["id"],
            "options": {"bucket": "8"},
            "comment": "Test table with timestamp"
        }"#;
        fs::write(base_path.join("schema/schema-0"), schema0).unwrap();
        fs::write(base_path.join("schema/schema-1"), schema1).unwrap();

        // Create some dummy data files
        fs::write(base_path.join("bucket-0/data-0001.orc"), "dummy orc data").unwrap();
        fs::write(base_path.join("bucket-0/data-0002.orc"), "dummy orc data 2").unwrap();

        format!("file://{}", base_path.display())
    }

    // Helper to create a Paimon table with partitions
    fn create_partitioned_paimon_table(temp_dir: &TempDir) -> String {
        let base_path = temp_dir.path();

        // Create directories
        fs::create_dir_all(base_path.join("snapshot")).unwrap();
        fs::create_dir_all(base_path.join("schema")).unwrap();
        fs::create_dir_all(base_path.join("manifest")).unwrap();
        fs::create_dir_all(base_path.join("dt=2024-01-01/bucket-0")).unwrap();
        fs::create_dir_all(base_path.join("dt=2024-01-02/bucket-0")).unwrap();
        fs::create_dir_all(base_path.join("dt=2024-01-02/bucket-1")).unwrap();

        // Create LATEST file
        fs::write(base_path.join("snapshot/LATEST"), "1").unwrap();

        // Create snapshot
        let snapshot = r#"{"id": 1, "schemaId": 0, "timeMillis": 1705000000000, "commitKind": "APPEND", "totalRecordCount": 5000}"#;
        fs::write(base_path.join("snapshot/snapshot-1"), snapshot).unwrap();

        // Create schema with partition keys
        let schema = r#"{
            "id": 0,
            "fields": [
                {"id": 0, "name": "id", "type": "BIGINT"},
                {"id": 1, "name": "dt", "type": "STRING"}
            ],
            "highestFieldId": 1,
            "partitionKeys": ["dt"],
            "primaryKeys": ["id"],
            "options": {"bucket": "2"}
        }"#;
        fs::write(base_path.join("schema/schema-0"), schema).unwrap();

        // Create data files in partitions
        fs::write(
            base_path.join("dt=2024-01-01/bucket-0/data-0001.parquet"),
            vec![0u8; 1024 * 100], // 100KB
        )
        .unwrap();
        fs::write(
            base_path.join("dt=2024-01-02/bucket-0/data-0002.parquet"),
            vec![0u8; 1024 * 200], // 200KB
        )
        .unwrap();
        fs::write(
            base_path.join("dt=2024-01-02/bucket-1/data-0003.parquet"),
            vec![0u8; 1024 * 150], // 150KB
        )
        .unwrap();

        format!("file://{}", base_path.display())
    }

    // Helper to create a minimal table without LATEST file
    fn create_paimon_table_no_latest(temp_dir: &TempDir) -> String {
        let base_path = temp_dir.path();

        fs::create_dir_all(base_path.join("snapshot")).unwrap();
        fs::create_dir_all(base_path.join("schema")).unwrap();

        // No LATEST file - should fall back to snapshot-1
        let snapshot = r#"{"id": 1, "schemaId": 0, "timeMillis": 1705000000000}"#;
        fs::write(base_path.join("snapshot/snapshot-1"), snapshot).unwrap();

        let schema = r#"{"id": 0, "fields": [], "partitionKeys": [], "primaryKeys": []}"#;
        fs::write(base_path.join("schema/schema-0"), schema).unwrap();

        format!("file://{}", base_path.display())
    }

    // Helper to create a table with invalid JSON
    fn create_paimon_table_invalid_json(temp_dir: &TempDir) -> String {
        let base_path = temp_dir.path();

        fs::create_dir_all(base_path.join("snapshot")).unwrap();
        fs::create_dir_all(base_path.join("schema")).unwrap();

        fs::write(base_path.join("snapshot/LATEST"), "1").unwrap();
        fs::write(base_path.join("snapshot/snapshot-1"), "not valid json").unwrap();
        fs::write(base_path.join("schema/schema-0"), "also not valid").unwrap();

        format!("file://{}", base_path.display())
    }

    // ========== PaimonReader::open tests ==========

    #[tokio::test]
    async fn test_open_paimon_table() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();

        assert!(reader.current_snapshot.is_some());
        let snapshot = reader.current_snapshot.unwrap();
        assert_eq!(snapshot.id, 3);
        assert_eq!(snapshot.schema_id, 1);
        assert_eq!(snapshot.commit_kind, Some("COMPACT".to_string()));

        assert!(reader.current_schema.is_some());
        let schema = reader.current_schema.unwrap();
        assert_eq!(schema.id, 1);
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.partition_keys, vec!["dt"]);
    }

    #[tokio::test]
    async fn test_open_paimon_table_no_latest() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table_no_latest(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();

        // Should fall back to snapshot-1
        assert!(reader.current_snapshot.is_some());
        assert_eq!(reader.current_snapshot.as_ref().unwrap().id, 1);
    }

    #[tokio::test]
    async fn test_open_paimon_table_invalid_json() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table_invalid_json(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();

        // Should handle invalid JSON gracefully
        assert!(reader.current_snapshot.is_none());
        assert!(reader.current_schema.is_none());
    }

    #[tokio::test]
    async fn test_open_invalid_url() {
        let result = PaimonReader::open("not-a-valid-url", &HashMap::new()).await;
        assert!(result.is_err());
    }

    // ========== extract_metrics tests ==========

    #[tokio::test]
    async fn test_extract_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let metrics = reader.extract_metrics().await.unwrap();

        // Check snapshot metrics
        assert_eq!(metrics.snapshot_info.current_snapshot_id, 3);
        assert_eq!(metrics.snapshot_info.total_snapshots, 3);
        assert_eq!(metrics.snapshot_info.earliest_snapshot_id, Some(1));
        assert_eq!(metrics.snapshot_info.total_record_count, 1000);

        // Check schema metrics
        assert_eq!(metrics.schema_info.current_schema_id, 1);
        assert_eq!(metrics.schema_info.total_schema_versions, 2);

        // Check table metadata
        assert_eq!(metrics.metadata.field_count, 3);
        assert_eq!(metrics.metadata.primary_keys, vec!["id"]);
        assert_eq!(metrics.metadata.partition_keys, vec!["dt"]);
        assert_eq!(metrics.metadata.bucket_count, 8);
    }

    #[tokio::test]
    async fn test_extract_metrics_partitioned_table() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_partitioned_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let metrics = reader.extract_metrics().await.unwrap();

        // Check partition metrics
        assert_eq!(metrics.partition_info.num_partition_keys, 1);
        assert!(metrics.partition_info.num_partitions >= 2); // dt=2024-01-01, dt=2024-01-02
        assert!(metrics.partition_info.num_buckets >= 2); // bucket-0, bucket-1

        // Check file statistics
        assert!(metrics.file_stats.num_data_files >= 3);
        assert!(metrics.file_stats.total_data_size_bytes > 0);
    }

    // ========== extract_snapshot_metrics tests ==========

    #[tokio::test]
    async fn test_extract_snapshot_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let snapshot_metrics = reader.extract_snapshot_metrics().await.unwrap();

        assert_eq!(snapshot_metrics.current_snapshot_id, 3);
        assert_eq!(snapshot_metrics.total_snapshots, 3);
        assert_eq!(snapshot_metrics.earliest_snapshot_id, Some(1));
        assert_eq!(
            snapshot_metrics.latest_commit_kind,
            Some("COMPACT".to_string())
        );
        assert_eq!(snapshot_metrics.total_record_count, 1000);
    }

    #[tokio::test]
    async fn test_extract_snapshot_metrics_no_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table_invalid_json(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let snapshot_metrics = reader.extract_snapshot_metrics().await.unwrap();

        // Should handle missing snapshot gracefully
        assert_eq!(snapshot_metrics.current_snapshot_id, 0);
        assert_eq!(snapshot_metrics.total_record_count, 0);
    }

    // ========== extract_schema_metrics tests ==========

    #[tokio::test]
    async fn test_extract_schema_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let schema_metrics = reader.extract_schema_metrics().await.unwrap();

        assert_eq!(schema_metrics.current_schema_id, 1);
        assert_eq!(schema_metrics.total_schema_versions, 2);
        assert!(!schema_metrics.schema_string.is_empty());
    }

    #[tokio::test]
    async fn test_extract_schema_metrics_no_schema() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table_invalid_json(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let schema_metrics = reader.extract_schema_metrics().await.unwrap();

        assert_eq!(schema_metrics.current_schema_id, 0);
        assert!(schema_metrics.schema_string.is_empty());
    }

    // ========== extract_file_statistics tests ==========

    #[tokio::test]
    async fn test_extract_file_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_partitioned_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let file_stats = reader.extract_file_statistics().await.unwrap();

        // Should find the parquet files
        assert!(file_stats.num_data_files >= 3);
        assert!(file_stats.total_data_size_bytes > 0);
        assert!(file_stats.avg_data_file_size_bytes > 0.0);
    }

    #[tokio::test]
    async fn test_extract_file_statistics_empty_table() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create minimal structure with no data files
        fs::create_dir_all(base_path.join("snapshot")).unwrap();
        fs::create_dir_all(base_path.join("schema")).unwrap();
        fs::write(base_path.join("snapshot/LATEST"), "1").unwrap();
        fs::write(
            base_path.join("snapshot/snapshot-1"),
            r#"{"id": 1, "schemaId": 0}"#,
        )
        .unwrap();
        fs::write(
            base_path.join("schema/schema-0"),
            r#"{"id": 0, "fields": [], "partitionKeys": [], "primaryKeys": []}"#,
        )
        .unwrap();

        let location = format!("file://{}", base_path.display());
        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let file_stats = reader.extract_file_statistics().await.unwrap();

        assert_eq!(file_stats.num_data_files, 0);
        assert_eq!(file_stats.total_data_size_bytes, 0);
    }

    // ========== extract_partition_metrics tests ==========

    #[tokio::test]
    async fn test_extract_partition_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_partitioned_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let partition_metrics = reader.extract_partition_metrics().await.unwrap();

        assert_eq!(partition_metrics.num_partition_keys, 1);
        assert!(partition_metrics.num_partitions >= 2);
        assert!(partition_metrics.num_buckets >= 2);
    }

    #[tokio::test]
    async fn test_extract_partition_metrics_no_partitions() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let partition_metrics = reader.extract_partition_metrics().await.unwrap();

        // Schema 1 has partition keys but no actual partition directories in this test
        assert_eq!(partition_metrics.num_partition_keys, 1);
    }

    // ========== extract_table_metadata tests ==========

    #[tokio::test]
    async fn test_extract_table_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let metadata = reader.extract_table_metadata().await.unwrap();

        assert_eq!(metadata.field_count, 3);
        assert_eq!(metadata.primary_keys, vec!["id"]);
        assert_eq!(metadata.partition_keys, vec!["dt"]);
        assert_eq!(metadata.bucket_count, 8);
        assert_eq!(
            metadata.comment,
            Some("Test table with timestamp".to_string())
        );
        assert_eq!(metadata.format_version, 0); // No version in snapshot
    }

    #[tokio::test]
    async fn test_extract_table_metadata_no_schema() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table_invalid_json(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let metadata = reader.extract_table_metadata().await.unwrap();

        assert_eq!(metadata.field_count, 0);
        assert!(metadata.primary_keys.is_empty());
        assert!(metadata.partition_keys.is_empty());
        assert_eq!(metadata.bucket_count, -1);
    }

    // ========== get_data_files tests ==========

    #[tokio::test]
    async fn test_get_data_files_no_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table_invalid_json(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let data_files = reader.get_data_files().await.unwrap();

        // No snapshot means no data files from manifest
        assert!(data_files.is_empty());
    }

    #[tokio::test]
    async fn test_get_data_files_no_manifest_lists() {
        let temp_dir = TempDir::new().unwrap();
        let location = create_paimon_table(&temp_dir);

        let reader = PaimonReader::open(&location, &HashMap::new())
            .await
            .unwrap();
        let data_files = reader.get_data_files().await.unwrap();

        // Snapshot exists but no manifest lists defined
        assert!(data_files.is_empty());
    }

    #[test]
    fn test_parse_snapshot_json() {
        let json = r#"{
            "version": 3,
            "id": 5,
            "schemaId": 0,
            "baseManifestList": "manifest-list-abc-0",
            "deltaManifestList": "manifest-list-abc-1",
            "commitKind": "APPEND",
            "timeMillis": 1705000000000,
            "totalRecordCount": 1000,
            "deltaRecordCount": 100,
            "changelogRecordCount": 0
        }"#;

        let snapshot: PaimonSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.id, 5);
        assert_eq!(snapshot.schema_id, 0);
        assert_eq!(snapshot.version, Some(3));
        assert_eq!(snapshot.commit_kind, Some("APPEND".to_string()));
        assert_eq!(snapshot.time_millis, Some(1705000000000));
        assert_eq!(snapshot.total_record_count, Some(1000));
    }

    #[test]
    fn test_parse_snapshot_minimal() {
        let json = r#"{"id": 1, "schemaId": 0}"#;
        let snapshot: PaimonSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.id, 1);
        assert_eq!(snapshot.schema_id, 0);
        assert!(snapshot.version.is_none());
        assert!(snapshot.base_manifest_list.is_none());
    }

    #[test]
    fn test_parse_schema_json() {
        let json = r#"{
            "id": 0,
            "fields": [
                {"id": 0, "name": "id", "type": "INT"},
                {"id": 1, "name": "name", "type": "STRING"}
            ],
            "highestFieldId": 1,
            "partitionKeys": ["dt"],
            "primaryKeys": ["id"],
            "options": {"bucket": "4"},
            "comment": "Test table"
        }"#;

        let schema: PaimonSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.id, 0);
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.partition_keys, vec!["dt"]);
        assert_eq!(schema.primary_keys, vec!["id"]);
        assert_eq!(schema.comment, Some("Test table".to_string()));
        assert_eq!(
            schema.options.as_ref().unwrap().get("bucket"),
            Some(&"4".to_string())
        );
    }

    #[test]
    fn test_parse_schema_minimal() {
        let json = r#"{
            "id": 0,
            "fields": [],
            "partitionKeys": [],
            "primaryKeys": []
        }"#;

        let schema: PaimonSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.id, 0);
        assert!(schema.fields.is_empty());
        assert!(schema.partition_keys.is_empty());
        assert!(schema.primary_keys.is_empty());
    }

    #[test]
    fn test_parse_field() {
        let json = r#"{
            "id": 0,
            "name": "user_id",
            "type": "BIGINT",
            "description": "User identifier"
        }"#;

        let field: PaimonField = serde_json::from_str(json).unwrap();
        assert_eq!(field.id, 0);
        assert_eq!(field.name, "user_id");
        assert_eq!(field.description, Some("User identifier".to_string()));
    }

    #[test]
    fn test_manifest_list_entry_default() {
        let entry = ManifestListEntry::default();
        assert!(entry.file_name.is_empty());
        assert_eq!(entry.file_size, 0);
        assert_eq!(entry.num_added_files, 0);
        assert_eq!(entry.num_deleted_files, 0);
    }

    #[test]
    fn test_manifest_entry_default() {
        let entry = ManifestEntry::default();
        assert_eq!(entry.kind, 0);
        assert_eq!(entry.bucket, 0);
        assert_eq!(entry.total_buckets, 0);
    }

    #[test]
    fn test_data_file_meta_default() {
        let meta = DataFileMeta::default();
        assert!(meta.file_name.is_empty());
        assert_eq!(meta.file_size, 0);
        assert_eq!(meta.row_count, 0);
        assert_eq!(meta.level, 0);
    }

    // ========== Additional snapshot parsing tests ==========

    #[test]
    fn test_parse_snapshot_with_log_offsets() {
        let json = r#"{
            "id": 10,
            "schemaId": 2,
            "logOffsets": {"partition-0": 100, "partition-1": 200},
            "watermark": 1705000000000
        }"#;

        let snapshot: PaimonSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.id, 10);
        assert_eq!(snapshot.schema_id, 2);
        assert!(snapshot.log_offsets.is_some());
        let offsets = snapshot.log_offsets.unwrap();
        assert_eq!(offsets.get("partition-0"), Some(&100));
        assert_eq!(offsets.get("partition-1"), Some(&200));
        assert_eq!(snapshot.watermark, Some(1705000000000));
    }

    #[test]
    fn test_parse_snapshot_all_commit_kinds() {
        for kind in &["APPEND", "COMPACT", "OVERWRITE", "ANALYZE"] {
            let json = format!(r#"{{"id": 1, "schemaId": 0, "commitKind": "{}"}}"#, kind);
            let snapshot: PaimonSnapshot = serde_json::from_str(&json).unwrap();
            assert_eq!(snapshot.commit_kind, Some(kind.to_string()));
        }
    }

    #[test]
    fn test_parse_snapshot_with_changelog() {
        let json = r#"{
            "id": 5,
            "schemaId": 0,
            "changelogManifestList": "changelog-manifest-list-abc",
            "changelogRecordCount": 500
        }"#;

        let snapshot: PaimonSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(
            snapshot.changelog_manifest_list,
            Some("changelog-manifest-list-abc".to_string())
        );
        assert_eq!(snapshot.changelog_record_count, Some(500));
    }

    #[test]
    fn test_snapshot_default() {
        let snapshot = PaimonSnapshot::default();
        assert_eq!(snapshot.id, 0);
        assert_eq!(snapshot.schema_id, 0);
        assert!(snapshot.version.is_none());
        assert!(snapshot.base_manifest_list.is_none());
        assert!(snapshot.delta_manifest_list.is_none());
        assert!(snapshot.changelog_manifest_list.is_none());
        assert!(snapshot.commit_kind.is_none());
        assert!(snapshot.time_millis.is_none());
        assert!(snapshot.log_offsets.is_none());
        assert!(snapshot.total_record_count.is_none());
        assert!(snapshot.delta_record_count.is_none());
        assert!(snapshot.changelog_record_count.is_none());
        assert!(snapshot.watermark.is_none());
    }

    // ========== Additional schema parsing tests ==========

    #[test]
    fn test_parse_schema_with_complex_types() {
        let json = r#"{
            "id": 1,
            "fields": [
                {"id": 0, "name": "id", "type": "BIGINT"},
                {"id": 1, "name": "data", "type": {"type": "ARRAY", "element": "STRING"}},
                {"id": 2, "name": "metadata", "type": {"type": "MAP", "key": "STRING", "value": "INT"}}
            ],
            "highestFieldId": 2,
            "partitionKeys": [],
            "primaryKeys": ["id"]
        }"#;

        let schema: PaimonSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.id, 1);
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.highest_field_id, Some(2));
    }

    #[test]
    fn test_parse_schema_with_time_millis() {
        let json = r#"{
            "id": 0,
            "fields": [],
            "partitionKeys": [],
            "primaryKeys": [],
            "timeMillis": 1705000000000
        }"#;

        let schema: PaimonSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.time_millis, Some(1705000000000));
    }

    #[test]
    fn test_parse_schema_with_multiple_options() {
        let json = r#"{
            "id": 0,
            "fields": [],
            "partitionKeys": [],
            "primaryKeys": [],
            "options": {
                "bucket": "16",
                "file.format": "parquet",
                "compaction.min.file-num": "5",
                "compaction.max.file-num": "50"
            }
        }"#;

        let schema: PaimonSchema = serde_json::from_str(json).unwrap();
        let options = schema.options.unwrap();
        assert_eq!(options.get("bucket"), Some(&"16".to_string()));
        assert_eq!(options.get("file.format"), Some(&"parquet".to_string()));
        assert_eq!(
            options.get("compaction.min.file-num"),
            Some(&"5".to_string())
        );
    }

    #[test]
    fn test_schema_default() {
        let schema = PaimonSchema::default();
        assert_eq!(schema.id, 0);
        assert!(schema.fields.is_empty());
        assert!(schema.highest_field_id.is_none());
        assert!(schema.partition_keys.is_empty());
        assert!(schema.primary_keys.is_empty());
        assert!(schema.options.is_none());
        assert!(schema.comment.is_none());
        assert!(schema.time_millis.is_none());
    }

    #[test]
    fn test_schema_serialization() {
        let schema = PaimonSchema {
            id: 1,
            fields: vec![PaimonField {
                id: 0,
                name: "id".to_string(),
                field_type: serde_json::json!("INT"),
                description: None,
            }],
            highest_field_id: Some(0),
            partition_keys: vec!["dt".to_string()],
            primary_keys: vec!["id".to_string()],
            options: Some(HashMap::from([("bucket".to_string(), "4".to_string())])),
            comment: Some("Test".to_string()),
            time_millis: Some(1705000000000),
        };

        let json = serde_json::to_string(&schema).unwrap();
        let parsed: PaimonSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, schema.id);
        assert_eq!(parsed.fields.len(), schema.fields.len());
        assert_eq!(parsed.partition_keys, schema.partition_keys);
    }

    // ========== Additional field parsing tests ==========

    #[test]
    fn test_parse_field_without_description() {
        let json = r#"{"id": 5, "name": "amount", "type": "DECIMAL(10,2)"}"#;
        let field: PaimonField = serde_json::from_str(json).unwrap();
        assert_eq!(field.id, 5);
        assert_eq!(field.name, "amount");
        assert!(field.description.is_none());
    }

    #[test]
    fn test_parse_field_with_complex_type() {
        let json = r#"{
            "id": 3,
            "name": "nested",
            "type": {
                "type": "ROW",
                "fields": [
                    {"id": 4, "name": "inner_id", "type": "INT"}
                ]
            }
        }"#;

        let field: PaimonField = serde_json::from_str(json).unwrap();
        assert_eq!(field.id, 3);
        assert_eq!(field.name, "nested");
        assert!(field.field_type.is_object());
    }

    #[test]
    fn test_field_default() {
        let field = PaimonField::default();
        assert_eq!(field.id, 0);
        assert!(field.name.is_empty());
        assert!(field.description.is_none());
    }

    // ========== Manifest list entry tests ==========

    #[test]
    fn test_manifest_list_entry_with_values() {
        // Note: ManifestListEntry uses custom field names with underscores
        let entry = ManifestListEntry {
            file_name: "manifest-abc-123.avro".to_string(),
            file_size: 4096,
            num_added_files: 10,
            num_deleted_files: 2,
            schema_id: 1,
        };

        assert_eq!(entry.file_name, "manifest-abc-123.avro");
        assert_eq!(entry.file_size, 4096);
        assert_eq!(entry.num_added_files, 10);
        assert_eq!(entry.num_deleted_files, 2);
        assert_eq!(entry.schema_id, 1);
    }

    // ========== Manifest entry tests ==========

    #[test]
    fn test_manifest_entry_add_kind() {
        let entry = ManifestEntry {
            kind: 0, // ADD
            bucket: 3,
            total_buckets: 8,
            file: DataFileMeta::default(),
        };

        assert_eq!(entry.kind, 0);
        assert_eq!(entry.bucket, 3);
        assert_eq!(entry.total_buckets, 8);
    }

    #[test]
    fn test_manifest_entry_delete_kind() {
        let entry = ManifestEntry {
            kind: 1, // DELETE
            bucket: 5,
            total_buckets: 16,
            file: DataFileMeta::default(),
        };

        assert_eq!(entry.kind, 1);
        assert_eq!(entry.bucket, 5);
        assert_eq!(entry.total_buckets, 16);
    }

    // ========== Data file meta tests ==========

    #[test]
    fn test_data_file_meta_with_values() {
        let meta = DataFileMeta {
            file_name: "data-abc-123.orc".to_string(),
            file_size: 1024 * 1024,
            row_count: 10000,
            min_sequence_number: Some(1),
            max_sequence_number: Some(100),
            schema_id: 2,
            level: 1,
            creation_time: Some(1705000000000),
            delete_row_count: Some(50),
        };

        assert_eq!(meta.file_name, "data-abc-123.orc");
        assert_eq!(meta.file_size, 1024 * 1024);
        assert_eq!(meta.row_count, 10000);
        assert_eq!(meta.min_sequence_number, Some(1));
        assert_eq!(meta.max_sequence_number, Some(100));
        assert_eq!(meta.schema_id, 2);
        assert_eq!(meta.level, 1);
        assert_eq!(meta.creation_time, Some(1705000000000));
        assert_eq!(meta.delete_row_count, Some(50));
    }

    #[test]
    fn test_data_file_meta_optional_fields() {
        let meta = DataFileMeta {
            file_name: "data.parquet".to_string(),
            file_size: 512,
            row_count: 100,
            min_sequence_number: None,
            max_sequence_number: None,
            schema_id: 0,
            level: 0,
            creation_time: None,
            delete_row_count: None,
        };

        assert!(meta.min_sequence_number.is_none());
        assert!(meta.max_sequence_number.is_none());
        assert!(meta.creation_time.is_none());
        assert!(meta.delete_row_count.is_none());
    }

    // ========== Clone trait tests ==========

    #[test]
    fn test_snapshot_clone() {
        let snapshot = PaimonSnapshot {
            version: Some(3),
            id: 5,
            schema_id: 1,
            base_manifest_list: Some("base-list".to_string()),
            delta_manifest_list: Some("delta-list".to_string()),
            changelog_manifest_list: None,
            commit_kind: Some("APPEND".to_string()),
            time_millis: Some(1705000000000),
            log_offsets: None,
            total_record_count: Some(1000),
            delta_record_count: Some(100),
            changelog_record_count: None,
            watermark: None,
        };

        let cloned = snapshot.clone();
        assert_eq!(cloned.id, snapshot.id);
        assert_eq!(cloned.version, snapshot.version);
        assert_eq!(cloned.base_manifest_list, snapshot.base_manifest_list);
    }

    #[test]
    fn test_schema_clone() {
        let schema = PaimonSchema {
            id: 2,
            fields: vec![PaimonField {
                id: 0,
                name: "test".to_string(),
                field_type: serde_json::json!("STRING"),
                description: None,
            }],
            highest_field_id: Some(0),
            partition_keys: vec!["dt".to_string()],
            primary_keys: vec!["id".to_string()],
            options: None,
            comment: None,
            time_millis: None,
        };

        let cloned = schema.clone();
        assert_eq!(cloned.id, schema.id);
        assert_eq!(cloned.fields.len(), schema.fields.len());
        assert_eq!(cloned.partition_keys, schema.partition_keys);
    }

    #[test]
    fn test_data_file_meta_clone() {
        let meta = DataFileMeta {
            file_name: "test.orc".to_string(),
            file_size: 1024,
            row_count: 100,
            min_sequence_number: Some(1),
            max_sequence_number: Some(10),
            schema_id: 0,
            level: 2,
            creation_time: Some(1705000000000),
            delete_row_count: Some(5),
        };

        let cloned = meta.clone();
        assert_eq!(cloned.file_name, meta.file_name);
        assert_eq!(cloned.file_size, meta.file_size);
        assert_eq!(cloned.level, meta.level);
    }

    // ========== Debug trait tests ==========

    #[test]
    fn test_snapshot_debug() {
        let snapshot = PaimonSnapshot {
            id: 1,
            schema_id: 0,
            ..Default::default()
        };
        let debug_str = format!("{:?}", snapshot);
        assert!(debug_str.contains("PaimonSnapshot"));
        assert!(debug_str.contains("id: 1"));
    }

    #[test]
    fn test_schema_debug() {
        let schema = PaimonSchema {
            id: 0,
            fields: vec![],
            partition_keys: vec!["dt".to_string()],
            primary_keys: vec![],
            ..Default::default()
        };
        let debug_str = format!("{:?}", schema);
        assert!(debug_str.contains("PaimonSchema"));
        assert!(debug_str.contains("partition_keys"));
    }

    #[test]
    fn test_field_debug() {
        let field = PaimonField {
            id: 0,
            name: "test_field".to_string(),
            field_type: serde_json::json!("INT"),
            description: Some("A test field".to_string()),
        };
        let debug_str = format!("{:?}", field);
        assert!(debug_str.contains("PaimonField"));
        assert!(debug_str.contains("test_field"));
    }

    #[test]
    fn test_manifest_list_entry_debug() {
        let entry = ManifestListEntry {
            file_name: "manifest.avro".to_string(),
            file_size: 1024,
            num_added_files: 5,
            num_deleted_files: 1,
            schema_id: 0,
        };
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("ManifestListEntry"));
        assert!(debug_str.contains("manifest.avro"));
    }

    #[test]
    fn test_manifest_entry_debug() {
        let entry = ManifestEntry {
            kind: 0,
            bucket: 1,
            total_buckets: 4,
            file: DataFileMeta::default(),
        };
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("ManifestEntry"));
        assert!(debug_str.contains("kind: 0"));
    }

    #[test]
    fn test_data_file_meta_debug() {
        let meta = DataFileMeta {
            file_name: "data.orc".to_string(),
            file_size: 2048,
            row_count: 500,
            ..Default::default()
        };
        let debug_str = format!("{:?}", meta);
        assert!(debug_str.contains("DataFileMeta"));
        assert!(debug_str.contains("data.orc"));
    }
}
