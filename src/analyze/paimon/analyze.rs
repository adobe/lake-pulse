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

use crate::analyze::common::{
    calculate_recommended_retention, calculate_retention_efficiency,
    calculate_schema_stability_score,
};
use crate::analyze::metrics::{
    ClusteringInfo, DeletionVectorMetrics, FileCompactionMetrics, HealthMetrics,
    SchemaEvolutionMetrics, TableConstraintsMetrics, TimeTravelMetrics,
};
use crate::analyze::table_analyzer::TableAnalyzer;
use crate::storage::{FileMetadata, StorageProvider};
use apache_avro::{from_value, Reader as AvroReader};
use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tracing::{info, warn};

/// Paimon snapshot JSON structure (minimal for parsing)
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct PaimonSnapshot {
    pub id: i64,
    pub schema_id: i64,
    pub time_millis: Option<i64>,
    pub total_record_count: Option<i64>,
    pub commit_kind: Option<String>,
    pub base_manifest_list: Option<String>,
    pub delta_manifest_list: Option<String>,
}

/// Paimon schema JSON structure (minimal for parsing)
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct PaimonSchema {
    pub id: i64,
    pub fields: Vec<serde_json::Value>,
    pub partition_keys: Vec<String>,
    pub primary_keys: Vec<String>,
    pub options: Option<HashMap<String, String>>,
}

/// Manifest list entry from Avro manifest-list file (for analyzer)
/// Uses deny_unknown_fields = false to allow extra fields in Avro schema
#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
struct ManifestListEntry {
    #[serde(rename = "_FILE_NAME", default)]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE", default)]
    pub file_size: i64,
    #[serde(rename = "_NUM_ADDED_FILES", default)]
    pub num_added_files: i64,
    #[serde(rename = "_NUM_DELETED_FILES", default)]
    pub num_deleted_files: i64,
    // Additional fields that may be present in newer Paimon versions
    #[serde(rename = "_VERSION", default)]
    pub version: i32,
    #[serde(rename = "_SCHEMA_ID", default)]
    pub schema_id: i64,
}

/// Data file meta from manifest entry (for analyzer)
#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
struct DataFileMeta {
    #[serde(rename = "_FILE_NAME", default)]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE", default)]
    pub file_size: i64,
}

/// Manifest entry from Avro manifest file (for analyzer)
#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
struct ManifestEntry {
    #[serde(rename = "_KIND", default)]
    pub kind: i8, // 0 = ADD, 1 = DELETE
    #[serde(rename = "_FILE", default)]
    pub file: DataFileMeta,
}

/// Apache Paimon-specific analyzer for processing Paimon tables.
///
/// This analyzer implements the `TableAnalyzer` trait and provides functionality
/// to parse Paimon snapshot and schema files, extract metrics, and analyze table health.
pub struct PaimonAnalyzer {
    storage_provider: Arc<dyn StorageProvider>,
    #[allow(dead_code)]
    parallelism: usize,
}

impl PaimonAnalyzer {
    /// Create a new PaimonAnalyzer.
    pub fn new(storage_provider: Arc<dyn StorageProvider>, parallelism: usize) -> Self {
        Self {
            storage_provider,
            parallelism,
        }
    }

    /// Categorize files into data files and Paimon metadata files.
    pub fn categorize_paimon_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        let mut data_files = Vec::new();
        let mut metadata_files = Vec::new();

        for obj in objects {
            let path = &obj.path;
            // Paimon data files are in bucket directories
            if (path.ends_with(".orc") || path.ends_with(".parquet"))
                && !path.contains("/snapshot/")
                && !path.contains("/schema/")
                && !path.contains("/manifest/")
            {
                data_files.push(obj);
            }
            // Paimon metadata files are in snapshot/, schema/, manifest/ directories
            else if path.contains("/snapshot/")
                || path.contains("/schema/")
                || path.contains("/manifest/")
            {
                metadata_files.push(obj);
            }
        }

        (data_files, metadata_files)
    }

    /// Find referenced files from Paimon metadata.
    ///
    /// This method parses the Paimon manifest files to find all data files
    /// that are referenced by the current snapshot.
    pub async fn find_paimon_referenced_files(
        &self,
        metadata_files: &[FileMetadata],
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        // Find the latest snapshot file to get manifest list references
        let mut latest_snapshot: Option<PaimonSnapshot> = None;
        let mut latest_snapshot_id: i64 = -1;

        for file in metadata_files {
            let filename = file.path.rsplit('/').next().unwrap_or(&file.path);
            if filename.starts_with("snapshot-") {
                if let Ok(content) = self.storage_provider.read_file(&file.path).await {
                    if let Ok(snapshot) = serde_json::from_slice::<PaimonSnapshot>(&content) {
                        if snapshot.id > latest_snapshot_id {
                            latest_snapshot_id = snapshot.id;
                            latest_snapshot = Some(snapshot);
                        }
                    }
                }
            }
        }

        let snapshot = match latest_snapshot {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let mut referenced_files = Vec::new();

        // Parse base manifest list
        if let Some(ref base_manifest_list) = snapshot.base_manifest_list {
            if let Ok(files) = self
                .parse_manifest_list_for_files(metadata_files, base_manifest_list)
                .await
            {
                referenced_files.extend(files);
            }
        }

        // Parse delta manifest list
        if let Some(ref delta_manifest_list) = snapshot.delta_manifest_list {
            if let Ok(files) = self
                .parse_manifest_list_for_files(metadata_files, delta_manifest_list)
                .await
            {
                referenced_files.extend(files);
            }
        }

        info!(
            "Found {} referenced files from Paimon manifests",
            referenced_files.len()
        );
        Ok(referenced_files)
    }

    /// Parse a manifest-list file and extract all referenced data file names.
    async fn parse_manifest_list_for_files(
        &self,
        metadata_files: &[FileMetadata],
        manifest_list_name: &str,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        // Find the manifest-list file
        let manifest_list_path = metadata_files
            .iter()
            .find(|f| f.path.ends_with(manifest_list_name))
            .map(|f| &f.path);

        let manifest_list_path = match manifest_list_path {
            Some(p) => p,
            None => {
                warn!("Manifest-list file not found: {}", manifest_list_name);
                return Ok(Vec::new());
            }
        };

        let content = self.storage_provider.read_file(manifest_list_path).await?;

        let reader = match AvroReader::new(content.as_slice()) {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to create Avro reader for manifest-list: {}", e);
                return Ok(Vec::new());
            }
        };

        let mut manifest_names = Vec::new();
        for value in reader {
            match value {
                Ok(v) => {
                    if let Ok(entry) = from_value::<ManifestListEntry>(&v) {
                        manifest_names.push(entry.file_name);
                    }
                }
                Err(e) => {
                    warn!("Failed to read manifest-list entry: {}", e);
                }
            }
        }

        // Now parse each manifest to get data file names
        let mut data_files = Vec::new();
        for manifest_name in manifest_names {
            if let Ok(files) = self
                .parse_manifest_for_files(metadata_files, &manifest_name)
                .await
            {
                data_files.extend(files);
            }
        }

        Ok(data_files)
    }

    /// Parse a manifest file and extract data file names.
    async fn parse_manifest_for_files(
        &self,
        metadata_files: &[FileMetadata],
        manifest_name: &str,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        // Find the manifest file
        let manifest_path = metadata_files
            .iter()
            .find(|f| f.path.ends_with(manifest_name))
            .map(|f| &f.path);

        let manifest_path = match manifest_path {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        let content = self.storage_provider.read_file(manifest_path).await?;
        let reader = AvroReader::new(content.as_slice())?;

        let mut data_files = Vec::new();
        for value in reader {
            match value {
                Ok(v) => {
                    if let Ok(entry) = from_value::<ManifestEntry>(&v) {
                        // Only include ADD entries (kind == 0)
                        if entry.kind == 0 {
                            data_files.push(entry.file.file_name);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read manifest entry: {}", e);
                }
            }
        }

        Ok(data_files)
    }

    /// Update health metrics from Paimon metadata files.
    pub async fn update_metrics_from_paimon_metadata(
        &self,
        metadata_files: &[FileMetadata],
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Updating metrics from Paimon metadata");

        let result = self.process_paimon_metadata(metadata_files).await?;

        // Calculate time-based metrics
        let now = Utc::now().timestamp_millis() as u64;
        let oldest_age_days = if result.oldest_timestamp > 0 {
            (now - result.oldest_timestamp) as f64 / (1000.0 * 60.0 * 60.0 * 24.0)
        } else {
            0.0
        };
        let newest_age_days = if result.newest_timestamp > 0 {
            (now - result.newest_timestamp) as f64 / (1000.0 * 60.0 * 60.0 * 24.0)
        } else {
            0.0
        };

        // Schema evolution metrics
        let schema_stability_score = calculate_schema_stability_score(
            result.total_schema_versions,
            0, // No breaking change detection yet
            if oldest_age_days > 0.0 {
                result.total_schema_versions as f64 / oldest_age_days
            } else {
                0.0
            },
            oldest_age_days,
        );

        metrics.schema_evolution = Some(SchemaEvolutionMetrics {
            total_schema_changes: result.total_schema_versions,
            breaking_changes: 0,
            non_breaking_changes: result.total_schema_versions,
            schema_stability_score,
            days_since_last_change: oldest_age_days,
            schema_change_frequency: 0.0,
            current_schema_version: result.current_schema_id as u64,
        });

        // Time travel metrics
        let retention_efficiency = calculate_retention_efficiency(
            result.total_snapshots,
            oldest_age_days,
            newest_age_days,
        );
        let recommended_retention =
            calculate_recommended_retention(result.total_snapshots, oldest_age_days);

        metrics.time_travel_metrics = Some(TimeTravelMetrics {
            total_snapshots: result.total_snapshots,
            oldest_snapshot_age_days: oldest_age_days,
            newest_snapshot_age_days: newest_age_days,
            total_historical_size_bytes: 0,
            avg_snapshot_size_bytes: 0.0,
            storage_cost_impact_score: 0.0,
            retention_efficiency_score: retention_efficiency,
            recommended_retention_days: recommended_retention,
        });

        // File compaction metrics
        let compaction_opportunity = if data_files_total_files > 100 {
            0.5
        } else {
            0.0
        };
        let compaction_priority = if compaction_opportunity > 0.7 {
            "high"
        } else if compaction_opportunity > 0.3 {
            "medium"
        } else {
            "low"
        };

        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: compaction_opportunity,
            small_files_count: 0,
            small_files_size_bytes: 0,
            potential_compaction_files: 0,
            estimated_compaction_savings_bytes: 0,
            recommended_target_file_size_bytes: 128 * 1024 * 1024,
            compaction_priority: compaction_priority.to_string(),
            z_order_opportunity: false,
            z_order_columns: Vec::new(),
        });

        // Deletion vector metrics (Paimon doesn't use deletion vectors)
        metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
            deletion_vector_count: 0,
            total_deletion_vector_size_bytes: 0,
            avg_deletion_vector_size_bytes: 0.0,
            deletion_vector_age_days: 0.0,
            deleted_rows_count: 0,
            deletion_vector_impact_score: 0.0,
        });

        // Table constraints metrics
        metrics.table_constraints = Some(TableConstraintsMetrics {
            total_constraints: 0,
            check_constraints: 0,
            not_null_constraints: 0,
            unique_constraints: 0,
            foreign_key_constraints: 0,
            constraint_violation_risk: 0.0,
            data_quality_score: 1.0,
            constraint_coverage_score: 0.0,
        });

        // Clustering info
        let partition_count = metrics.partitions.len().max(1);
        let avg_files_per_cluster = if partition_count > 0 {
            data_files_total_files as f64 / partition_count as f64
        } else {
            0.0
        };
        let avg_cluster_size_bytes = if partition_count > 0 {
            data_files_total_size as f64 / partition_count as f64
        } else {
            0.0
        };

        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: result.partition_keys.clone(),
            cluster_count: partition_count,
            avg_files_per_cluster,
            avg_cluster_size_bytes,
        });

        info!(
            "Paimon metrics: snapshots={}, schemas={}, partitions={}",
            result.total_snapshots, result.total_schema_versions, partition_count
        );

        Ok(())
    }

    /// Process Paimon metadata files and aggregate results.
    async fn process_paimon_metadata(
        &self,
        metadata_files: &[FileMetadata],
    ) -> Result<PaimonMetadataResult, Box<dyn Error + Send + Sync>> {
        let mut result = PaimonMetadataResult {
            oldest_timestamp: u64::MAX,
            ..Default::default()
        };

        // Count snapshots and find timestamps
        for file in metadata_files {
            let filename = file.path.rsplit('/').next().unwrap_or(&file.path);

            if filename.starts_with("snapshot-") {
                result.total_snapshots += 1;

                // Try to read and parse snapshot
                if let Ok(content) = self.storage_provider.read_file(&file.path).await {
                    if let Ok(snapshot) = serde_json::from_slice::<PaimonSnapshot>(&content) {
                        if let Some(ts) = snapshot.time_millis {
                            let ts = ts as u64;
                            result.oldest_timestamp = result.oldest_timestamp.min(ts);
                            result.newest_timestamp = result.newest_timestamp.max(ts);
                        }
                        result.current_snapshot_id = result.current_snapshot_id.max(snapshot.id);
                        result.current_schema_id = snapshot.schema_id;
                    }
                }
            } else if filename.starts_with("schema-") {
                result.total_schema_versions += 1;

                // Try to read and parse schema for partition keys
                if result.partition_keys.is_empty() {
                    if let Ok(content) = self.storage_provider.read_file(&file.path).await {
                        if let Ok(schema) = serde_json::from_slice::<PaimonSchema>(&content) {
                            result.partition_keys = schema.partition_keys;
                            result.primary_keys = schema.primary_keys;
                        }
                    }
                }
            }
        }

        if result.oldest_timestamp == u64::MAX {
            result.oldest_timestamp = 0;
        }

        Ok(result)
    }
}

/// Intermediate structure to hold aggregated results from Paimon metadata processing.
#[derive(Debug, Default)]
struct PaimonMetadataResult {
    pub total_snapshots: usize,
    pub total_schema_versions: usize,
    pub current_snapshot_id: i64,
    pub current_schema_id: i64,
    pub oldest_timestamp: u64,
    pub newest_timestamp: u64,
    pub partition_keys: Vec<String>,
    pub primary_keys: Vec<String>,
}

#[async_trait]
impl TableAnalyzer for PaimonAnalyzer {
    fn categorize_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> (Vec<FileMetadata>, Vec<FileMetadata>) {
        self.categorize_paimon_files(objects)
    }

    async fn find_referenced_files(
        &self,
        metadata_files: &[FileMetadata],
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        self.find_paimon_referenced_files(metadata_files).await
    }

    async fn update_metrics_from_metadata(
        &self,
        metadata_files: &[FileMetadata],
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.update_metrics_from_paimon_metadata(
            metadata_files,
            data_files_total_size,
            data_files_total_files,
            metrics,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::error::StorageResult;
    use std::collections::HashMap;
    use std::sync::OnceLock;

    // Mock storage provider for testing
    struct MockStorageProvider {
        options: OnceLock<HashMap<String, String>>,
    }

    impl MockStorageProvider {
        fn new() -> Self {
            Self {
                options: OnceLock::new(),
            }
        }
    }

    #[async_trait]
    impl StorageProvider for MockStorageProvider {
        fn base_path(&self) -> &str {
            "/mock/paimon/table"
        }

        async fn validate_connection(&self, _path: &str) -> StorageResult<()> {
            Ok(())
        }

        async fn list_files(
            &self,
            _path: &str,
            _recursive: bool,
        ) -> StorageResult<Vec<FileMetadata>> {
            Ok(vec![])
        }

        async fn discover_partitions(
            &self,
            _path: &str,
            _exclude_prefixes: Vec<&str>,
        ) -> StorageResult<Vec<String>> {
            Ok(vec![])
        }

        async fn list_files_parallel(
            &self,
            _path: &str,
            _partitions: Vec<String>,
            _parallelism: usize,
        ) -> StorageResult<Vec<FileMetadata>> {
            Ok(vec![])
        }

        async fn read_file(&self, _path: &str) -> StorageResult<Vec<u8>> {
            Ok(vec![])
        }

        async fn exists(&self, _path: &str) -> StorageResult<bool> {
            Ok(true)
        }

        async fn get_metadata(&self, _path: &str) -> StorageResult<FileMetadata> {
            Ok(FileMetadata {
                path: "test".to_string(),
                size: 0,
                last_modified: None,
            })
        }

        fn options(&self) -> &HashMap<String, String> {
            self.options.get_or_init(HashMap::new)
        }

        fn clean_options(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn uri_from_path(&self, path: &str) -> String {
            path.to_string()
        }
    }

    // Configurable mock storage provider for more complex tests
    struct ConfigurableMockStorageProvider {
        base_path: String,
        options: OnceLock<HashMap<String, String>>,
        file_contents: HashMap<String, Vec<u8>>,
    }

    impl ConfigurableMockStorageProvider {
        fn new(base_path: &str) -> Self {
            Self {
                base_path: base_path.to_string(),
                options: OnceLock::new(),
                file_contents: HashMap::new(),
            }
        }

        fn with_file_content(mut self, path: &str, content: Vec<u8>) -> Self {
            self.file_contents.insert(path.to_string(), content);
            self
        }
    }

    #[async_trait]
    impl StorageProvider for ConfigurableMockStorageProvider {
        fn base_path(&self) -> &str {
            &self.base_path
        }

        async fn validate_connection(&self, _path: &str) -> StorageResult<()> {
            Ok(())
        }

        async fn list_files(
            &self,
            _path: &str,
            _recursive: bool,
        ) -> StorageResult<Vec<FileMetadata>> {
            Ok(vec![])
        }

        async fn discover_partitions(
            &self,
            _path: &str,
            _exclude_prefixes: Vec<&str>,
        ) -> StorageResult<Vec<String>> {
            Ok(vec![])
        }

        async fn list_files_parallel(
            &self,
            _path: &str,
            _partitions: Vec<String>,
            _parallelism: usize,
        ) -> StorageResult<Vec<FileMetadata>> {
            Ok(vec![])
        }

        async fn read_file(&self, path: &str) -> StorageResult<Vec<u8>> {
            // Extract filename from path
            let filename = path.rsplit('/').next().unwrap_or(path);
            if let Some(content) = self.file_contents.get(filename) {
                Ok(content.clone())
            } else if let Some(content) = self.file_contents.get(path) {
                Ok(content.clone())
            } else {
                Ok(vec![])
            }
        }

        async fn exists(&self, _path: &str) -> StorageResult<bool> {
            Ok(true)
        }

        async fn get_metadata(&self, _path: &str) -> StorageResult<FileMetadata> {
            Ok(FileMetadata {
                path: "test".to_string(),
                size: 0,
                last_modified: None,
            })
        }

        fn options(&self) -> &HashMap<String, String> {
            self.options.get_or_init(HashMap::new)
        }

        fn clean_options(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn uri_from_path(&self, path: &str) -> String {
            path.to_string()
        }
    }

    // Helper function to create a test PaimonAnalyzer
    fn create_test_analyzer() -> PaimonAnalyzer {
        let mock_storage = Arc::new(MockStorageProvider::new());
        PaimonAnalyzer::new(mock_storage, 4)
    }

    fn create_file_metadata(path: &str, size: u64) -> FileMetadata {
        FileMetadata {
            path: path.to_string(),
            size,
            last_modified: None,
        }
    }

    // ========== Tests for PaimonAnalyzer::new ==========

    #[test]
    fn test_paimon_analyzer_new() {
        let mock_storage = Arc::new(MockStorageProvider::new());
        let analyzer = PaimonAnalyzer::new(mock_storage, 8);
        assert_eq!(analyzer.parallelism, 8);
    }

    #[test]
    fn test_paimon_analyzer_new_default_parallelism() {
        let mock_storage = Arc::new(MockStorageProvider::new());
        let analyzer = PaimonAnalyzer::new(mock_storage, 1);
        assert_eq!(analyzer.parallelism, 1);
    }

    // ========== Tests for categorize_paimon_files ==========

    #[test]
    fn test_categorize_paimon_files_empty() {
        let analyzer = create_test_analyzer();
        let (data_files, metadata_files) = analyzer.categorize_paimon_files(vec![]);
        assert!(data_files.is_empty());
        assert!(metadata_files.is_empty());
    }

    #[test]
    fn test_categorize_paimon_files_data_only() {
        let analyzer = create_test_analyzer();
        let files = vec![
            create_file_metadata("bucket-0/data-file-1.orc", 1024),
            create_file_metadata("bucket-0/data-file-2.parquet", 2048),
            create_file_metadata("dt=2024-01-01/bucket-0/data.orc", 512),
        ];

        let (data_files, metadata_files) = analyzer.categorize_paimon_files(files);
        assert_eq!(data_files.len(), 3);
        assert!(metadata_files.is_empty());
    }

    #[test]
    fn test_categorize_paimon_files_metadata_only() {
        let analyzer = create_test_analyzer();
        let files = vec![
            create_file_metadata("/table/snapshot/snapshot-1", 100),
            create_file_metadata("/table/snapshot/snapshot-2", 100),
            create_file_metadata("/table/schema/schema-0", 200),
            create_file_metadata("/table/manifest/manifest-list-abc.avro", 300),
            create_file_metadata("/table/manifest/manifest-xyz.avro", 400),
        ];

        let (data_files, metadata_files) = analyzer.categorize_paimon_files(files);
        assert!(data_files.is_empty());
        assert_eq!(metadata_files.len(), 5);
    }

    #[test]
    fn test_categorize_paimon_files_mixed() {
        let analyzer = create_test_analyzer();
        let files = vec![
            // Data files
            create_file_metadata("bucket-0/data-file-1.orc", 1024),
            create_file_metadata("dt=2024-01-01/bucket-0/data.parquet", 2048),
            // Metadata files
            create_file_metadata("/table/snapshot/snapshot-1", 100),
            create_file_metadata("/table/schema/schema-0", 200),
            create_file_metadata("/table/manifest/manifest-list.avro", 300),
            // Files that should be ignored (not data or metadata)
            create_file_metadata("some/other/file.txt", 50),
        ];

        let (data_files, metadata_files) = analyzer.categorize_paimon_files(files);
        assert_eq!(data_files.len(), 2);
        assert_eq!(metadata_files.len(), 3);
    }

    #[test]
    fn test_categorize_paimon_files_excludes_metadata_orc() {
        let analyzer = create_test_analyzer();
        // ORC files in snapshot/schema/manifest directories should NOT be data files
        let files = vec![
            create_file_metadata("/table/snapshot/some.orc", 100),
            create_file_metadata("/table/schema/some.parquet", 200),
            create_file_metadata("/table/manifest/some.orc", 300),
        ];

        let (data_files, metadata_files) = analyzer.categorize_paimon_files(files);
        assert!(data_files.is_empty());
        assert_eq!(metadata_files.len(), 3);
    }

    // ========== Tests for find_paimon_referenced_files ==========

    #[tokio::test]
    async fn test_find_paimon_referenced_files_empty() {
        let analyzer = create_test_analyzer();
        let result = analyzer.find_paimon_referenced_files(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_paimon_referenced_files_returns_empty() {
        let analyzer = create_test_analyzer();
        let metadata_files = vec![
            create_file_metadata("/table/snapshot/snapshot-1", 100),
            create_file_metadata("/table/manifest/manifest-list.avro", 200),
        ];
        // Currently returns empty - would need manifest parsing
        let result = analyzer
            .find_paimon_referenced_files(&metadata_files)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    // ========== Tests for process_paimon_metadata ==========

    #[tokio::test]
    async fn test_process_paimon_metadata_empty() {
        let analyzer = create_test_analyzer();
        let result = analyzer.process_paimon_metadata(&[]).await.unwrap();
        assert_eq!(result.total_snapshots, 0);
        assert_eq!(result.total_schema_versions, 0);
        assert_eq!(result.oldest_timestamp, 0);
        assert_eq!(result.newest_timestamp, 0);
    }

    #[tokio::test]
    async fn test_process_paimon_metadata_with_snapshots() {
        let snapshot_json = r#"{
            "id": 5,
            "schemaId": 2,
            "timeMillis": 1705000000000,
            "totalRecordCount": 1000,
            "commitKind": "APPEND"
        }"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test")
                .with_file_content("snapshot-5", snapshot_json.as_bytes().to_vec()),
        );
        let analyzer = PaimonAnalyzer::new(mock_storage, 4);

        let metadata_files = vec![create_file_metadata("/table/snapshot/snapshot-5", 100)];

        let result = analyzer
            .process_paimon_metadata(&metadata_files)
            .await
            .unwrap();
        assert_eq!(result.total_snapshots, 1);
        assert_eq!(result.current_snapshot_id, 5);
        assert_eq!(result.current_schema_id, 2);
        assert_eq!(result.oldest_timestamp, 1705000000000);
        assert_eq!(result.newest_timestamp, 1705000000000);
    }

    #[tokio::test]
    async fn test_process_paimon_metadata_with_schema() {
        let schema_json = r#"{
            "id": 0,
            "fields": [{"id": 0, "name": "id", "type": "INT"}],
            "partitionKeys": ["dt", "region"],
            "primaryKeys": ["id"]
        }"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test")
                .with_file_content("schema-0", schema_json.as_bytes().to_vec()),
        );
        let analyzer = PaimonAnalyzer::new(mock_storage, 4);

        let metadata_files = vec![create_file_metadata("/table/schema/schema-0", 200)];

        let result = analyzer
            .process_paimon_metadata(&metadata_files)
            .await
            .unwrap();
        assert_eq!(result.total_schema_versions, 1);
        assert_eq!(result.partition_keys, vec!["dt", "region"]);
        assert_eq!(result.primary_keys, vec!["id"]);
    }

    #[tokio::test]
    async fn test_process_paimon_metadata_multiple_snapshots() {
        let snapshot1_json = r#"{"id": 1, "schemaId": 0, "timeMillis": 1700000000000}"#;
        let snapshot2_json = r#"{"id": 2, "schemaId": 0, "timeMillis": 1705000000000}"#;
        let snapshot3_json = r#"{"id": 3, "schemaId": 1, "timeMillis": 1710000000000}"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test")
                .with_file_content("snapshot-1", snapshot1_json.as_bytes().to_vec())
                .with_file_content("snapshot-2", snapshot2_json.as_bytes().to_vec())
                .with_file_content("snapshot-3", snapshot3_json.as_bytes().to_vec()),
        );
        let analyzer = PaimonAnalyzer::new(mock_storage, 4);

        let metadata_files = vec![
            create_file_metadata("/table/snapshot/snapshot-1", 100),
            create_file_metadata("/table/snapshot/snapshot-2", 100),
            create_file_metadata("/table/snapshot/snapshot-3", 100),
        ];

        let result = analyzer
            .process_paimon_metadata(&metadata_files)
            .await
            .unwrap();
        assert_eq!(result.total_snapshots, 3);
        assert_eq!(result.current_snapshot_id, 3);
        assert_eq!(result.current_schema_id, 1);
        assert_eq!(result.oldest_timestamp, 1700000000000);
        assert_eq!(result.newest_timestamp, 1710000000000);
    }

    // ========== Tests for update_metrics_from_paimon_metadata ==========

    #[tokio::test]
    async fn test_update_metrics_from_paimon_metadata_empty() {
        let analyzer = create_test_analyzer();
        let mut metrics = HealthMetrics::default();

        analyzer
            .update_metrics_from_paimon_metadata(&[], 0, 0, &mut metrics)
            .await
            .unwrap();

        // Should have schema evolution metrics
        assert!(metrics.schema_evolution.is_some());
        let schema_evo = metrics.schema_evolution.unwrap();
        assert_eq!(schema_evo.total_schema_changes, 0);

        // Should have time travel metrics
        assert!(metrics.time_travel_metrics.is_some());
        let time_travel = metrics.time_travel_metrics.unwrap();
        assert_eq!(time_travel.total_snapshots, 0);

        // Should have file compaction metrics
        assert!(metrics.file_compaction.is_some());

        // Should have deletion vector metrics (empty for Paimon)
        assert!(metrics.deletion_vector_metrics.is_some());
        let dv = metrics.deletion_vector_metrics.unwrap();
        assert_eq!(dv.deletion_vector_count, 0);

        // Should have table constraints metrics
        assert!(metrics.table_constraints.is_some());

        // Should have clustering info
        assert!(metrics.clustering.is_some());
    }

    #[tokio::test]
    async fn test_update_metrics_from_paimon_metadata_with_data() {
        let snapshot_json = r#"{
            "id": 5,
            "schemaId": 2,
            "timeMillis": 1705000000000,
            "totalRecordCount": 1000
        }"#;
        let schema_json = r#"{
            "id": 2,
            "fields": [],
            "partitionKeys": ["dt"],
            "primaryKeys": ["id"]
        }"#;

        let mock_storage = Arc::new(
            ConfigurableMockStorageProvider::new("/test")
                .with_file_content("snapshot-5", snapshot_json.as_bytes().to_vec())
                .with_file_content("schema-2", schema_json.as_bytes().to_vec()),
        );
        let analyzer = PaimonAnalyzer::new(mock_storage, 4);

        let metadata_files = vec![
            create_file_metadata("/table/snapshot/snapshot-5", 100),
            create_file_metadata("/table/schema/schema-2", 200),
        ];

        let mut metrics = HealthMetrics::default();
        analyzer
            .update_metrics_from_paimon_metadata(&metadata_files, 1024 * 1024, 10, &mut metrics)
            .await
            .unwrap();

        // Check schema evolution
        let schema_evo = metrics.schema_evolution.unwrap();
        assert_eq!(schema_evo.total_schema_changes, 1);
        assert_eq!(schema_evo.current_schema_version, 2);

        // Check time travel
        let time_travel = metrics.time_travel_metrics.unwrap();
        assert_eq!(time_travel.total_snapshots, 1);

        // Check clustering
        let clustering = metrics.clustering.unwrap();
        assert_eq!(clustering.clustering_columns, vec!["dt"]);
    }

    #[tokio::test]
    async fn test_update_metrics_compaction_priority_high() {
        let analyzer = create_test_analyzer();
        let mut metrics = HealthMetrics::default();

        // More than 100 files should trigger compaction opportunity
        analyzer
            .update_metrics_from_paimon_metadata(&[], 1024 * 1024 * 1024, 150, &mut metrics)
            .await
            .unwrap();

        let compaction = metrics.file_compaction.unwrap();
        assert_eq!(compaction.compaction_opportunity_score, 0.5);
        assert_eq!(compaction.compaction_priority, "medium");
    }

    #[tokio::test]
    async fn test_update_metrics_compaction_priority_low() {
        let analyzer = create_test_analyzer();
        let mut metrics = HealthMetrics::default();

        // Less than 100 files should have low compaction opportunity
        analyzer
            .update_metrics_from_paimon_metadata(&[], 1024 * 1024, 50, &mut metrics)
            .await
            .unwrap();

        let compaction = metrics.file_compaction.unwrap();
        assert_eq!(compaction.compaction_opportunity_score, 0.0);
        assert_eq!(compaction.compaction_priority, "low");
    }

    // ========== Tests for TableAnalyzer trait implementation ==========

    #[test]
    fn test_categorize_files_trait() {
        let analyzer = create_test_analyzer();
        let files = vec![
            create_file_metadata("bucket-0/data.orc", 1024),
            create_file_metadata("/table/snapshot/snapshot-1", 100),
        ];

        let (data_files, metadata_files) = analyzer.categorize_files(files);
        assert_eq!(data_files.len(), 1);
        assert_eq!(metadata_files.len(), 1);
    }

    #[tokio::test]
    async fn test_find_referenced_files_trait() {
        let analyzer = create_test_analyzer();
        let result = analyzer.find_referenced_files(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_update_metrics_from_metadata_trait() {
        let analyzer = create_test_analyzer();
        let mut metrics = HealthMetrics::default();

        analyzer
            .update_metrics_from_metadata(&[], 0, 0, &mut metrics)
            .await
            .unwrap();

        assert!(metrics.schema_evolution.is_some());
        assert!(metrics.time_travel_metrics.is_some());
    }

    // ========== Tests for PaimonSnapshot parsing ==========

    #[test]
    fn test_paimon_snapshot_parse() {
        let json = r#"{
            "id": 10,
            "schemaId": 3,
            "timeMillis": 1705000000000,
            "totalRecordCount": 5000,
            "commitKind": "COMPACT"
        }"#;

        let snapshot: PaimonSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.id, 10);
        assert_eq!(snapshot.schema_id, 3);
        assert_eq!(snapshot.time_millis, Some(1705000000000));
        assert_eq!(snapshot.total_record_count, Some(5000));
        assert_eq!(snapshot.commit_kind, Some("COMPACT".to_string()));
    }

    #[test]
    fn test_paimon_snapshot_minimal() {
        let json = r#"{"id": 1, "schemaId": 0}"#;
        let snapshot: PaimonSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.id, 1);
        assert_eq!(snapshot.schema_id, 0);
        assert!(snapshot.time_millis.is_none());
        assert!(snapshot.total_record_count.is_none());
    }

    #[test]
    fn test_paimon_snapshot_default() {
        let snapshot = PaimonSnapshot::default();
        assert_eq!(snapshot.id, 0);
        assert_eq!(snapshot.schema_id, 0);
        assert!(snapshot.time_millis.is_none());
    }

    // ========== Tests for PaimonSchema parsing ==========

    #[test]
    fn test_paimon_schema_parse() {
        let json = r#"{
            "id": 2,
            "fields": [{"id": 0, "name": "id", "type": "INT"}],
            "partitionKeys": ["dt", "hour"],
            "primaryKeys": ["id", "dt"],
            "options": {"bucket": "8"}
        }"#;

        let schema: PaimonSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.id, 2);
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.partition_keys, vec!["dt", "hour"]);
        assert_eq!(schema.primary_keys, vec!["id", "dt"]);
        assert!(schema.options.is_some());
    }

    #[test]
    fn test_paimon_schema_minimal() {
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
        assert!(schema.options.is_none());
    }

    #[test]
    fn test_paimon_schema_default() {
        let schema = PaimonSchema::default();
        assert_eq!(schema.id, 0);
        assert!(schema.fields.is_empty());
        assert!(schema.partition_keys.is_empty());
        assert!(schema.primary_keys.is_empty());
    }

    // ========== Tests for PaimonMetadataResult ==========

    #[test]
    fn test_paimon_metadata_result_default() {
        let result = PaimonMetadataResult::default();
        assert_eq!(result.total_snapshots, 0);
        assert_eq!(result.total_schema_versions, 0);
        assert_eq!(result.current_snapshot_id, 0);
        assert_eq!(result.current_schema_id, 0);
        assert_eq!(result.oldest_timestamp, 0);
        assert_eq!(result.newest_timestamp, 0);
        assert!(result.partition_keys.is_empty());
        assert!(result.primary_keys.is_empty());
    }
}
