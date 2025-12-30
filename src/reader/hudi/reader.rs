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
    FileStatistics, HudiMetrics, PartitionMetrics, TableMetadata, TimelineMetrics,
};
use chrono::NaiveDateTime;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tracing::{info, warn};
use url::Url;

/// Convert a Hudi timestamp string (YYYYMMDDHHmmssSSS) to Unix milliseconds.
fn hudi_timestamp_to_millis(timestamp_str: &str) -> u64 {
    if timestamp_str.len() < 14 {
        return 0;
    }
    let datetime_str = &timestamp_str[..14];
    let millis_str = if timestamp_str.len() >= 17 {
        &timestamp_str[14..17]
    } else {
        "000"
    };
    let millis: u64 = millis_str.parse().unwrap_or(0);
    match NaiveDateTime::parse_from_str(datetime_str, "%Y%m%d%H%M%S") {
        Ok(dt) => dt.and_utc().timestamp_millis() as u64 + millis,
        Err(_) => 0,
    }
}

/// Hudi table type.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HudiTableType {
    #[default]
    CopyOnWrite,
    MergeOnRead,
}

impl From<&str> for HudiTableType {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "COPY_ON_WRITE" => HudiTableType::CopyOnWrite,
            "MERGE_ON_READ" => HudiTableType::MergeOnRead,
            _ => HudiTableType::CopyOnWrite,
        }
    }
}

/// Parsed hoodie.properties configuration.
#[derive(Debug, Clone, Default)]
pub struct HoodieProperties {
    pub table_name: String,
    pub table_type: HudiTableType,
    pub table_version: u32,
    pub precombine_field: String,
    pub record_key_fields: Vec<String>,
    pub partition_fields: Vec<String>,
    pub base_file_format: String,
    pub timeline_layout_version: u32,
}

impl HoodieProperties {
    /// Parse hoodie.properties content.
    pub fn parse(content: &str) -> Self {
        let mut props = HoodieProperties::default();
        let mut properties: HashMap<String, String> = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                properties.insert(key.trim().to_string(), value.trim().to_string());
            }
        }

        props.table_name = properties
            .get("hoodie.table.name")
            .cloned()
            .unwrap_or_default();
        props.table_type = properties
            .get("hoodie.table.type")
            .map(|s| HudiTableType::from(s.as_str()))
            .unwrap_or_default();
        props.table_version = properties
            .get("hoodie.table.version")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        props.precombine_field = properties
            .get("hoodie.table.precombine.field")
            .cloned()
            .unwrap_or_default();
        props.record_key_fields = properties
            .get("hoodie.table.recordkey.fields")
            .map(|s| s.split(',').map(|f| f.trim().to_string()).collect())
            .unwrap_or_default();
        props.partition_fields = properties
            .get("hoodie.table.partition.fields")
            .map(|s| {
                s.split(',')
                    .map(|f| f.trim().to_string())
                    .filter(|f| !f.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        props.base_file_format = properties
            .get("hoodie.table.base.file.format")
            .cloned()
            .unwrap_or_else(|| "PARQUET".to_string());
        props.timeline_layout_version = properties
            .get("hoodie.timeline.layout.version")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        props
    }
}

/// Write statistics from a Hudi commit.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct HoodieWriteStat {
    pub file_id: Option<String>,
    pub path: Option<String>,
    pub prev_commit: Option<String>,
    pub num_writes: Option<i64>,
    pub num_deletes: Option<i64>,
    pub num_inserts: Option<i64>,
    pub num_update_writes: Option<i64>,
    pub total_write_bytes: Option<i64>,
    pub file_size_in_bytes: Option<i64>,
    pub total_log_blocks: Option<i64>,
    pub total_log_records: Option<i64>,
    pub total_log_files_size: Option<i64>,
}

/// Hudi commit metadata structure.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct HoodieCommitMetadata {
    pub partition_to_write_stats: Option<HashMap<String, Vec<HoodieWriteStat>>>,
    pub compacted: Option<bool>,
    pub extra_metadata: Option<HashMap<String, String>>,
    pub operation_type: Option<String>,
}

impl HoodieCommitMetadata {
    /// Get the schema from extra metadata if present.
    pub fn get_schema(&self) -> Option<serde_json::Value> {
        self.extra_metadata
            .as_ref()
            .and_then(|m| m.get("schema"))
            .and_then(|s| serde_json::from_str(s).ok())
    }

    /// Get all file paths referenced in this commit.
    pub fn get_file_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        if let Some(partition_stats) = &self.partition_to_write_stats {
            for stats in partition_stats.values() {
                for stat in stats {
                    if let Some(path) = &stat.path {
                        paths.push(path.clone());
                    }
                }
            }
        }
        paths
    }

    /// Get total records written.
    pub fn total_records_written(&self) -> i64 {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| {
                m.values()
                    .flat_map(|stats| stats.iter())
                    .map(|s| s.num_writes.unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Get total deletes.
    pub fn total_deletes(&self) -> i64 {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| {
                m.values()
                    .flat_map(|stats| stats.iter())
                    .map(|s| s.num_deletes.unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Get total bytes written.
    pub fn total_bytes_written(&self) -> i64 {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| {
                m.values()
                    .flat_map(|stats| stats.iter())
                    .map(|s| s.total_write_bytes.unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Get total log files size (for MoR tables).
    pub fn total_log_size(&self) -> i64 {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| {
                m.values()
                    .flat_map(|stats| stats.iter())
                    .map(|s| s.total_log_files_size.unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Get total log files count (for MoR tables).
    pub fn total_log_files(&self) -> i64 {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| {
                m.values()
                    .flat_map(|stats| stats.iter())
                    .filter(|s| s.total_log_files_size.unwrap_or(0) > 0)
                    .count() as i64
            })
            .unwrap_or(0)
    }

    /// Get partition paths.
    pub fn get_partition_paths(&self) -> Vec<String> {
        self.partition_to_write_stats
            .as_ref()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }
}

/// Hudi action type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HudiActionType {
    Commit,
    DeltaCommit,
    Clean,
    Compaction,
    Rollback,
    Savepoint,
    ReplaceCommit,
    Indexing,
    Unknown(String),
}

impl From<&str> for HudiActionType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "commit" => HudiActionType::Commit,
            "deltacommit" => HudiActionType::DeltaCommit,
            "clean" => HudiActionType::Clean,
            "compaction" => HudiActionType::Compaction,
            "rollback" => HudiActionType::Rollback,
            "savepoint" => HudiActionType::Savepoint,
            "replacecommit" => HudiActionType::ReplaceCommit,
            "indexing" => HudiActionType::Indexing,
            _ => HudiActionType::Unknown(s.to_string()),
        }
    }
}

/// Hudi action state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HudiActionState {
    Requested,
    Inflight,
    Completed,
}

/// Parsed timeline action.
#[derive(Debug, Clone)]
pub struct HudiTimelineAction {
    pub timestamp: String,
    pub action_type: HudiActionType,
    pub state: HudiActionState,
}

/// Parse a timeline filename into its components.
fn parse_timeline_filename(filename: &str) -> Option<HudiTimelineAction> {
    let parts: Vec<&str> = filename.split('.').collect();
    if parts.is_empty() {
        return None;
    }

    let timestamp = parts[0].to_string();
    if timestamp.len() < 14 {
        return None;
    }

    let (action_type, state) = if parts.len() >= 3 {
        let action = HudiActionType::from(parts[1]);
        let state = match parts[2] {
            "requested" => HudiActionState::Requested,
            "inflight" => HudiActionState::Inflight,
            _ => HudiActionState::Completed,
        };
        (action, state)
    } else if parts.len() == 2 {
        (HudiActionType::from(parts[1]), HudiActionState::Completed)
    } else {
        return None;
    };

    Some(HudiTimelineAction {
        timestamp,
        action_type,
        state,
    })
}

/// Hudi table reader for extracting metrics.
///
/// This reader parses Hudi table metadata from the `.hoodie/` directory
/// to extract comprehensive metrics about the table's health and structure.
pub struct HudiReader {
    store: Arc<dyn ObjectStore>,
    base_path: Path,
    properties: HoodieProperties,
}

impl HudiReader {
    /// Open a Hudi table from the given location.
    ///
    /// # Arguments
    ///
    /// * `location` - The URI of the Hudi table (e.g., "file:///path/to/table", "s3://bucket/path")
    /// * `storage_options` - Optional storage configuration options
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// * `Ok(HudiReader)` - A successfully opened Hudi table reader
    /// * `Err(Box<dyn Error + Send + Sync>)` - If the table cannot be opened
    pub async fn open(
        location: &str,
        storage_options: &HashMap<String, String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        info!("Opening Hudi table at location={}", location);

        let url = Url::parse(location)?;
        let (store, base_path) = object_store::parse_url_opts(&url, storage_options.iter())?;
        let store = Arc::new(store);

        // Read hoodie.properties
        let props_path = base_path.child(".hoodie").child("hoodie.properties");
        let props_content = match store.get(&props_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                String::from_utf8_lossy(&bytes).to_string()
            }
            Err(e) => {
                warn!("Failed to read hoodie.properties: {}", e);
                String::new()
            }
        };

        let properties = HoodieProperties::parse(&props_content);
        info!(
            "Opened Hudi table: name={}, type={:?}, version={}",
            properties.table_name, properties.table_type, properties.table_version
        );

        Ok(Self {
            store,
            base_path,
            properties,
        })
    }

    /// Extract comprehensive metrics from the Hudi table.
    pub async fn extract_metrics(&self) -> Result<HudiMetrics, Box<dyn Error + Send + Sync>> {
        info!("Extracting metrics from Hudi table");

        // List timeline files
        let hoodie_path = self.base_path.child(".hoodie");
        let timeline_files = self.list_timeline_files(&hoodie_path).await?;

        // Parse timeline and extract metrics
        let timeline_info = self.extract_timeline_metrics(&timeline_files).await?;
        let file_stats = self.extract_file_statistics(&timeline_files).await?;
        let partition_info = self.extract_partition_metrics(&timeline_files).await?;

        // Extract table metadata
        let metadata = self.extract_table_metadata(&timeline_files).await?;

        // Build table properties map
        let mut table_properties = HashMap::new();
        table_properties.insert(
            "hoodie.table.name".to_string(),
            self.properties.table_name.clone(),
        );
        table_properties.insert(
            "hoodie.table.type".to_string(),
            match self.properties.table_type {
                HudiTableType::CopyOnWrite => "COPY_ON_WRITE".to_string(),
                HudiTableType::MergeOnRead => "MERGE_ON_READ".to_string(),
            },
        );
        table_properties.insert(
            "hoodie.table.version".to_string(),
            self.properties.table_version.to_string(),
        );
        if !self.properties.precombine_field.is_empty() {
            table_properties.insert(
                "hoodie.table.precombine.field".to_string(),
                self.properties.precombine_field.clone(),
            );
        }
        if !self.properties.record_key_fields.is_empty() {
            table_properties.insert(
                "hoodie.table.recordkey.fields".to_string(),
                self.properties.record_key_fields.join(","),
            );
        }

        let table_type = match self.properties.table_type {
            HudiTableType::CopyOnWrite => "COPY_ON_WRITE".to_string(),
            HudiTableType::MergeOnRead => "MERGE_ON_READ".to_string(),
        };

        Ok(HudiMetrics {
            table_type,
            table_name: self.properties.table_name.clone(),
            metadata,
            table_properties,
            file_stats,
            partition_info,
            timeline_info,
        })
    }

    /// List timeline files in the .hoodie directory.
    async fn list_timeline_files(
        &self,
        hoodie_path: &Path,
    ) -> Result<Vec<(String, HudiTimelineAction)>, Box<dyn Error + Send + Sync>> {
        use futures::TryStreamExt;

        let mut timeline_files = Vec::new();
        let mut list_stream = self.store.list(Some(hoodie_path));

        while let Some(meta) = list_stream.try_next().await? {
            let filename = meta.location.filename().unwrap_or_default().to_string();

            // Skip non-timeline files
            if filename.starts_with("hoodie.") || filename.starts_with('.') {
                continue;
            }

            if let Some(action) = parse_timeline_filename(&filename) {
                if action.state == HudiActionState::Completed {
                    timeline_files.push((meta.location.to_string(), action));
                }
            }
        }

        // Sort by timestamp
        timeline_files.sort_by(|a, b| a.1.timestamp.cmp(&b.1.timestamp));

        Ok(timeline_files)
    }

    /// Extract timeline metrics from timeline files.
    async fn extract_timeline_metrics(
        &self,
        timeline_files: &[(String, HudiTimelineAction)],
    ) -> Result<TimelineMetrics, Box<dyn Error + Send + Sync>> {
        let mut total_commits = 0;
        let mut total_delta_commits = 0;
        let mut total_compactions = 0;
        let mut total_cleans = 0;
        let mut total_rollbacks = 0;
        let mut total_savepoints = 0;
        let mut pending_compactions = 0;

        let mut earliest_timestamp: Option<String> = None;
        let mut latest_timestamp: Option<String> = None;

        for (_, action) in timeline_files {
            match action.action_type {
                HudiActionType::Commit => total_commits += 1,
                HudiActionType::DeltaCommit => total_delta_commits += 1,
                HudiActionType::Compaction => {
                    if action.state == HudiActionState::Completed {
                        total_compactions += 1;
                    } else {
                        pending_compactions += 1;
                    }
                }
                HudiActionType::Clean => total_cleans += 1,
                HudiActionType::Rollback => total_rollbacks += 1,
                HudiActionType::Savepoint => total_savepoints += 1,
                _ => {}
            }

            if earliest_timestamp.is_none() {
                earliest_timestamp = Some(action.timestamp.clone());
            }
            latest_timestamp = Some(action.timestamp.clone());
        }

        Ok(TimelineMetrics {
            total_commits,
            total_delta_commits,
            total_compactions,
            total_cleans,
            total_rollbacks,
            total_savepoints,
            latest_commit_timestamp: latest_timestamp,
            earliest_commit_timestamp: earliest_timestamp,
            pending_compactions,
        })
    }

    /// Extract file statistics from commit metadata.
    async fn extract_file_statistics(
        &self,
        timeline_files: &[(String, HudiTimelineAction)],
    ) -> Result<FileStatistics, Box<dyn Error + Send + Sync>> {
        let mut total_size: u64 = 0;
        let mut total_files: usize = 0;
        let mut min_size: u64 = u64::MAX;
        let mut max_size: u64 = 0;
        let mut total_log_size: u64 = 0;
        let mut total_log_files: usize = 0;

        // Get the latest commit to extract current file statistics
        let commit_files: Vec<_> = timeline_files
            .iter()
            .filter(|(_, a)| {
                matches!(
                    a.action_type,
                    HudiActionType::Commit | HudiActionType::DeltaCommit
                )
            })
            .collect();

        if let Some((path, _)) = commit_files.last() {
            let obj_path = Path::from(path.as_str());
            if let Ok(result) = self.store.get(&obj_path).await {
                if let Ok(bytes) = result.bytes().await {
                    if let Ok(metadata) = serde_json::from_slice::<HoodieCommitMetadata>(&bytes) {
                        if let Some(partition_stats) = &metadata.partition_to_write_stats {
                            for stats in partition_stats.values() {
                                for stat in stats {
                                    if let Some(size) = stat.file_size_in_bytes {
                                        let size = size as u64;
                                        total_size += size;
                                        total_files += 1;
                                        min_size = min_size.min(size);
                                        max_size = max_size.max(size);
                                    }
                                    if let Some(log_size) = stat.total_log_files_size {
                                        if log_size > 0 {
                                            total_log_size += log_size as u64;
                                            total_log_files += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if min_size == u64::MAX {
            min_size = 0;
        }

        let avg_size = if total_files > 0 {
            total_size as f64 / total_files as f64
        } else {
            0.0
        };

        Ok(FileStatistics {
            num_files: total_files,
            total_size_bytes: total_size,
            avg_file_size_bytes: avg_size,
            min_file_size_bytes: min_size,
            max_file_size_bytes: max_size,
            num_log_files: total_log_files,
            total_log_size_bytes: total_log_size,
        })
    }

    /// Extract partition metrics from commit metadata.
    async fn extract_partition_metrics(
        &self,
        timeline_files: &[(String, HudiTimelineAction)],
    ) -> Result<PartitionMetrics, Box<dyn Error + Send + Sync>> {
        let mut partition_paths: Vec<String> = Vec::new();
        let mut partition_sizes: HashMap<String, u64> = HashMap::new();

        // Scan commits to find all partitions
        for (path, action) in timeline_files {
            if !matches!(
                action.action_type,
                HudiActionType::Commit | HudiActionType::DeltaCommit
            ) {
                continue;
            }

            let obj_path = Path::from(path.as_str());
            if let Ok(result) = self.store.get(&obj_path).await {
                if let Ok(bytes) = result.bytes().await {
                    if let Ok(metadata) = serde_json::from_slice::<HoodieCommitMetadata>(&bytes) {
                        for partition in metadata.get_partition_paths() {
                            if !partition_paths.contains(&partition) {
                                partition_paths.push(partition.clone());
                            }
                        }

                        if let Some(partition_stats) = &metadata.partition_to_write_stats {
                            for (partition, stats) in partition_stats {
                                let size: u64 = stats
                                    .iter()
                                    .map(|s| s.file_size_in_bytes.unwrap_or(0) as u64)
                                    .sum();
                                *partition_sizes.entry(partition.clone()).or_insert(0) += size;
                            }
                        }
                    }
                }
            }
        }

        let sizes: Vec<u64> = partition_sizes.values().copied().collect();
        let largest = sizes.iter().max().copied().unwrap_or(0);
        let smallest = sizes.iter().min().copied().unwrap_or(0);
        let avg = if !sizes.is_empty() {
            sizes.iter().sum::<u64>() as f64 / sizes.len() as f64
        } else {
            0.0
        };

        Ok(PartitionMetrics {
            num_partition_columns: self.properties.partition_fields.len(),
            num_partitions: partition_paths.len(),
            partition_paths,
            largest_partition_size_bytes: largest,
            smallest_partition_size_bytes: smallest,
            avg_partition_size_bytes: avg,
        })
    }

    /// Extract table metadata.
    async fn extract_table_metadata(
        &self,
        timeline_files: &[(String, HudiTimelineAction)],
    ) -> Result<TableMetadata, Box<dyn Error + Send + Sync>> {
        let mut schema_string = "{}".to_string();
        let mut field_count = 0;

        // Get schema from the latest commit
        let commit_files: Vec<_> = timeline_files
            .iter()
            .filter(|(_, a)| {
                matches!(
                    a.action_type,
                    HudiActionType::Commit | HudiActionType::DeltaCommit
                )
            })
            .collect();

        if let Some((path, _)) = commit_files.last() {
            let obj_path = Path::from(path.as_str());
            if let Ok(result) = self.store.get(&obj_path).await {
                if let Ok(bytes) = result.bytes().await {
                    if let Ok(metadata) = serde_json::from_slice::<HoodieCommitMetadata>(&bytes) {
                        if let Some(schema) = metadata.get_schema() {
                            schema_string = serde_json::to_string_pretty(&schema)?;
                            if let Some(fields) = schema.get("fields").and_then(|f| f.as_array()) {
                                field_count = fields.len();
                            }
                        }
                    }
                }
            }
        }

        // Get earliest commit timestamp as created time
        let created_time = timeline_files
            .first()
            .map(|(_, a)| hudi_timestamp_to_millis(&a.timestamp) as i64);

        Ok(TableMetadata {
            name: self.properties.table_name.clone(),
            base_path: self.base_path.to_string(),
            schema_string,
            field_count,
            partition_columns: self.properties.partition_fields.clone(),
            created_time,
            format_provider: self.properties.base_file_format.to_lowercase(),
            format_options: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_hudi_table_path() -> String {
        let current_dir = std::env::current_dir().unwrap();
        let test_path = current_dir.join("examples/data/hudi_dataset");
        format!("file://{}", test_path.to_str().unwrap())
    }

    #[test]
    fn test_hudi_timestamp_to_millis() {
        let ts = hudi_timestamp_to_millis("20251110222213846");
        assert!(ts > 0);
        // Nov 10, 2025 should be around 1762732800000 ms
        assert!(ts >= 1762732800000);
        assert!(ts <= 1762819199999);
    }

    #[test]
    fn test_hudi_timestamp_to_millis_short() {
        let ts = hudi_timestamp_to_millis("2025");
        assert_eq!(ts, 0);
    }

    #[test]
    fn test_hudi_table_type_from() {
        assert!(matches!(
            HudiTableType::from("COPY_ON_WRITE"),
            HudiTableType::CopyOnWrite
        ));
        assert!(matches!(
            HudiTableType::from("MERGE_ON_READ"),
            HudiTableType::MergeOnRead
        ));
        assert!(matches!(
            HudiTableType::from("unknown"),
            HudiTableType::CopyOnWrite
        ));
    }

    #[test]
    fn test_hoodie_properties_parse() {
        let content = r#"
hoodie.table.name=test_table
hoodie.table.type=COPY_ON_WRITE
hoodie.table.version=6
hoodie.table.recordkey.fields=id
hoodie.table.precombine.field=ts
hoodie.table.partition.fields=date
hoodie.table.base.file.format=PARQUET
"#;
        let props = HoodieProperties::parse(content);
        assert_eq!(props.table_name, "test_table");
        assert_eq!(props.table_type, HudiTableType::CopyOnWrite);
        assert_eq!(props.table_version, 6);
        assert_eq!(props.record_key_fields, vec!["id".to_string()]);
        assert_eq!(props.precombine_field, "ts");
        assert_eq!(props.partition_fields, vec!["date".to_string()]);
        assert_eq!(props.base_file_format, "PARQUET");
    }

    #[test]
    fn test_hudi_action_type_from() {
        assert!(matches!(
            HudiActionType::from("commit"),
            HudiActionType::Commit
        ));
        assert!(matches!(
            HudiActionType::from("deltacommit"),
            HudiActionType::DeltaCommit
        ));
        assert!(matches!(
            HudiActionType::from("clean"),
            HudiActionType::Clean
        ));
        assert!(matches!(
            HudiActionType::from("compaction"),
            HudiActionType::Compaction
        ));
        assert!(matches!(
            HudiActionType::from("rollback"),
            HudiActionType::Rollback
        ));
    }

    #[test]
    fn test_parse_timeline_filename() {
        let action = parse_timeline_filename("20251110222213846.commit").unwrap();
        assert_eq!(action.timestamp, "20251110222213846");
        assert!(matches!(action.action_type, HudiActionType::Commit));
        assert!(matches!(action.state, HudiActionState::Completed));

        let action = parse_timeline_filename("20251110222213846.deltacommit.inflight").unwrap();
        assert!(matches!(action.action_type, HudiActionType::DeltaCommit));
        assert!(matches!(action.state, HudiActionState::Inflight));

        assert!(parse_timeline_filename("invalid").is_none());
    }

    #[test]
    fn test_hoodie_commit_metadata_helpers() {
        let json = r#"{
            "partitionToWriteStats": {
                "": [{
                    "fileId": "file1",
                    "path": "file1.parquet",
                    "numWrites": 100,
                    "numDeletes": 5,
                    "totalWriteBytes": 1000,
                    "fileSizeInBytes": 900
                }]
            },
            "extraMetadata": {
                "schema": "{\"type\":\"record\",\"name\":\"test\"}"
            }
        }"#;

        let metadata: HoodieCommitMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.total_records_written(), 100);
        assert_eq!(metadata.total_deletes(), 5);
        assert_eq!(metadata.total_bytes_written(), 1000);
        assert_eq!(metadata.get_file_paths(), vec!["file1.parquet".to_string()]);
        assert!(metadata.get_schema().is_some());
    }

    #[tokio::test]
    async fn test_hudi_reader_open_success() {
        let location = get_test_hudi_table_path();
        let storage_options = HashMap::new();

        let result = HudiReader::open(&location, &storage_options).await;

        assert!(
            result.is_ok(),
            "Failed to open Hudi table: {:?}",
            result.err()
        );

        let reader = result.unwrap();
        assert_eq!(reader.properties.table_name, "hudi_test_table");
        assert_eq!(reader.properties.table_type, HudiTableType::CopyOnWrite);
    }

    #[tokio::test]
    async fn test_hudi_reader_open_invalid_location() {
        let location = "file:///nonexistent/path/to/hudi/table";
        let storage_options = HashMap::new();

        let result = HudiReader::open(location, &storage_options).await;

        // Should still succeed but with empty properties
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_hudi_reader_extract_metrics_success() {
        let location = get_test_hudi_table_path();
        let storage_options = HashMap::new();

        let reader = HudiReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Hudi table");

        let result = reader.extract_metrics().await;

        assert!(
            result.is_ok(),
            "Failed to extract metrics: {:?}",
            result.err()
        );

        let metrics = result.unwrap();
        assert_eq!(metrics.table_name, "hudi_test_table");
        assert_eq!(metrics.table_type, "COPY_ON_WRITE");
        assert!(metrics.timeline_info.total_commits > 0);
    }

    #[tokio::test]
    async fn test_hudi_reader_extract_timeline_metrics() {
        let location = get_test_hudi_table_path();
        let storage_options = HashMap::new();

        let reader = HudiReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Hudi table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        // The test table has 3 commits
        assert!(
            metrics.timeline_info.total_commits >= 3,
            "Expected at least 3 commits, got {}",
            metrics.timeline_info.total_commits
        );
        assert!(metrics.timeline_info.earliest_commit_timestamp.is_some());
        assert!(metrics.timeline_info.latest_commit_timestamp.is_some());
    }

    #[tokio::test]
    async fn test_hudi_reader_extract_table_metadata() {
        let location = get_test_hudi_table_path();
        let storage_options = HashMap::new();

        let reader = HudiReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Hudi table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        assert_eq!(metrics.metadata.name, "hudi_test_table");
        assert!(!metrics.metadata.schema_string.is_empty());
        assert!(metrics.metadata.field_count > 0);
        assert_eq!(metrics.metadata.format_provider, "parquet");
    }

    #[tokio::test]
    async fn test_hudi_reader_table_properties() {
        let location = get_test_hudi_table_path();
        let storage_options = HashMap::new();

        let reader = HudiReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Hudi table");

        let metrics = reader
            .extract_metrics()
            .await
            .expect("Failed to extract metrics");

        assert!(metrics.table_properties.contains_key("hoodie.table.name"));
        assert!(metrics.table_properties.contains_key("hoodie.table.type"));
        assert!(metrics
            .table_properties
            .contains_key("hoodie.table.version"));
    }

    #[tokio::test]
    async fn test_hudi_reader_multiple_extractions() {
        let location = get_test_hudi_table_path();
        let storage_options = HashMap::new();

        let reader = HudiReader::open(&location, &storage_options)
            .await
            .expect("Failed to open Hudi table");

        let metrics1 = reader
            .extract_metrics()
            .await
            .expect("First extraction failed");
        let metrics2 = reader
            .extract_metrics()
            .await
            .expect("Second extraction failed");

        assert_eq!(metrics1.table_name, metrics2.table_name);
        assert_eq!(metrics1.table_type, metrics2.table_type);
        assert_eq!(
            metrics1.timeline_info.total_commits,
            metrics2.timeline_info.total_commits
        );
    }
}
