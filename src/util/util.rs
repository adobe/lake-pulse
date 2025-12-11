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

use crate::storage::FileMetadata;
use log::info;
use std::collections::LinkedList;
use std::future::Future;
use std::time::{Duration, SystemTime};

/// Check if content is NDJSON format (used by Delta logs)
pub fn is_ndjson(content: &str) -> bool {
    let non_empty_lines: Vec<&str> = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();

    // NDJSON typically has multiple lines starting with { or [
    non_empty_lines.len() > 1
        && non_empty_lines.iter().all(|line| {
            let trimmed = line.trim();
            trimmed.starts_with('{') || trimmed.starts_with('[')
        })
}

pub fn detect_table_type(objects: &Vec<FileMetadata>) -> String {
    // Check for Lance: look for _versions directory and .lance files
    // Lance tables have a _versions directory and data stored in .lance files
    let has_versions_dir = objects.iter().any(|f| f.path.contains("/_versions/"));

    let has_lance_files = objects.iter().any(|f| f.path.ends_with(".lance"));

    if has_versions_dir || has_lance_files {
        return "lance".to_string();
    }

    // Check for Delta Lake: look for _delta_log directory
    if objects
        .into_iter()
        .find(|f| f.path.contains("_delta_log"))
        .is_some()
    {
        return "delta".to_string();
    }

    // Check for Iceberg: look for metadata directory with .metadata.json files
    if objects
        .into_iter()
        .find(|f| f.path.contains("/metadata/") && f.path.ends_with(".metadata.json"))
        .is_some()
    {
        return "iceberg".to_string();
    }

    // Check for Hudi: look for .hoodie directory
    if objects
        .into_iter()
        .find(|f| f.path.contains("/.hoodie/"))
        .is_some()
    {
        return "hudi".to_string();
    }

    "unknown".to_string()
}

/// Wrapper function to measure duration of an async operation
pub async fn measure_dur_async<'a, F, Fut, T, E>(
    metric_name: &'a str,
    internal_metrics: &mut LinkedList<(&'a str, SystemTime, Duration)>,
    operation: F,
    trace_log_fn: Option<fn(&T) -> String>,
) -> Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let start = SystemTime::now();
    let result = operation().await;
    let dur = start.elapsed().unwrap_or_else(|_| Duration::from_millis(0));
    internal_metrics.push_back((metric_name, start, dur));
    let log_line = result
        .as_ref()
        .ok()
        .and_then(|r| trace_log_fn.map(|f| f(r)))
        .unwrap_or_default();
    info!("{} | {}, took={}", metric_name, log_line, dur.as_millis());
    result
}

/// Wrapper function to measure duration of a synchronous operation that returns Result
pub fn measure_dur_with_error<'a, F, T, E>(
    metric_name: &'a str,
    internal_metrics: &mut LinkedList<(&'a str, SystemTime, Duration)>,
    mut operation: F,
    trace_log_fn: Option<fn(&T) -> String>,
) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
{
    let start = SystemTime::now();
    let result = operation();
    let dur = start.elapsed().unwrap_or_else(|_| Duration::from_millis(0));
    internal_metrics.push_back((metric_name, start, dur));
    let log_line = result
        .as_ref()
        .ok()
        .and_then(|r| trace_log_fn.map(|f| f(r)))
        .unwrap_or_default();
    info!("{} | {}, took={}", metric_name, log_line, dur.as_millis());
    result
}

pub async fn measure_dur<'a, F, T>(
    metric_name: &'a str,
    internal_metrics: &mut LinkedList<(&'a str, SystemTime, Duration)>,
    mut operation: F,
    trace_log_fn: Option<fn(T) -> String>,
) -> T
where
    T: Clone,
    F: FnMut() -> T,
{
    let start = SystemTime::now();
    let result = operation();
    let dur = start.elapsed().unwrap_or_else(|_| Duration::from_millis(0));
    internal_metrics.push_back((metric_name, start, dur));
    let log_line = trace_log_fn.map(|f| f(result.clone())).unwrap_or_default();
    info!("{} | {}, took={}", metric_name, log_line, dur.as_millis());
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_ndjson_valid() {
        let content = r#"{"key": "value1"}
{"key": "value2"}
{"key": "value3"}"#;
        assert!(is_ndjson(content));
    }

    #[test]
    fn test_is_ndjson_with_arrays() {
        let content = r#"[1, 2, 3]
[4, 5, 6]
[7, 8, 9]"#;
        assert!(is_ndjson(content));
    }

    #[test]
    fn test_is_ndjson_mixed_objects_arrays() {
        let content = r#"{"key": "value"}
[1, 2, 3]
{"another": "object"}"#;
        assert!(is_ndjson(content));
    }

    #[test]
    fn test_is_ndjson_single_line() {
        let content = r#"{"key": "value"}"#;
        assert!(!is_ndjson(content)); // Single line is not NDJSON
    }

    #[test]
    fn test_is_ndjson_invalid_format() {
        let content = r#"not json
also not json"#;
        assert!(!is_ndjson(content));
    }

    #[test]
    fn test_is_ndjson_mixed_valid_invalid() {
        let content = r#"{"key": "value"}
not json
{"another": "value"}"#;
        assert!(!is_ndjson(content)); // All lines must be valid
    }

    #[test]
    fn test_is_ndjson_empty_lines() {
        let content = r#"{"key": "value1"}

{"key": "value2"}

{"key": "value3"}"#;
        assert!(is_ndjson(content)); // Empty lines are filtered out
    }

    #[test]
    fn test_is_ndjson_empty_string() {
        let content = "";
        assert!(!is_ndjson(content));
    }

    #[test]
    fn test_detect_table_type_delta() {
        let objects = vec![
            FileMetadata {
                path: "table/_delta_log/00000000000000000000.json".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "table/part-00000.parquet".to_string(),
                size: 2048,
                last_modified: None,
            },
        ];

        assert_eq!(detect_table_type(&objects), "delta");
    }

    #[test]
    fn test_detect_table_type_iceberg() {
        let objects = vec![
            FileMetadata {
                path: "table/metadata/v1.metadata.json".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "table/data/file.parquet".to_string(),
                size: 2048,
                last_modified: None,
            },
        ];

        assert_eq!(detect_table_type(&objects), "iceberg");
    }

    #[test]
    fn test_detect_table_type_hudi() {
        let objects = vec![
            FileMetadata {
                path: "table/.hoodie/hoodie.properties".to_string(),
                size: 512,
                last_modified: None,
            },
            FileMetadata {
                path: "table/data/file.parquet".to_string(),
                size: 2048,
                last_modified: None,
            },
        ];

        assert_eq!(detect_table_type(&objects), "hudi");
    }

    #[test]
    fn test_detect_table_type_lance_versions() {
        let objects = vec![
            FileMetadata {
                path: "table/_versions/1.manifest".to_string(),
                size: 1024,
                last_modified: None,
            },
            FileMetadata {
                path: "table/data.lance".to_string(),
                size: 2048,
                last_modified: None,
            },
        ];

        assert_eq!(detect_table_type(&objects), "lance");
    }

    #[test]
    fn test_detect_table_type_lance_files() {
        let objects = vec![
            FileMetadata {
                path: "table/data/file1.lance".to_string(),
                size: 2048,
                last_modified: None,
            },
            FileMetadata {
                path: "table/data/file2.lance".to_string(),
                size: 3072,
                last_modified: None,
            },
        ];

        assert_eq!(detect_table_type(&objects), "lance");
    }

    #[test]
    fn test_detect_table_type_unknown() {
        let objects = vec![
            FileMetadata {
                path: "table/data/file.parquet".to_string(),
                size: 2048,
                last_modified: None,
            },
            FileMetadata {
                path: "table/data/file2.parquet".to_string(),
                size: 3072,
                last_modified: None,
            },
        ];

        assert_eq!(detect_table_type(&objects), "unknown");
    }

    #[test]
    fn test_detect_table_type_empty() {
        let objects = vec![];
        assert_eq!(detect_table_type(&objects), "unknown");
    }

    #[tokio::test]
    async fn test_measure_dur_async_success() {
        let mut metrics = LinkedList::new();

        let result = measure_dur_async(
            "test_operation",
            &mut metrics,
            || async { Ok::<i32, String>(42) },
            None,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(metrics.len(), 1);

        let (name, _start, _duration) = metrics.front().unwrap();
        assert_eq!(*name, "test_operation");
    }

    #[tokio::test]
    async fn test_measure_dur_async_with_trace_fn() {
        let mut metrics = LinkedList::new();

        fn trace_fn(value: &i32) -> String {
            format!("result={}", value)
        }

        let result = measure_dur_async(
            "test_operation",
            &mut metrics,
            || async { Ok::<i32, String>(100) },
            Some(trace_fn),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
        assert_eq!(metrics.len(), 1);
    }

    #[tokio::test]
    async fn test_measure_dur_async_error() {
        let mut metrics = LinkedList::new();

        let result = measure_dur_async(
            "test_operation",
            &mut metrics,
            || async { Err::<i32, String>("error occurred".to_string()) },
            None,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "error occurred");
        // Metrics should still be recorded even on error
        assert_eq!(metrics.len(), 1);
    }

    #[test]
    fn test_measure_dur_with_error_success() {
        let mut metrics = LinkedList::new();

        let result = measure_dur_with_error(
            "test_operation",
            &mut metrics,
            || Ok::<i32, String>(42),
            None,
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(metrics.len(), 1);

        let (name, _start, _duration) = metrics.front().unwrap();
        assert_eq!(*name, "test_operation");
    }

    #[test]
    fn test_measure_dur_with_error_with_trace_fn() {
        let mut metrics = LinkedList::new();

        fn trace_fn(value: &String) -> String {
            format!("output={}", value)
        }

        let result = measure_dur_with_error(
            "test_operation",
            &mut metrics,
            || Ok::<String, String>("success".to_string()),
            Some(trace_fn),
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(metrics.len(), 1);
    }

    #[test]
    fn test_measure_dur_with_error_failure() {
        let mut metrics = LinkedList::new();

        let result = measure_dur_with_error(
            "test_operation",
            &mut metrics,
            || Err::<i32, String>("error occurred".to_string()),
            None,
        );

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "error occurred");
        // Metrics should still be recorded even on error
        assert_eq!(metrics.len(), 1);
    }

    #[tokio::test]
    async fn test_measure_dur_basic() {
        let mut metrics = LinkedList::new();

        let result = measure_dur("test_operation", &mut metrics, || 42, None).await;

        assert_eq!(result, 42);
        assert_eq!(metrics.len(), 1);

        let (name, _start, _duration) = metrics.front().unwrap();
        assert_eq!(*name, "test_operation");
    }

    #[tokio::test]
    async fn test_measure_dur_with_trace_fn() {
        let mut metrics = LinkedList::new();

        fn trace_fn(value: i32) -> String {
            format!("computed={}", value)
        }

        let result = measure_dur("test_operation", &mut metrics, || 100, Some(trace_fn)).await;

        assert_eq!(result, 100);
        assert_eq!(metrics.len(), 1);
    }

    #[tokio::test]
    async fn test_measure_dur_multiple_operations() {
        let mut metrics = LinkedList::new();

        let _result1 = measure_dur("operation1", &mut metrics, || 10, None).await;
        let _result2 = measure_dur("operation2", &mut metrics, || 20, None).await;
        let _result3 = measure_dur("operation3", &mut metrics, || 30, None).await;

        assert_eq!(metrics.len(), 3);

        let names: Vec<&str> = metrics.iter().map(|(name, _, _)| *name).collect();
        assert_eq!(names, vec!["operation1", "operation2", "operation3"]);
    }

    #[test]
    fn test_measure_dur_with_error_multiple_operations() {
        let mut metrics = LinkedList::new();

        let _result1 = measure_dur_with_error("op1", &mut metrics, || Ok::<i32, String>(1), None);
        let _result2 = measure_dur_with_error("op2", &mut metrics, || Ok::<i32, String>(2), None);
        let _result3 = measure_dur_with_error(
            "op3",
            &mut metrics,
            || Err::<i32, String>("err".to_string()),
            None,
        );

        assert_eq!(metrics.len(), 3);

        let names: Vec<&str> = metrics.iter().map(|(name, _, _)| *name).collect();
        assert_eq!(names, vec!["op1", "op2", "op3"]);
    }

    #[tokio::test]
    async fn test_measure_dur_async_multiple_operations() {
        let mut metrics = LinkedList::new();

        let _result1 = measure_dur_async(
            "async_op1",
            &mut metrics,
            || async { Ok::<i32, String>(1) },
            None,
        )
        .await;
        let _result2 = measure_dur_async(
            "async_op2",
            &mut metrics,
            || async { Ok::<i32, String>(2) },
            None,
        )
        .await;

        assert_eq!(metrics.len(), 2);

        let names: Vec<&str> = metrics.iter().map(|(name, _, _)| *name).collect();
        assert_eq!(names, vec!["async_op1", "async_op2"]);
    }

    #[tokio::test]
    async fn test_measure_dur_timing() {
        let mut metrics = LinkedList::new();

        let result = measure_dur(
            "sleep_operation",
            &mut metrics,
            || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                42
            },
            None,
        )
        .await;

        assert_eq!(result, 42);
        assert_eq!(metrics.len(), 1);

        let (_name, _start, duration) = metrics.front().unwrap();
        // Should have taken at least 10ms
        assert!(duration.as_millis() >= 5); // Allow some tolerance
    }
}
