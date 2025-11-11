use crate::storage::FileMetadata;
use log::info;
use std::future::Future;
use std::collections::LinkedList;
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
