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

//! Common constants used across table format analyzers.
//!
//! This module defines file size thresholds, scoring parameters, and other
//! constants that are shared between Delta, Iceberg, Hudi, and Lance analyzers.

/// Files smaller than this are considered "small" and candidates for compaction.
/// Default: 16 MB
pub const SMALL_FILE_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;

/// Target file size for compaction operations.
/// Default: 128 MB
pub const TARGET_COMPACTION_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// Medium file threshold for file size distribution.
/// Files >= SMALL_FILE_THRESHOLD and < MEDIUM_FILE_THRESHOLD are "medium".
/// Default: 128 MB
pub const MEDIUM_FILE_THRESHOLD_BYTES: u64 = 128 * 1024 * 1024;

/// Large file threshold for file size distribution.
/// Files >= MEDIUM_FILE_THRESHOLD and < LARGE_FILE_THRESHOLD are "large".
/// Default: 1 GB
pub const LARGE_FILE_THRESHOLD_BYTES: u64 = 1024 * 1024 * 1024;

// Compaction target size recommendations based on average file size

/// Target size for compaction when average file size is small (< 16 MB).
/// Default: 128 MB
pub const COMPACTION_TARGET_SMALL_AVG: u64 = 128 * 1024 * 1024;

/// Target size for compaction when average file size is medium (16-64 MB).
/// Default: 256 MB
pub const COMPACTION_TARGET_MEDIUM_AVG: u64 = 256 * 1024 * 1024;

/// Target size for compaction when average file size is large (>= 64 MB).
/// Default: 512 MB
pub const COMPACTION_TARGET_LARGE_AVG: u64 = 512 * 1024 * 1024;

/// Threshold for "medium" average file size (used in target size calculation).
/// Default: 64 MB
pub const AVG_FILE_SIZE_MEDIUM_THRESHOLD: f64 = 64.0 * 1024.0 * 1024.0;

/// Seconds per day for time calculations.
pub const SECONDS_PER_DAY: f64 = 86400.0;

/// Default stability score when no schema changes have occurred.
pub const DEFAULT_STABILITY_SCORE: f64 = 365.0;

// Schema stability scoring thresholds

/// High number of total schema changes threshold.
pub const SCHEMA_CHANGES_HIGH_THRESHOLD: usize = 50;

/// Medium number of total schema changes threshold.
pub const SCHEMA_CHANGES_MEDIUM_THRESHOLD: usize = 20;

/// Low number of total schema changes threshold.
pub const SCHEMA_CHANGES_LOW_THRESHOLD: usize = 10;

/// High number of breaking changes threshold.
pub const BREAKING_CHANGES_HIGH_THRESHOLD: usize = 10;

/// Medium number of breaking changes threshold.
pub const BREAKING_CHANGES_MEDIUM_THRESHOLD: usize = 5;

/// High frequency of schema changes (changes per day).
pub const SCHEMA_CHANGE_FREQUENCY_HIGH: f64 = 1.0;

/// Medium frequency of schema changes (changes per day).
pub const SCHEMA_CHANGE_FREQUENCY_MEDIUM: f64 = 0.5;

/// Low frequency of schema changes (changes per day).
pub const SCHEMA_CHANGE_FREQUENCY_LOW: f64 = 0.1;

/// Days since last change to consider schema very stable.
pub const DAYS_SINCE_CHANGE_VERY_STABLE: f64 = 30.0;

/// Days since last change to consider schema stable.
pub const DAYS_SINCE_CHANGE_STABLE: f64 = 7.0;

// Storage cost impact thresholds

/// High storage size threshold in GB.
pub const STORAGE_SIZE_HIGH_GB: f64 = 100.0;

/// Medium storage size threshold in GB.
pub const STORAGE_SIZE_MEDIUM_GB: f64 = 50.0;

/// Low storage size threshold in GB.
pub const STORAGE_SIZE_LOW_GB: f64 = 10.0;

/// High snapshot count threshold.
pub const SNAPSHOT_COUNT_HIGH: usize = 100;

/// Medium snapshot count threshold.
pub const SNAPSHOT_COUNT_MEDIUM: usize = 50;

/// Low snapshot count threshold.
pub const SNAPSHOT_COUNT_LOW: usize = 20;

/// Old snapshot age threshold in days.
pub const SNAPSHOT_AGE_OLD_DAYS: f64 = 90.0;

/// Medium snapshot age threshold in days.
pub const SNAPSHOT_AGE_MEDIUM_DAYS: f64 = 30.0;

// Retention efficiency thresholds

/// Very high snapshot count threshold for retention efficiency.
pub const RETENTION_SNAPSHOT_VERY_HIGH: usize = 1000;

/// High snapshot count threshold for retention efficiency.
pub const RETENTION_SNAPSHOT_HIGH: usize = 500;

/// Medium snapshot count threshold for retention efficiency.
pub const RETENTION_SNAPSHOT_MEDIUM: usize = 100;

/// Low snapshot count threshold for retention efficiency.
pub const RETENTION_SNAPSHOT_LOW: usize = 50;

/// Penalty for very high snapshot count.
pub const RETENTION_PENALTY_VERY_HIGH: f64 = 0.4;

/// Penalty for high snapshot count.
pub const RETENTION_PENALTY_HIGH: f64 = 0.3;

/// Penalty for medium snapshot count.
pub const RETENTION_PENALTY_MEDIUM: f64 = 0.2;

/// Penalty for low snapshot count.
pub const RETENTION_PENALTY_LOW: f64 = 0.1;

/// Maximum retention period in days before penalty.
pub const RETENTION_MAX_DAYS: f64 = 365.0;

/// Minimum retention period in days before penalty.
pub const RETENTION_MIN_DAYS: f64 = 7.0;

/// Penalty for too long retention.
pub const RETENTION_TOO_LONG_PENALTY: f64 = 0.2;

/// Penalty for too short retention.
pub const RETENTION_TOO_SHORT_PENALTY: f64 = 0.1;

// Recommended retention thresholds

/// Very old data threshold in days.
pub const RECOMMENDED_RETENTION_VERY_OLD_DAYS: f64 = 365.0;

/// Old data threshold in days.
pub const RECOMMENDED_RETENTION_OLD_DAYS: f64 = 90.0;

/// Recent data threshold in days.
pub const RECOMMENDED_RETENTION_RECENT_DAYS: f64 = 30.0;

/// Recommended retention for high snapshot count or very old data.
pub const RECOMMENDED_RETENTION_SHORT: u64 = 30;

/// Recommended retention for medium snapshot count or old data.
pub const RECOMMENDED_RETENTION_MEDIUM: u64 = 60;

/// Recommended retention for moderate snapshot count or recent data.
pub const RECOMMENDED_RETENTION_LONG: u64 = 90;

/// Recommended retention for low snapshot count and recent data.
pub const RECOMMENDED_RETENTION_VERY_LONG: u64 = 180;

// Compile-time assertions to verify constant ordering
const _: () = assert!(SMALL_FILE_THRESHOLD_BYTES < MEDIUM_FILE_THRESHOLD_BYTES);
const _: () = assert!(MEDIUM_FILE_THRESHOLD_BYTES < LARGE_FILE_THRESHOLD_BYTES);
const _: () = assert!(SCHEMA_CHANGES_LOW_THRESHOLD < SCHEMA_CHANGES_MEDIUM_THRESHOLD);
const _: () = assert!(SCHEMA_CHANGES_MEDIUM_THRESHOLD < SCHEMA_CHANGES_HIGH_THRESHOLD);
const _: () = assert!(COMPACTION_TARGET_SMALL_AVG < COMPACTION_TARGET_MEDIUM_AVG);
const _: () = assert!(COMPACTION_TARGET_MEDIUM_AVG < COMPACTION_TARGET_LARGE_AVG);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_size_thresholds_ordering() {
        // Verify thresholds have expected values (compile-time assertions ensure ordering)
        assert_eq!(SMALL_FILE_THRESHOLD_BYTES, 16 * 1024 * 1024); // 16 MB
        assert_eq!(MEDIUM_FILE_THRESHOLD_BYTES, 128 * 1024 * 1024); // 128 MB
        assert_eq!(LARGE_FILE_THRESHOLD_BYTES, 1024 * 1024 * 1024); // 1 GB
    }

    #[test]
    fn test_compaction_target_thresholds() {
        // Verify compaction target values
        assert_eq!(COMPACTION_TARGET_SMALL_AVG, 128 * 1024 * 1024); // 128 MB
        assert_eq!(COMPACTION_TARGET_MEDIUM_AVG, 256 * 1024 * 1024); // 256 MB
        assert_eq!(COMPACTION_TARGET_LARGE_AVG, 512 * 1024 * 1024); // 512 MB
        assert!((AVG_FILE_SIZE_MEDIUM_THRESHOLD - 64.0 * 1024.0 * 1024.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_schema_change_thresholds_ordering() {
        // Verify thresholds have expected values (compile-time assertions ensure ordering)
        assert_eq!(SCHEMA_CHANGES_LOW_THRESHOLD, 10);
        assert_eq!(SCHEMA_CHANGES_MEDIUM_THRESHOLD, 20);
        assert_eq!(SCHEMA_CHANGES_HIGH_THRESHOLD, 50);
    }

    #[test]
    fn test_frequency_thresholds_ordering() {
        // Verify frequency thresholds have expected values (compile-time assertions ensure ordering)
        assert!((SCHEMA_CHANGE_FREQUENCY_LOW - 0.1).abs() < f64::EPSILON);
        assert!((SCHEMA_CHANGE_FREQUENCY_MEDIUM - 0.5).abs() < f64::EPSILON);
        assert!((SCHEMA_CHANGE_FREQUENCY_HIGH - 1.0).abs() < f64::EPSILON);
    }
}
