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

//! Scoring utilities for table health analysis.
//!
//! This module provides common scoring functions used across different
//! table format analyzers for calculating schema stability, storage cost
//! impact, and other health metrics.

use super::constants::*;

/// Calculate schema stability score based on change patterns.
///
/// The score ranges from 0.0 (unstable) to 1.0 (highly stable).
/// Factors considered:
/// - Total number of schema changes (more changes = lower score)
/// - Number of breaking changes (heavily penalized)
/// - Frequency of changes (high frequency = lower score)
/// - Days since last change (longer = bonus points)
///
/// # Arguments
///
/// * `total_changes` - Total number of schema changes
/// * `breaking_changes` - Number of breaking schema changes
/// * `frequency` - Average changes per day
/// * `days_since_last` - Days since the last schema change
///
/// # Returns
///
/// A score between 0.0 and 1.0 where 1.0 is highly stable.
pub fn calculate_schema_stability_score(
    total_changes: usize,
    breaking_changes: usize,
    frequency: f64,
    days_since_last: f64,
) -> f64 {
    let mut score: f64 = 1.0;

    // Penalize total changes
    if total_changes > SCHEMA_CHANGES_HIGH_THRESHOLD {
        score -= 0.3;
    } else if total_changes > SCHEMA_CHANGES_MEDIUM_THRESHOLD {
        score -= 0.2;
    } else if total_changes > SCHEMA_CHANGES_LOW_THRESHOLD {
        score -= 0.1;
    }

    // Penalize breaking changes heavily
    if breaking_changes > BREAKING_CHANGES_HIGH_THRESHOLD {
        score -= 0.4;
    } else if breaking_changes > BREAKING_CHANGES_MEDIUM_THRESHOLD {
        score -= 0.3;
    } else if breaking_changes > 0 {
        score -= 0.2;
    }

    // Penalize high frequency changes
    if frequency > SCHEMA_CHANGE_FREQUENCY_HIGH {
        // More than 1 change per day
        score -= 0.3;
    } else if frequency > SCHEMA_CHANGE_FREQUENCY_MEDIUM {
        // More than 1 change every 2 days
        score -= 0.2;
    } else if frequency > SCHEMA_CHANGE_FREQUENCY_LOW {
        // More than 1 change every 10 days
        score -= 0.1;
    }

    // Reward stability (no recent changes)
    if days_since_last > DAYS_SINCE_CHANGE_VERY_STABLE {
        score += 0.1;
    } else if days_since_last > DAYS_SINCE_CHANGE_STABLE {
        score += 0.05;
    }

    score.clamp(0.0_f64, 1.0_f64)
}

/// Calculate storage cost impact score.
///
/// The score ranges from 0.0 (low impact) to 1.0 (high impact).
/// Factors considered:
/// - Total storage size
/// - Number of snapshots
/// - Age of oldest snapshot
///
/// # Arguments
///
/// * `total_size` - Total storage size in bytes
/// * `snapshot_count` - Number of snapshots/versions
/// * `oldest_age` - Age of oldest snapshot in days
///
/// # Returns
///
/// A score between 0.0 and 1.0 where higher means more cost impact.
pub fn calculate_storage_cost_impact(
    total_size: u64,
    snapshot_count: usize,
    oldest_age: f64,
) -> f64 {
    let mut impact: f64 = 0.0;

    // Impact from total size
    let size_gb = total_size as f64 / (1024.0 * 1024.0 * 1024.0);
    if size_gb > STORAGE_SIZE_HIGH_GB {
        impact += 0.4;
    } else if size_gb > STORAGE_SIZE_MEDIUM_GB {
        impact += 0.3;
    } else if size_gb > STORAGE_SIZE_LOW_GB {
        impact += 0.2;
    }

    // Impact from snapshot count
    if snapshot_count > SNAPSHOT_COUNT_HIGH {
        impact += 0.3;
    } else if snapshot_count > SNAPSHOT_COUNT_MEDIUM {
        impact += 0.2;
    } else if snapshot_count > SNAPSHOT_COUNT_LOW {
        impact += 0.1;
    }

    // Impact from age
    if oldest_age > SNAPSHOT_AGE_OLD_DAYS {
        impact += 0.3;
    } else if oldest_age > SNAPSHOT_AGE_MEDIUM_DAYS {
        impact += 0.2;
    }

    impact.clamp(0.0_f64, 1.0_f64)
}

/// Calculate retention efficiency based on snapshot count and age.
///
/// The score ranges from 0.0 (poor efficiency) to 1.0 (optimal efficiency).
/// Factors considered:
/// - Number of snapshots (too many = lower efficiency)
/// - Retention period (too long or too short = lower efficiency)
///
/// # Arguments
///
/// * `snapshot_count` - Total number of snapshots
/// * `oldest_age` - Age of the oldest snapshot in days
/// * `newest_age` - Age of the newest snapshot in days
///
/// # Returns
///
/// A score between 0.0 and 1.0 where:
/// * 1.0 = optimal retention efficiency
/// * 0.0 = poor retention efficiency (too many snapshots or inappropriate retention period)
pub fn calculate_retention_efficiency(
    snapshot_count: usize,
    oldest_age: f64,
    newest_age: f64,
) -> f64 {
    let mut efficiency: f64 = 1.0;

    // Penalize too many snapshots
    if snapshot_count > RETENTION_SNAPSHOT_VERY_HIGH {
        efficiency -= RETENTION_PENALTY_VERY_HIGH;
    } else if snapshot_count > RETENTION_SNAPSHOT_HIGH {
        efficiency -= RETENTION_PENALTY_HIGH;
    } else if snapshot_count > RETENTION_SNAPSHOT_MEDIUM {
        efficiency -= RETENTION_PENALTY_MEDIUM;
    } else if snapshot_count > RETENTION_SNAPSHOT_LOW {
        efficiency -= RETENTION_PENALTY_LOW;
    }

    // Reward appropriate retention period
    let retention_days = oldest_age - newest_age;
    if retention_days > RETENTION_MAX_DAYS {
        efficiency -= RETENTION_TOO_LONG_PENALTY; // Too long retention
    } else if retention_days < RETENTION_MIN_DAYS {
        efficiency -= RETENTION_TOO_SHORT_PENALTY; // Too short retention
    }

    efficiency.clamp(0.0_f64, 1.0_f64)
}

/// Calculate recommended retention period.
///
/// Provides a retention period recommendation based on snapshot count and age.
/// Uses heuristics to balance time travel capabilities with storage costs.
///
/// # Arguments
///
/// * `snapshot_count` - Total number of snapshots
/// * `oldest_age` - Age of the oldest snapshot in days
///
/// # Returns
///
/// Recommended retention period in days:
/// * 30 days for high snapshot count (>1000) or very old data (>365 days)
/// * 60 days for medium snapshot count (>500) or old data (>90 days)
/// * 90 days for moderate snapshot count (>100) or recent data (>30 days)
/// * 180 days for low snapshot count and recent data
pub fn calculate_recommended_retention(snapshot_count: usize, oldest_age: f64) -> u64 {
    // Simple heuristic: recommend retention based on snapshot count and age
    if snapshot_count > RETENTION_SNAPSHOT_VERY_HIGH
        || oldest_age > RECOMMENDED_RETENTION_VERY_OLD_DAYS
    {
        RECOMMENDED_RETENTION_SHORT // 30 days for high snapshot count or very old data
    } else if snapshot_count > RETENTION_SNAPSHOT_HIGH
        || oldest_age > RECOMMENDED_RETENTION_OLD_DAYS
    {
        RECOMMENDED_RETENTION_MEDIUM // 60 days for medium snapshot count or old data
    } else if snapshot_count > RETENTION_SNAPSHOT_MEDIUM
        || oldest_age > RECOMMENDED_RETENTION_RECENT_DAYS
    {
        RECOMMENDED_RETENTION_LONG // 90 days for moderate snapshot count or recent data
    } else {
        RECOMMENDED_RETENTION_VERY_LONG // 180 days for low snapshot count and recent data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_stability_perfect_score() {
        // No changes = perfect stability (clamped to 1.0)
        let score = calculate_schema_stability_score(0, 0, 0.0, 60.0);
        assert!((score - 1.0).abs() < 0.01); // 1.0 + 0.1 bonus, clamped to 1.0
    }

    #[test]
    fn test_schema_stability_with_breaking_changes() {
        let score = calculate_schema_stability_score(5, 3, 0.05, 10.0);
        // Should be penalized for breaking changes
        assert!(score < 1.0);
        assert!(score > 0.0);
    }

    #[test]
    fn test_schema_stability_high_frequency() {
        let score = calculate_schema_stability_score(100, 20, 2.0, 1.0);
        // Should be heavily penalized
        assert!(score < 0.5);
    }

    #[test]
    fn test_storage_cost_impact_low() {
        let impact = calculate_storage_cost_impact(1024 * 1024, 5, 7.0);
        // Small size, few snapshots, recent = low impact
        assert!(impact < 0.3);
    }

    #[test]
    fn test_storage_cost_impact_high() {
        let impact = calculate_storage_cost_impact(200 * 1024 * 1024 * 1024, 150, 120.0);
        // Large size, many snapshots, old = high impact
        assert!(impact > 0.8);
    }

    #[test]
    fn test_retention_efficiency_perfect() {
        // Few snapshots, reasonable retention period
        let efficiency = calculate_retention_efficiency(10, 30.0, 0.0);
        assert!((0.9..=1.0).contains(&efficiency));
    }

    #[test]
    fn test_retention_efficiency_too_many_snapshots() {
        // Very high snapshot count (>1000) should be penalized
        let efficiency = calculate_retention_efficiency(1500, 30.0, 0.0);
        assert!(efficiency < 0.7);
    }

    #[test]
    fn test_retention_efficiency_too_long_retention() {
        // Retention period > 365 days should be penalized
        let efficiency = calculate_retention_efficiency(10, 400.0, 0.0);
        assert!(efficiency < 1.0);
    }

    #[test]
    fn test_retention_efficiency_too_short_retention() {
        // Retention period < 7 days should be penalized
        let efficiency = calculate_retention_efficiency(10, 5.0, 0.0);
        assert!(efficiency < 1.0);
    }

    #[test]
    fn test_recommended_retention_few_snapshots_recent() {
        // Few snapshots, recent data = longest retention
        let retention = calculate_recommended_retention(10, 20.0);
        assert_eq!(retention, RECOMMENDED_RETENTION_VERY_LONG); // 180 days
    }

    #[test]
    fn test_recommended_retention_moderate_snapshots() {
        // Moderate snapshot count (>100) = 90 days
        let retention = calculate_recommended_retention(150, 20.0);
        assert_eq!(retention, RECOMMENDED_RETENTION_LONG); // 90 days
    }

    #[test]
    fn test_recommended_retention_high_snapshots() {
        // High snapshot count (>500) = 60 days
        let retention = calculate_recommended_retention(600, 20.0);
        assert_eq!(retention, RECOMMENDED_RETENTION_MEDIUM); // 60 days
    }

    #[test]
    fn test_recommended_retention_very_high_snapshots() {
        // Very high snapshot count (>1000) = 30 days
        let retention = calculate_recommended_retention(1500, 20.0);
        assert_eq!(retention, RECOMMENDED_RETENTION_SHORT); // 30 days
    }

    #[test]
    fn test_recommended_retention_very_old_data() {
        // Very old data (>365 days) = 30 days regardless of snapshot count
        let retention = calculate_recommended_retention(10, 400.0);
        assert_eq!(retention, RECOMMENDED_RETENTION_SHORT); // 30 days
    }
}
