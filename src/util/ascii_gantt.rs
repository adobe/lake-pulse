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

use std::collections::LinkedList;
use std::error::Error;

/// Represents a task in the Gantt chart
#[derive(Debug, Clone)]
struct GanttTask {
    name: String,
    start: u128,
    duration: u128,
}

/// Configuration for ASCII Gantt chart rendering
pub struct GanttConfig {
    /// Width of the timeline in characters
    pub timeline_width: usize,
    /// Maximum width for task names
    pub name_width: usize,
    /// Number of time markers to show
    pub num_markers: usize,
}

impl Default for GanttConfig {
    fn default() -> Self {
        Self {
            timeline_width: 80,
            name_width: 30,
            num_markers: 6,
        }
    }
}

/// Generate an ASCII Gantt chart from timing data
pub fn to_ascii_gantt(
    start_duration_collection: &LinkedList<(String, u128, u128)>,
    config: Option<GanttConfig>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let config = config.unwrap_or_default();

    if start_duration_collection.is_empty() {
        return Err("No timing data available".to_string().into());
    }

    // Convert to internal representation
    let tasks: Vec<GanttTask> = start_duration_collection
        .iter()
        .map(|(name, start, duration)| GanttTask {
            name: name.clone(),
            start: *start,
            duration: *duration,
        })
        .collect();

    // Find the overall time range
    let min_start = tasks.iter().map(|t| t.start).min().unwrap_or(0);
    let max_end = tasks
        .iter()
        .map(|t| t.start + t.duration)
        .max()
        .unwrap_or(0);
    let total_duration = max_end - min_start;

    if total_duration == 0 {
        return Err("All tasks have zero duration".to_string().into());
    }

    let mut output = String::new();

    // Title
    output.push_str("Timeline (ms):\n");

    // Time markers
    let marker_line = generate_time_markers(
        min_start,
        max_end,
        config.num_markers,
        config.name_width,
        config.timeline_width,
    );
    output.push_str(&marker_line);
    output.push('\n');

    // Separator line with tick marks
    let separator =
        generate_separator(config.num_markers, config.name_width, config.timeline_width);
    output.push_str(&separator);
    output.push('\n');

    // Generate each task bar
    for task in &tasks {
        let task_line = generate_task_line(
            &task.name,
            task.start,
            task.duration,
            min_start,
            total_duration,
            &config,
        );
        output.push_str(&task_line);
        output.push('\n');
    }

    Ok(output)
}

/// Convert milliseconds to hh:mm:ss.s format
fn format_time_hms(ms: u128) -> String {
    let total_seconds = ms / 1000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    let deciseconds = (ms % 1000) / 100;

    format!("{:02}:{:02}:{:02}.{}", hours, minutes, seconds, deciseconds)
}

/// Generate the time marker line
fn generate_time_markers(
    min_start: u128,
    max_end: u128,
    num_markers: usize,
    name_width: usize,
    timeline_width: usize,
) -> String {
    // Add padding for the name column
    let mut line = " ".repeat(name_width);

    // Calculate segment width to match the separator line
    let segment_width = timeline_width / (num_markers - 1);

    // Generate time markers aligned with tick marks
    // Each tick mark is at position: i * segment_width (relative to start of timeline)
    for i in 0..num_markers {
        let time = min_start + (max_end - min_start) * i as u128 / (num_markers - 1) as u128;
        // Convert to relative time (elapsed time from min_start) and format as hh:mm:ss.s
        let relative_time = time - min_start;
        let marker = format_time_hms(relative_time);

        // Calculate the target position for this tick mark
        let tick_position = name_width + i * segment_width;

        // Calculate how much padding we need to reach the tick position
        let current_len = line.len();

        // Align the leftmost digit of the marker with the tick mark
        let padding = tick_position.saturating_sub(current_len);
        line.push_str(&" ".repeat(padding));
        line.push_str(&marker);
    }

    line
}

/// Generate the separator line with tick marks
fn generate_separator(num_markers: usize, name_width: usize, timeline_width: usize) -> String {
    let mut line = String::new();

    // Add padding for the name column
    line.push_str(&" ".repeat(name_width));

    // Generate the timeline with tick marks
    let segment_width = timeline_width / (num_markers - 1);
    for i in 0..num_markers {
        if i == 0 {
            line.push('|');
        } else {
            line.push_str(&"-".repeat(segment_width - 1));
            line.push('|');
        }
    }

    line
}

/// Generate a single task line with its bar
fn generate_task_line(
    name: &str,
    start: u128,
    duration: u128,
    min_start: u128,
    total_duration: u128,
    config: &GanttConfig,
) -> String {
    let mut line = String::new();

    // Truncate or pad the task name
    let display_name = if name.len() > config.name_width - 2 {
        format!("{}.. ", &name[..config.name_width - 4])
    } else {
        format!("{:<width$}", name, width = config.name_width)
    };
    line.push_str(&display_name);

    // Calculate bar position and length
    let relative_start = start.saturating_sub(min_start);
    let start_pos =
        (relative_start as f64 / total_duration as f64 * config.timeline_width as f64) as usize;
    let bar_length =
        ((duration as f64 / total_duration as f64 * config.timeline_width as f64) as usize).max(1);

    // Add leading spaces
    line.push_str(&" ".repeat(start_pos));

    // Add the bar
    line.push('[');
    if bar_length > 2 {
        line.push_str(&"=".repeat(bar_length - 2));
    }
    line.push(']');

    // Add duration info at the end
    line.push_str(&format!(" {}ms", duration));

    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_collection() {
        let collection = LinkedList::new();
        let result = to_ascii_gantt(&collection, None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "No timing data available");
    }

    #[test]
    fn test_single_task() {
        let mut collection = LinkedList::new();
        collection.push_back(("task1".to_string(), 0, 100));
        let result = to_ascii_gantt(&collection, None).unwrap();
        assert!(result.contains("task1"));
        assert!(result.contains("100ms"));
    }

    #[test]
    fn test_multiple_tasks() {
        let mut collection = LinkedList::new();
        collection.push_back(("task1".to_string(), 0, 100));
        collection.push_back(("task2".to_string(), 50, 150));
        collection.push_back(("task3".to_string(), 100, 100));

        let result = to_ascii_gantt(&collection, None).unwrap();
        assert!(result.contains("task1"));
        assert!(result.contains("task2"));
        assert!(result.contains("task3"));
        assert!(result.contains("Timeline (ms)"));
    }

    #[test]
    fn test_custom_config() {
        let mut collection = LinkedList::new();
        collection.push_back(("task1".to_string(), 0, 100));

        let config = GanttConfig {
            timeline_width: 40,
            name_width: 20,
            num_markers: 5,
        };

        let result = to_ascii_gantt(&collection, Some(config));
        assert!(result.unwrap().contains("task1"));
    }

    #[test]
    fn test_long_task_name() {
        let mut collection = LinkedList::new();
        collection.push_back(("very_long_task_name_that_exceeds_width".to_string(), 0, 100));

        let config = GanttConfig {
            timeline_width: 60,
            name_width: 20,
            num_markers: 6,
        };

        let result = to_ascii_gantt(&collection, Some(config));
        assert!(result.unwrap().contains(".."));
    }
}
