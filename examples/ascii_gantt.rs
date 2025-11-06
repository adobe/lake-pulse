use lake_health::analyze::metrics::{GanttConfig, TimedLikeMetrics};
use std::collections::LinkedList;

fn main() {
    // Create sample timing data
    let mut collection = LinkedList::new();

    // Simulate some timing data (name, start_time_ms, duration_ms)
    collection.push_back(("storage_config_new_dur".to_string(), 1000, 50));
    collection.push_back(("analyzer_new_dur".to_string(), 1050, 300));
    collection.push_back(("list_files".to_string(), 1350, 1200));
    collection.push_back(("read_metadata".to_string(), 2550, 800));
    collection.push_back(("analyze_partitions".to_string(), 3350, 400));
    collection.push_back(("analyze_total_dur".to_string(), 1350, 2400));
    collection.push_back(("total_dur".to_string(), 1000, 2750));

    let metrics = TimedLikeMetrics {
        duration_collection: collection,
    };

    println!("ASCII Gantt Chart - Default Configuration");
    println!("==========================================\n");

    // Generate with default configuration
    let gantt_default = metrics.duration_collection_as_gantt(None);
    println!("{}", gantt_default.unwrap());

    println!("\n\nASCII Gantt Chart - Custom Configuration");
    println!("=========================================\n");

    // Generate with custom configuration
    let custom_config = GanttConfig {
        timeline_width: 80, // Wider timeline
        name_width: 25,     // Narrower name column
        num_markers: 8,     // More time markers
    };

    let gantt_custom = metrics.duration_collection_as_gantt(Some(custom_config));
    println!("{}", gantt_custom.unwrap());

    println!("\n\nASCII Gantt Chart - Compact Configuration");
    println!("==========================================\n");

    // Generate with compact configuration
    let compact_config = GanttConfig {
        timeline_width: 40, // Narrower timeline
        name_width: 20,     // Shorter names
        num_markers: 5,     // Fewer markers
    };

    let gantt_compact = metrics.duration_collection_as_gantt(Some(compact_config));
    println!("{}", gantt_compact.unwrap());
}
