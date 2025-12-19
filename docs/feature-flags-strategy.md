# Feature Flags Strategy for Hudi and Lance

This document outlines the strategy for implementing optional feature flags for Apache Hudi and Lance table format support.

## Motivation

- Hudi and Lance modules currently have 0% test coverage
- These formats may not be production-ready
- Users who only need Delta/Iceberg shouldn't pay the compile-time cost
- The `lance` crate adds significant dependencies

## Proposed Features

| Feature | Default | Dependencies | Description |
|---------|---------|--------------|-------------|
| `hudi` | Off | None | Apache Hudi table support |
| `lance` | Off | `lance` crate | Lance table support |

## Implementation Options

### Option A: Features Off by Default (Recommended)

Users explicitly opt-in to Hudi/Lance support:

```toml
[dependencies]
lake-pulse = { version = "0.1", features = ["hudi", "lance"] }
```

**Pros:**
- Smaller default binary size
- Faster default compile times
- Clear signal that these are experimental

**Cons:**
- Users must know to enable features

### Option B: Features On by Default

Users can opt-out if they don't need Hudi/Lance:

```toml
[dependencies]
lake-pulse = { version = "0.1", default-features = false, features = ["delta", "iceberg"] }
```

**Pros:**
- Full functionality out of the box

**Cons:**
- Larger binary for users who don't need these formats
- Slower compile times

## Files to Modify

### 1. `Cargo.toml`

```toml
[features]
default = []
hudi = []
lance = ["dep:lance"]

[dependencies]
lance = { version = "0.39", optional = true }
```

### 2. `src/analyze/mod.rs`

```rust
pub mod delta;
pub mod iceberg;

#[cfg(feature = "hudi")]
pub mod hudi;

#[cfg(feature = "lance")]
pub mod lance;
```

### 3. `src/reader/mod.rs`

```rust
pub mod delta;
pub mod iceberg;

#[cfg(feature = "hudi")]
pub mod hudi;

#[cfg(feature = "lance")]
pub mod lance;
```

### 4. `src/analyze/analyzer.rs`

Gate imports:

```rust
use crate::analyze::delta::DeltaAnalyzer;
use crate::analyze::iceberg::IcebergAnalyzer;

#[cfg(feature = "hudi")]
use crate::analyze::hudi::HudiAnalyzer;

#[cfg(feature = "lance")]
use crate::analyze::lance::analyze::LanceAnalyzer;

#[cfg(feature = "hudi")]
use crate::reader::hudi::reader::HudiReader;
```

Gate match arms in `analyze()`:

```rust
match table_type.as_str() {
    "delta" => Arc::new(DeltaAnalyzer::new(...)),
    "iceberg" => Arc::new(IcebergAnalyzer::new(...)),
    
    #[cfg(feature = "hudi")]
    "hudi" => Arc::new(HudiAnalyzer::new(...)),
    
    #[cfg(feature = "lance")]
    "lance" => Arc::new(LanceAnalyzer::new(...)),
    
    #[cfg(not(feature = "hudi"))]
    "hudi" => return Err("Hudi support not enabled. Enable the 'hudi' feature.".into()),
    
    #[cfg(not(feature = "lance"))]
    "lance" => return Err("Lance support not enabled. Enable the 'lance' feature.".into()),
    
    _ => return Err(format!("Unknown table type: {}", table_type).into()),
}
```

### 5. `src/util/helpers.rs` (Optional)

Detection logic can remain ungated since it's just string matching and very cheap.
This allows the analyzer to detect the table type and provide a helpful error message
when the feature is not enabled.

## Error Handling

When a table type is detected but the corresponding feature is disabled:

```
Error: Hudi support not enabled. Enable the 'hudi' feature in Cargo.toml:
  lake-pulse = { version = "0.1", features = ["hudi"] }
```

## Testing Considerations

- Tests for Hudi/Lance modules should be gated with `#[cfg(feature = "...")]`
- CI should test with all feature combinations:
  - `cargo test` (default features)
  - `cargo test --features hudi`
  - `cargo test --features lance`
  - `cargo test --all-features`

## Migration Path

1. Implement feature flags with features **off** by default
2. Add tests for Hudi/Lance modules
3. Once coverage reaches 80%+, consider making features **on** by default

