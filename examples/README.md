# Examples

This is a folder where some examples are provided to understand the use of the library.

## Prerequisites

Some tables need to be created, some are easy to create, some are not. The `data/README.md` file
contains instructions on how to create the tables.

For Azure ADLS and AWS S3 storage examples you will need to provide the needed credentials.

## Run the Examples

To run the examples, you need to have `cargo` installed. Then, you can run the examples as follows:

```bash
cargo run --example <example_name>
```

For example, to run the `local_store.rs` example, you can run:

```bash
cargo run --example local_store
```

### Feature Flags

Some examples require specific feature flags to be enabled. Lake Pulse uses feature flags to control which table formats are compiled:

| Feature | Formats Included |
|---------|------------------|
| `default` | Delta Lake + Apache Iceberg |
| `delta` | Delta Lake only |
| `iceberg` | Apache Iceberg only |
| `hudi` | Apache Hudi only |
| `lance` | Lance only |
| `experimental` | Apache Hudi + Lance |

To run examples for non-default formats, use `--all-features` or enable specific features:

```bash
# Run with all features enabled
cargo run --all-features --example local_store_hudi
cargo run --all-features --example local_store_lance

# Or enable specific features
cargo run --features experimental --example local_store_hudi
cargo run --features lance --example local_store_lance
```

Examples that work with default features (no extra flags needed):
- `local_store.rs` - Delta Lake example
- `local_store_iceberg.rs` - Iceberg example
- `s3_store.rs` - AWS S3 example
- `adl_store.rs` - Azure Data Lake example

Examples that require additional features:
- `local_store_hudi.rs` - Requires `hudi` or `experimental` feature
- `local_store_lance.rs` - Requires `lance` or `experimental` feature
