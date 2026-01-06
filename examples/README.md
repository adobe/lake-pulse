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

## Feature-Gated Examples

Some examples require specific feature flags to be enabled:

| Example | Required Feature |
|---------|------------------|
| `local_store_hudi` | `hudi` |
| `local_store_lance` | `lance` |

To run these examples, use the `--features` flag:

```bash
cargo run --features hudi --example local_store_hudi
cargo run --features lance --example local_store_lance

# Or use the all feature to enable all table formats:
cargo run --features all --example local_store_hudi
```
