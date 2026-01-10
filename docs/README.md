![Lake Pulse Logo](../lake-pulse-logo-new.svg)

A Rust library for analyzing data lake table health — *checking the pulse* — across multiple formats (Delta Lake, Apache Iceberg, Apache Hudi, Lance) and storage providers (AWS S3, Azure Data Lake, GCS, HDFS, Local).

# Documentation

This directory contains documentation resources for Lake Pulse.

## Supported Formats

[![Delta Lake][delta-badge]][delta-link]
[![Apache Iceberg][iceberg-badge]][iceberg-link]
[![Apache Hudi][hudi-badge]][hudi-link]
[![Lance][lance-badge]][lance-link]

[delta-badge]: badges/delta-lake-logo.svg
[delta-link]: https://github.com/delta-io/delta
[iceberg-badge]: badges/apache-iceberg-logo.svg
[iceberg-link]: https://github.com/apache/iceberg
[hudi-badge]: badges/apache-hudi-logo.svg
[hudi-link]: https://github.com/apache/hudi
[lance-badge]: badges/lance-logo.svg
[lance-link]: https://github.com/lancedb/lance

## Examples

See the [examples](../examples/) directory for more detailed usage examples.

## Cloud Storage Documentation

For detailed information on configuration options, refer to the `object_store` and 
`hdfs-native-object-store` crates documentation:
- [AWS S3 Configuration](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
- [Azure Configuration](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
- [GCP Configuration](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
- [HDFS Configuration](https://docs.rs/hdfs-native-object-store/latest/hdfs_native_object_store/)

## License

See LICENSE files for details.