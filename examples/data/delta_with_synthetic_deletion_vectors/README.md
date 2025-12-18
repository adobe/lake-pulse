# Synthetic Delta Table with Deletion Vectors

This is a **synthetic test dataset** created to test the lake-pulse analyzer's ability to extract and display deletion vector metrics.

## Purpose

Deletion vectors in Delta Lake are a **cost-based optimization** feature. Spark's optimizer decides at runtime whether to use deletion vectors or rewrite files based on:
- File size
- Number of rows to delete vs total rows
- I/O cost estimates

With small test datasets, Spark almost never creates deletion vectors because it's cheaper to rewrite the files. This makes it very difficult to test deletion vector functionality reliably.

This synthetic dataset solves that problem by providing a **deterministic, controlled test case** with manually crafted transaction log files that contain deletion vector references.

## Dataset Structure

### Transaction Log Files

- **00000000000000000000.json**: CREATE TABLE with deletion vectors enabled
  - Protocol: minReaderVersion=3, minWriterVersion=7
  - Reader features: `["deletionVectors"]`
  - Writer features: `["deletionVectors"]`
  - Table property: `delta.enableDeletionVectors=true`

- **00000000000000000001.json**: Initial data load
  - 3 parquet files added
  - 1000 total rows (350 + 350 + 300)
  - ~45KB total size

- **00000000000000000002.json**: First DELETE operation
  - Deletes rows where `category = 'A'`
  - Creates 2 deletion vectors (for first 2 files)
  - 150 rows deleted (75 + 75)
  - Total DV size: 85 bytes (45 + 40)

- **00000000000000000003.json**: Second DELETE operation
  - Deletes rows where `value > 900`
  - Updates all 3 files with new deletion vectors
  - 45 additional rows deleted (15 + 13 + 17 estimated)
  - Total DV size: 138 bytes (52 + 48 + 38)

### Data Files

- `part-00000-*.parquet`: 15KB dummy file
- `part-00001-*.parquet`: 15KB dummy file
- `part-00002-*.parquet`: 15KB dummy file

### Deletion Vector Files

- `deletion_vector_vw@~5&4S8{+Jm!pP-Ad.bin`: Used by transaction 2
- `deletion_vector_xy0~605T90,Kn0qQ.Be.bin`: Used by transaction 3

## Expected Metrics

When analyzed by lake-pulse, this dataset should show:

- **Deletion Vector Count**: 5 (2 from txn 2 + 3 from txn 3)
- **Total DV Size**: ~223 bytes
- **Deleted Rows**: 410 total
- **DV Age**: Based on timestamps in transaction log

## Notes

- The parquet files are dummy files (filled with zeros) since we're only testing metadata extraction
- The deletion vector `.bin` files contain dummy data - they don't need to be valid RoaringBitmap format
- The `pathOrInlineDv` values in the transaction log are encoded references to the deletion vector files
- This dataset demonstrates the Merge-on-Read (MoR) pattern where deletes are tracked via bitmaps instead of rewriting files

