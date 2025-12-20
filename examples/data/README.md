# Create Different Datasets

This is a document to help create different datasets for testing purposes.

This document uses Spark to create the datasets. You can use any other tool
to create the datasets. The important part is to create the datasets with
the correct structure and files.

Be aware that there are already some tables created in the `data` folder that
can be used for testing purposes.

> **Note:** Iceberg metadata files contain placeholders instead of paths.
> This is an Iceberg limitation that is intended. After cloning the
> repository, you need to run the path fix script to update these paths for
> your machine:
>
> ```bash
> cd examples/data/iceberg_dataset
> ./fix_paths.sh
> ```
>
> This only needs to be done once after cloning.

## Delta Lake

First, set your absolute path where you want to create the dataset,
or get it using:

```scala
val absolutePath = 
    new java.io.File("data/delta_dataset").getAbsolutePath
```

```scala
val s = (1 to 20 map { case(i) => 
    (i, s"a-$i", s"${i*2}") 
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")

val absolutePath = "data/delta_dataset"

s.repartition(1).write.format("delta").save("absolutePath")

val s2 = (100 to 200 map { case(i) => 
    (i, s"a-$i", s"${i*2}") 
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")

s2.repartition(1).write.mode("append").format("delta")
    .save(s"file://$absolutePath")

spark.sql(s"DELETE FROM delta.`$absolutePath` WHERE id = 120 OR id = 121")

s2.repartition(1).write.mode("append").format("delta")
    .save(s"file://$absolutePath")
```

Spark Shell Command:
```bash
spark-shell \
--driver-memory 8g \
--packages "com.typesafe:config:1.3.2,org.apache.spark:spark-avro_2.12:3.2.1,com.typesafe:config:1.3.2,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-streaming_2.12:3.2.1,org.apache.spark:spark-sql_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0,io.delta:delta-standalone_2.12:3.3.0,org.json4s:json4s-native_2.12:3.7.0-M11,joda-time:joda-time:2.12.5" \
--driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 -XX:+UseG1GC -Dlog4j.debug=true" \
--conf spark.hadoop.parquet.page.verify-checksum.enabled=true --conf spark.hadoop.parquet.write-checksum.enabled=true --conf parquet.page.verify-checksum.enabled=true --conf parquet.write-checksum.enabled=true --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

## Iceberg dataset

First, set your absolute path where you want to create the dataset,
or get it using:

```scala
val absolutePath = 
    new java.io.File("data/iceberg_dataset").getAbsolutePath
```

```scala
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.{DataFiles, PartitionSpec, Schema}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.expressions.Expressions._

import scala.collection.JavaConverters._

val s = (1 to 20 map { case(i) => 
    (i, s"a-$i", s"${i*2}") 
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")

val absolutePath = "data/iceberg_dataset"
val schema = SparkSchemaUtil.convert(s.schema)
val tables = new HadoopTables(spark.sessionState.newHadoopConf())
val spec = PartitionSpec.unpartitioned()
val table = tables.create(schema, spec, s"file://$absolutePath")

s.repartition(1).write.mode("append")
    .format("iceberg")
    .option("write.metadata.metrics.default", "none")
    .save(s"file://$absolutePath")

val s2 = (100 to 200 map { case(i) => 
    (i, s"a-$i", s"${i*2}") 
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")
    
s2.repartition(1).write.mode("append")
    .format("iceberg")
    .option("write.metadata.metrics.default", "none")
    .save(s"file://$absolutePath")

// Do a delete/rewrite
val table = tables.load(s"file://$absolutePath")
val allDataFiles = table.currentSnapshot()
    .addedDataFiles(table.io()).asScala.toList
val affectedFiles = allDataFiles.filter { dataFile =>
  val filePath = dataFile.path().toString
  val fileDF = spark.read.parquet(filePath)
  val matchCount = fileDF.filter("id = 120 OR id = 121").count()
  matchCount > 0
}

val rewrite = table.newRewrite()
val rewriteBasePath = 
    s"$absolutePath/data/rewrite-${System.currentTimeMillis()}"
val hashes = affectedFiles.map { dataFile =>
	val filePath = dataFile.path().toString
	val fileDF = spark.read.parquet(filePath)
	val filteredDF = fileDF.filter("id != 120 AND id != 121")
	val hash = filePath.hashCode.abs
	val tempPath = s"$rewriteBasePath/$hash"
	filteredDF.write.mode("overwrite").parquet(tempPath)
	rewrite.deleteFile(dataFile)
	hash
}

val fs = FileSystem.get(spark.sessionState.newHadoopConf())
hashes.foreach { h =>

	val newFiles = fs.listStatus(
	    new Path(s"$rewriteBasePath/$h/")
	).filter(_.getPath.getName.endsWith(".parquet"))
	
	newFiles.foreach { file =>
		val filePath = file.getPath.toString
		val recordCount = spark.read.parquet(filePath).count()

		val newDataFile = DataFiles.builder(table.spec())
			.withPath(filePath)
			.withFileSizeInBytes(file.getLen)
			.withFormat("parquet")
			.withRecordCount(recordCount)
			.build()
		rewrite.addFile(newDataFile)
	}	
}
rewrite.commit()
```
Spark Shell Command:

```bash
spark-shell \
--driver-memory 8g \
--packages "com.typesafe:config:1.3.2,org.apache.spark:spark-avro_2.12:3.2.1,com.typesafe:config:1.3.2,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-streaming_2.12:3.2.1,org.apache.spark:spark-sql_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0,io.delta:delta-standalone_2.12:3.3.0,org.json4s:json4s-native_2.12:3.7.0-M11,joda-time:joda-time:2.12.5,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.7.2" \
--driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 -XX:+UseG1GC -Dlog4j.debug=true" \
--conf spark.hadoop.parquet.page.verify-checksum.enabled=true \
--conf spark.hadoop.parquet.write-checksum.enabled=true \
--conf parquet.page.verify-checksum.enabled=true \
--conf parquet.write-checksum.enabled=true \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalogImplementation=in-memory
```

## Apache Hudi

First, set your absolute path where you want to create the dataset:

```scala
val absolutePath =
    new java.io.File("data/hudi_dataset").getAbsolutePath
```

```scala
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

// Create initial dataset
val s = (1 to 20 map { case(i) =>
    (i, s"a-$i", s"${i*2}")
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")

val tableName = "hudi_test_table"

// Write initial data as COPY_ON_WRITE table
s.write.format("hudi")
    .option(PRECOMBINE_FIELD_OPT_KEY, "id")
    .option(RECORDKEY_FIELD_OPT_KEY, "id")
    .option(TABLE_NAME, tableName)
    .option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
    .mode(Overwrite)
    .save(s"file://$absolutePath")

// Append more data
val s2 = (100 to 200 map { case(i) =>
    (i, s"a-$i", s"${i*2}")
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")

s2.write.format("hudi")
    .option(PRECOMBINE_FIELD_OPT_KEY, "id")
    .option(RECORDKEY_FIELD_OPT_KEY, "id")
    .option(TABLE_NAME, tableName)
    .option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
    .mode(Append)
    .save(s"file://$absolutePath")

// Update some records (Hudi will handle upserts)
val s3 = (100 to 110 map { case(i) =>
    (i, s"updated-$i", s"${i*3}")
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")

s3.write.format("hudi")
    .option(PRECOMBINE_FIELD_OPT_KEY, "id")
    .option(RECORDKEY_FIELD_OPT_KEY, "id")
    .option(TABLE_NAME, tableName)
    .option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
    .mode(Append)
    .save(s"file://$absolutePath")
```

Spark Shell Command:

```bash
spark-shell \
--driver-memory 8g \
--packages "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0" \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
```

## Lance

This will work based on the setup and the root location for the catalog,
meaning the `spark.sql.catalog.lance.root=file://$DATA_DIR` setting.

```scala
// Create initial dataset
val s = (1 to 20 map { case(i) =>
    (i, s"a-$i", s"${i*2}")
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")

// Create Lance table using SQL (specify lance catalog explicitly)
spark.sql("""
    CREATE TABLE lance.lance_dataset (
        id INT,
        f1 STRING,
        f2 STRING
    )
""")

// Insert data into the table (use fully qualified name)
s.writeTo("lance.lance_dataset").append()

// Create second dataset for append
val s2 = (100 to 200 map { case(i) =>
    (i, s"a-$i", s"${i*2}")
}).toDF
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "f1")
    .withColumnRenamed("_3", "f2")

// Append more data (use fully qualified name)
s2.writeTo("lance.lance_dataset").append()

// Read Lance table
val lanceDF = spark.table("lance.lance_dataset")

// DELETE operations (supported with Lance Namespace catalog)
spark.sql("DELETE FROM lance.lance_dataset WHERE id = 120 OR id = 121")

// Verify deletion
spark.table("lance.lance_dataset").filter("id IN (119, 120, 121, 122)").show()

// Append more data after deletion
s2.writeTo("lance.lance_dataset").append()

// The .lance extension is added automatically
```

Spark Shell Command:

```bash
DATA_DIR=$(cd examples/data && pwd)

spark-shell \
--driver-memory 8g \
--packages "com.typesafe:config:1.3.2,org.apache.spark:spark-avro_2.12:3.2.1,com.typesafe:config:1.3.2,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-streaming_2.12:3.2.1,org.apache.spark:spark-sql_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0,io.delta:delta-standalone_2.12:3.3.0,org.json4s:json4s-native_2.12:3.7.0-M11,joda-time:joda-time:2.12.5,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.7.2,com.lancedb:lance-spark-bundle-3.5_2.12:0.0.15" \
--driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 -XX:+UseG1GC -Dlog4j.debug=true" \
--conf spark.hadoop.parquet.page.verify-checksum.enabled=true \
--conf spark.hadoop.parquet.write-checksum.enabled=true \
--conf parquet.page.verify-checksum.enabled=true \
--conf parquet.write-checksum.enabled=true \
--conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceNamespaceSparkCatalog \
--conf spark.sql.catalog.lance.impl=dir \
--conf spark.sql.catalog.lance.root=file://$DATA_DIR
```

## Delta Lake with Features

This will create a Delta Table that has the following features enabled:
- Change Data Feed
- Deletion Vectors
- Z-Ordering

```scala
import org.apache.spark.sql.functions._
import io.delta.tables._

val tablePath = "data/delta_dataset_with_features"

spark.sql("DROP TABLE IF EXISTS delta_table_clustered")

spark.sql(s"""
  CREATE TABLE delta_table_clustered (
    id INT,
    name STRING,
    department STRING,
    salary INT
  )
  USING DELTA
  LOCATION '$tablePath'
  TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true'
  )
""")
spark.sql("""
  INSERT INTO delta_table_clustered VALUES
  (1, 'Alice', 'Engineering', 100000),
  (2, 'Bob', 'Sales', 80000),
  (3, 'Charlie', 'Engineering', 95000)
""")

val deltaTable = DeltaTable.forPath(spark, tablePath)
deltaTable.optimize().executeZOrderBy("department")

val append1 = Seq(
  (4, "David", "Marketing", 75000),
  (5, "Eve", "Engineering", 105000)
).toDF("id", "name", "department", "salary")
append1.write
  .format("delta")
  .mode("append")
  .save(tablePath)

val append2 = Seq(
  (6, "Frank", "Sales", 85000),
  (7, "Grace", "Marketing", 78000)
).toDF("id", "name", "department", "salary")
append2.write
  .format("delta")
  .mode("append")
  .save(tablePath)

deltaTable.delete(col("salary") < 80000)

val append3 = Seq(
  (8, "Henry", "Engineering", 98000),
  (9, "Iris", "Sales", 82000)
).toDF("id", "name", "department", "salary")
append3.write
  .format("delta")
  .mode("append")
  .save(tablePath)

val append4 = Seq(
  (10, "Jack", "Marketing", 88000),
  (11, "Kate", "Engineering", 110000)
).toDF("id", "name", "department", "salary")
append4.write
  .format("delta")
  .mode("append")
  .save(tablePath)

deltaTable.delete(col("department") === "Marketing")

val mergeData = Seq(
  (1, "Alice Updated", "Engineering", 110000),
  (12, "Leo", "Sales", 90000)
).toDF("id", "name", "department", "salary")
deltaTable.as("target")
  .merge(
    mergeData.as("source"),
    "target.id = source.id"
  )
  .whenMatched()
  .updateAll()
  .whenNotMatched()
  .insertAll()
  .execute()
```

Spark Shell Command:

```bash
spark-shell \
--driver-memory 8g \
--packages "com.typesafe:config:1.3.2,org.apache.spark:spark-avro_2.12:3.2.1,com.typesafe:config:1.3.2,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-streaming_2.12:3.2.1,org.apache.spark:spark-sql_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0,io.delta:delta-standalone_2.12:3.3.0,org.json4s:json4s-native_2.12:3.7.0-M11,joda-time:joda-time:2.12.5,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.7.2" \
--driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 -XX:+UseG1GC -Dlog4j.debug=true" \
--conf spark.hadoop.parquet.page.verify-checksum.enabled=true --conf spark.hadoop.parquet.write-checksum.enabled=true --conf parquet.page.verify-checksum.enabled=true --conf parquet.write-checksum.enabled=true --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```