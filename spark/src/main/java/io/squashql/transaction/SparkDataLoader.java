package io.squashql.transaction;

import io.squashql.SparkDatastore;
import io.squashql.SparkUtil;
import io.squashql.type.TableTypedField;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;

public class SparkDataLoader implements DataLoader {

  protected final SparkSession spark;

  public SparkDataLoader(SparkSession spark) {
    this.spark = spark;
  }

  public void createTemporaryTable(String table, List<TableTypedField> fields) {
    createTemporaryTable(this.spark, table, fields);
  }

  public static void createTemporaryTable(SparkSession spark, String table, List<TableTypedField> fields) {
    StructType schema = SparkUtil.createSchema(fields);
    spark.conf().set("spark.sql.caseSensitive", String.valueOf(true)); // without it, table names are lowercase.
    spark
            .createDataFrame(Collections.emptyList(), schema)
            .createOrReplaceTempView(table);
  }

  @Override
  public void load(String table, List<Object[]> tuples) {
    Dataset<Row> dataFrame = this.spark.createDataFrame(
            tuples.stream().map(RowFactory::create).toList(),
            SparkUtil.createSchema(SparkDatastore.getFields(this.spark, table)));// to load pojo
    appendDataset(this.spark, table, dataFrame);
  }

  static void appendDataset(SparkSession spark, String store, Dataset<Row> dataset) {
    String viewName = "tmp_" + store;
    spark.sql("ALTER VIEW " + store + " RENAME TO " + viewName);
    Dataset<Row> table = spark.table(viewName);
    Dataset<Row> union = table.union(dataset);
    union.createOrReplaceTempView(store);
    spark.catalog().dropTempView(viewName);
  }

  @Override
  public void loadCsv(String store, String path, String delimiter, boolean header) {
    Dataset<Row> dataFrame = this.spark.read()
            .option("delimiter", delimiter)
            .option("header", true)
            .csv(path);

    Table table = null;
    try {
      table = this.spark.catalog().getTable(store);
    } catch (AnalysisException e) {
      // Swallow, table not found
    }

    if (table == null) {
      this.spark.conf().set("spark.sql.caseSensitive", String.valueOf(true)); // without it, table names are lowercase.
      dataFrame.createOrReplaceTempView(store);
    } else {
      appendDataset(this.spark, store, dataFrame);
    }
  }
}
