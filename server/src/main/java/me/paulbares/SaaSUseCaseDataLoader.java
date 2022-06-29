package me.paulbares;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.net.URISyntaxException;

/**
 * --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 * <p>
 * The first dataset used.
 */
public class SaaSUseCaseDataLoader {

  public static SparkDatastore createTestDatastoreWithData() {
    SparkDatastore datastore = new SparkDatastore();

    String property = System.getProperty("dataset.path");
    Dataset<Row> dataFrame;
    try {
      String path = property != null ? property : Thread.currentThread().getContextClassLoader().getResource("data/saas.csv").toURI().getPath();
      dataFrame = datastore.spark.read()
              .option("delimiter", ",")
              .option("header", true)
              .option("inferSchema", true)
              .csv(path);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    datastore.spark.conf().set("spark.sql.caseSensitive", String.valueOf(true)); // without it, table names are lowercase.
    dataFrame.createOrReplaceTempView("saas");
    return datastore;
  }
}
