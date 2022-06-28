package me.paulbares;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.net.URISyntaxException;
import java.net.URL;

/**
 * --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 * <p>
 * The first dataset used.
 */
public class SaaSUseCaseDataLoader {

  public static SparkDatastore createTestDatastoreWithData() {
    SparkDatastore datastore = new SparkDatastore();

    URL url = Thread.currentThread().getContextClassLoader().getResource("data/saas.csv");
    Dataset<Row> dataFrame;
    try {
      dataFrame = datastore.spark.read()
              .option("delimiter", ",")
              .option("header", true)
              .option("inferSchema", true)
              .csv(url.toURI().getPath());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    datastore.spark.conf().set("spark.sql.caseSensitive", String.valueOf(true)); // without it, table names are lowercase.
    dataFrame.createOrReplaceTempView("saas");
    return datastore;
  }
}
