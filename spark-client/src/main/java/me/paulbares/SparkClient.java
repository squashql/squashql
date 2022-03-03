package me.paulbares;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;

/**
 * First step is to spin up a spark cluster in docker by running 'docker-compose up' in ./docker directory. Cluster is
 * available to this address spark://127.0.0.1:7077.
 */
public class SparkClient {

  static {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
            .setMaster("spark://127.0.0.1:7077")
            .setAppName("Java Spark SQL Example");
    SparkSession spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();

    Dataset<Long> range = spark.range(3);
    range.show();
  }
}
