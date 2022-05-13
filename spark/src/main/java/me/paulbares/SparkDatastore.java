package me.paulbares;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import me.paulbares.store.Datastore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SparkDatastore implements Datastore {

  static {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
  }

  public final Map<String, SparkStore> stores = new HashMap<>();

  public final SparkSession spark;

  public SparkDatastore(SparkStore... stores) {
    this(SparkSession
                    .builder()
                    .appName("Java Spark SQL Example")
                    .config("spark.master", "local")
                    .getOrCreate(),
            stores);
  }

  public SparkDatastore(SparkSession sparkSession, SparkStore... stores) {
    this.spark = sparkSession;
    for (SparkStore store : stores) {
      store.setSparkSession(this.spark);
      this.stores.put(store.name(), store);
    }
  }

  @Override
  public Map<String, SparkStore> storesByName() {
    return this.stores; // FIXME lazy load
  }

  public Dataset<Row> get(String storeName) {
    return this.stores.get(storeName).get();
  }

  public void persist(StorageLevel storageLevel) {
    this.stores.forEach((name, store) -> store.persist(storageLevel));
  }
}
