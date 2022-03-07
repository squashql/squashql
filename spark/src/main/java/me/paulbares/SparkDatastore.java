package me.paulbares;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import me.paulbares.store.Datastore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkDatastore implements Datastore {

  static {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
  }

  public final Map<String, SparkStore> stores = new HashMap<>();

  public final SparkSession spark;

  public SparkDatastore(SparkStore... stores) {
    this.spark = SparkSession
            .builder()
            .appName("Java Spark SQL Example")
            .config("spark.master", "local")
            .getOrCreate();

    for (SparkStore store : stores) {
      store.setSparkSession(this.spark);
      this.stores.put(store.name(), store);
    }
  }

  public SparkDatastore(SparkSession sparkSession, SparkStore... stores) {
    this.spark = sparkSession;
    for (SparkStore store : stores) {
      store.setSparkSession(this.spark);
      this.stores.put(store.name(), store);
    }
  }

  @Override
  public List<SparkStore> stores() {
    return new ArrayList<>(this.stores.values());
  }

  public Dataset<Row> get(String storeName) {
    return this.stores.get(storeName).get();
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    this.stores.get(store).loadCsv(scenario, path, delimiter, header);
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    this.stores.get(store).load(scenario, tuples);
  }

  public void persist(StorageLevel storageLevel) {
    this.stores.forEach((name, store) -> store.persist(storageLevel));
  }

  public static Class<?> datatypeToClass(DataType type) {
    Class<?> klass;
    if (type.equals(DataTypes.StringType)) {
      klass = String.class;
    } else if (type.equals(DataTypes.DoubleType)) {
      klass = double.class;
    } else if (type.equals(DataTypes.FloatType)) {
      klass = float.class;
    } else if (type.equals(DataTypes.IntegerType)) {
      klass = int.class;
    } else if (type.equals(DataTypes.LongType)) {
      klass = long.class;
    } else {
      throw new IllegalArgumentException("Unsupported field type " + type);
    }
    return klass;
  }

  public static DataType classToDatatype(Class<?> clazz) {
    DataType type;
    if (clazz.equals(String.class)) {
      type = DataTypes.StringType;
    } else if (clazz.equals(Double.class) || clazz.equals(double.class)) {
      type = DataTypes.DoubleType;
    } else if (clazz.equals(Float.class) || clazz.equals(float.class)) {
      type = DataTypes.FloatType;
    } else if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
      type = DataTypes.IntegerType;
    } else if (clazz.equals(Long.class) || clazz.equals(long.class)) {
      type = DataTypes.LongType;
    } else {
      throw new IllegalArgumentException("Unsupported field type " + clazz);
    }
    return type;
  }
}
