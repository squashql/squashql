package me.paulbares;

import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public final class SparkUtil {

  private SparkUtil() {
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

  public static StructType createSchema(List<Field> fields) {
    StructType schema = new StructType();
    for (Field field : fields) {
      schema = schema.add(field.name(), classToDatatype(field.type()));
    }
    return schema;
  }

  public static String getScenarioName(String storeName) {
    return storeName + "_" + Datastore.SCENARIO_FIELD_NAME;
  }
}