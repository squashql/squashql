package io.squashql;

import io.squashql.type.TypedField;
import io.squashql.util.Types;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.time.LocalDate;
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
    } else if (type.equals(DataTypes.ByteType)) {
      klass = byte.class;
    } else if (type.equals(DataTypes.BooleanType)) {
      klass = boolean.class;
    } else if (type.equals(DataTypes.DateType)) {
      klass = LocalDate.class;
    } else {
      if (type.sql().contains("DECIMAL")) {
        return BigDecimal.class;
      }
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
    } else if (clazz.equals(Byte.class) || clazz.equals(byte.class)) {
      type = DataTypes.ByteType;
    } else if (clazz.equals(Boolean.class) || clazz.equals(boolean.class)) {
      type = DataTypes.BooleanType;
    } else if (clazz.equals(LocalDate.class)) {
      type = DataTypes.DateType;
    } else {
      throw new IllegalArgumentException("Unsupported field type " + clazz);
    }
    return type;
  }

  public static Object getTypeValue(Object o) {
    if (o instanceof BigDecimal bd) {
      return Types.castToDouble(bd);
    } else {
      return o;
    }
  }


  public static StructType createSchema(List<TypedField> fields) {
    StructType schema = new StructType();
    for (TypedField field : fields) {
      schema = schema.add(field.fieldName(), classToDatatype(field.type()));
    }
    return schema;
  }
}
