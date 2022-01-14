package me.paulbares;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

  public static final String BASE_STORE_NAME = "base_store";
  public static final String MAIN_SCENARIO_NAME = "base";

  private final Map<String, Dataset<Row>> m = new HashMap<>();

  public final StructType schema;

  public final SparkSession spark;

  private Column[] columns;

  public SparkDatastore(List<Field> fields, Column... columns) {
    this.schema = createSchema(fields.toArray(new Field[0]));
    this.spark = SparkSession
            .builder()
            .appName("Java Spark SQL Example")
            .config("spark.master", "local")
            .getOrCreate();
    this.columns = columns;

  }

  @Override
  public StructField[] getFields() {
    Dataset<Row> base = this.m.get(MAIN_SCENARIO_NAME);
    return base.schema().fields();
  }

  @Override
  public void load(String scenario, List<Object[]> tuples) {
    List<Row> rows = tuples.stream().map(RowFactory::create).toList();
    Dataset<Row> dataFrame = this.spark.createDataFrame(rows, schema);// to load pojo
    for (Column column : columns) {
      dataFrame = dataFrame.withColumn(column.named().name(), column);
    }
    Dataset<Row> previous = this.m.putIfAbsent(scenario, dataFrame);
    if (previous != null) {
      throw new RuntimeException("Already existing dataset for scenario " + scenario);
    }
  }

  public Dataset<Row> get() {
    List<Dataset<Row>> list = new ArrayList<>();
    Dataset<Row> union = null;
    for (Map.Entry<String, Dataset<Row>> e : this.m.entrySet()) {
      if (e.getKey().equals(MAIN_SCENARIO_NAME)) {
        union = e.getValue().withColumn("scenario", functions.lit(e.getKey()));
        for (Dataset<Row> d : list) {
          union = union.unionAll(d);
        }
      } else {
        Dataset<Row> scenario = e.getValue().withColumn("scenario", functions.lit(e.getKey()));
        if (union == null) {
          list.add(scenario);
        } else {
          union = union.unionAll(scenario);
        }
      }
    }
    return union;
  }

  private static StructType createSchema(Field... fields) {
    StructType schema = new StructType();
    for (Field field : fields) {
      DataType type;
      if (field.type().equals(String.class)) {
        type = DataTypes.StringType;
      } else if (field.type().equals(Double.class) || field.type().equals(double.class)) {
        type = DataTypes.DoubleType;
      } else if (field.type().equals(Float.class) || field.type().equals(float.class)) {
        type = DataTypes.FloatType;
      } else if (field.type().equals(Integer.class) || field.type().equals(int.class)) {
        type = DataTypes.IntegerType;
      } else if (field.type().equals(Long.class) || field.type().equals(long.class)) {
        type = DataTypes.LongType;
      } else {
        throw new RuntimeException();
      }
      schema = schema.add(field.name(), type);
    }
    return schema;
  }
}
