package me.paulbares;

import me.paulbares.store.Field;
import me.paulbares.store.Store;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CopySparkStore implements Store {

  protected final String name;

  protected final List<Column> columns;
  protected final Map<String, Dataset<Row>> dfByScenario = new HashMap<>();

  protected SparkSession spark;
  /**
   * The schema does not take into account calculated columns and the additional column used to store the scenario
   * values.
   */
  protected StructType baseSchema;

  public CopySparkStore(String name, Column... columns) {
    this(name, null, columns);
  }

  public CopySparkStore(String name, List<Field> fields, Column... columns) {
    this.name = name;
    this.columns = columns != null ? Arrays.asList(columns) : Collections.emptyList();
    if (fields != null) {
      this.baseSchema = createSchema(fields.toArray(new Field[0]));
    }
  }

  public StructType getBaseSchema() {
    return this.baseSchema;
  }

  public static StructType createSchema(Field... fields) {
    StructType schema = new StructType();
    for (Field field : fields) {
      schema = schema.add(field.name(), CopySparkStore.classToDatatype(field.type()));
    }
    return schema;
  }

  public void setSparkSession(SparkSession spark) {
    this.spark = spark;
  }

  public void persist(StorageLevel storageLevel) {
    this.dfByScenario.replaceAll((k, v) -> v.persist(storageLevel));
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public List<Field> getFields() {
    return Arrays
            .stream(get().schema().fields())
            .map(f -> new Field(f.name(), CopySparkStore.datatypeToClass(f.dataType())))
            .collect(Collectors.toList());
  }

//  @Override
//  public void load(String scenario, List<Object[]> tuples) {
//    List<Row> rows = tuples.stream().map(RowFactory::create).toList();
//    Dataset<Row> dataFrame = this.spark.createDataFrame(rows, this.baseSchema);// to load pojo
//    Dataset<Row> dataset = addAdditionalColumns(scenario, dataFrame);
//    save(scenario, dataset);
//  }

//  protected Dataset<Row> addAdditionalColumns(String scenario, Dataset<Row> dataFrame) {
//    for (Column column : this.columns) {
//      dataFrame = dataFrame.withColumn(column.named().name(), column);
//    }
//    return dataFrame.withColumn(scenarioFieldName(), functions.lit(scenario));
//  }

//  protected void save(String scenario, Dataset<Row> dataset) {
//    Dataset<Row> previous = this.dfByScenario.putIfAbsent(scenario, dataset);
//    if (previous != null) {
//      this.dfByScenario.put(scenario, previous.union(dataset));
//    }
//  }

//  @Override
//  public void loadCsv(String scenario, String path, String delimiter, boolean header) {
//    DataFrameReader reader = this.spark.read()
//            .option("delimiter", delimiter)
//            .option("header", true);
//    if (this.baseSchema != null) {
//      reader = reader.schema(this.baseSchema);
//    }
//
//    Dataset<Row> dataFrame = reader.csv(path);
//
//    if (this.baseSchema == null) {
//      this.baseSchema = dataFrame.schema();
//    }
//
//    Dataset<Row> dataset = addAdditionalColumns(scenario, dataFrame);
//    save(scenario, dataset);
//  }

  public Dataset<Row> get() {
    Dataset<Row> merge = null;
    for (Map.Entry<String, Dataset<Row>> e : this.dfByScenario.entrySet()) {
      if (merge == null) {
        merge = e.getValue();
      } else {
        merge = merge.union(e.getValue());
      }
    }
    return merge;
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
