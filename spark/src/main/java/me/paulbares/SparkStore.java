package me.paulbares;

import me.paulbares.store.Field;
import me.paulbares.store.Store;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

public class SparkStore implements Store {

  private final String name;
  private final List<Column> columns;
  private final Map<String, Dataset<Row>> dfByScenario = new HashMap<>();

  private SparkSession spark;
  /**
   * The schema does not take into account calculated columns and the additional column used to store the scenario
   * values.
   */
  private StructType baseSchema;

  public SparkStore(String name, Column... columns) {
    this(name, null, columns);
  }

  public SparkStore(String name, List<Field> fields, Column... columns) {
    this.name = name;
    this.columns = columns != null ? Arrays.asList(columns) : Collections.emptyList();
    if (fields != null) {
      this.baseSchema = createSchema(fields.toArray(new Field[0]));
    }
  }

  private static StructType createSchema(Field... fields) {
    StructType schema = new StructType();
    for (Field field : fields) {
      schema = schema.add(field.name(), SparkDatastore.classToDatatype(field.type()));
    }
    return schema;
  }

  public void setSparkSession(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public List<Field> getFields() {
    return Arrays
            .stream(get().schema().fields())
            .map(f -> new Field(f.name(), SparkDatastore.datatypeToClass(f.dataType())))
            .collect(Collectors.toList());
  }

  @Override
  public void load(String scenario, List<Object[]> tuples) {
    List<Row> rows = tuples.stream().map(RowFactory::create).toList();
    Dataset<Row> dataFrame = this.spark.createDataFrame(rows, this.baseSchema);// to load pojo
    Dataset<Row> dataset = addAdditionalColumns(scenario, dataFrame);
    save(scenario, dataset);
  }

  protected Dataset<Row> addAdditionalColumns(String scenario, Dataset<Row> dataFrame) {
    for (Column column : this.columns) {
      dataFrame = dataFrame.withColumn(column.named().name(), column);
    }
    return dataFrame.withColumn(scenarioFieldName(this.name), functions.lit(scenario));
  }

  protected void save(String scenario, Dataset<Row> dataset) {
    if (this.dfByScenario.putIfAbsent(scenario, dataset) != null) {
      throw new RuntimeException("Already existing dataset for scenario " + scenario);
    }
  }

  public static String scenarioFieldName(String storeName) {
    return storeName.toLowerCase() + "." + SCENARIO_FIELD_NAME;
  }

  @Override
  public void loadCsv(String scenario, String path, String delimiter, boolean header) {
    Dataset<Row> dataFrame = this.spark.read()
            .option("delimiter", delimiter)
            .option("header", true)
            .csv(path);

    if (this.baseSchema != null) {
      StructType schema = dataFrame.schema();
      if (!schema.equals(this.baseSchema)) {
        throw new IllegalStateException("Schema for scenario " + scenario + " is not compatible with previous schema." +
                " Schema from csv: " + schema + ". Previous: " + this.baseSchema);
      }
    } else {
      this.baseSchema = dataFrame.schema();
    }

    Dataset<Row> dataset = addAdditionalColumns(scenario, dataFrame);
    save(scenario, dataset);
  }

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
}
