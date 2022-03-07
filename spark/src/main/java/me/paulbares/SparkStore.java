package me.paulbares;

import me.paulbares.store.Field;
import me.paulbares.store.Store;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkStore implements Store {

  protected final String name;
  protected final List<Column> columns;
  protected final Map<String, Dataset<Row>> dfByScenario = new HashMap<>();

  protected SparkSession spark;
  /**
   * The schema does not take into account calculated columns and the additional column used to store the scenario
   * values.
   */
  protected StructType baseSchema;

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

  public StructType getBaseSchema() {
    return this.baseSchema;
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
    return dataFrame.withColumn(scenarioFieldName(), functions.lit(scenario));
  }

  protected void save(String scenario, Dataset<Row> dataset) {
    Dataset<Row> previous = this.dfByScenario.putIfAbsent(scenario, dataset);
    if (previous != null) {
      this.dfByScenario.put(scenario, previous.union(dataset));
    }
  }

  @Override
  public void loadCsv(String scenario, String path, String delimiter, boolean header) {
    DataFrameReader reader = this.spark.read()
            .option("delimiter", delimiter)
            .option("header", true);
    if (this.baseSchema != null) {
      reader = reader.schema(this.baseSchema);
    }

    Dataset<Row> dataFrame = reader.csv(path);

    if (this.baseSchema == null) {
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
