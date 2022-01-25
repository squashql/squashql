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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

public class SparkStore implements Store {

  private SparkSession spark;
  private final String name;
  private final List<Column> columns;
  private final StructType schema;
  private final Map<String, Dataset<Row>> m = new HashMap<>();

  public SparkStore(String name, List<Field> fields, Column... columns) {
    this.name = name;
    this.columns = columns != null ? Arrays.asList(columns) : Collections.emptyList();
    this.schema = createSchema(fields.toArray(new Field[0]));
  }

  public void setSparkSession(SparkSession spark) {
    this.spark = spark;
  }

  private static StructType createSchema(Field... fields) {
    StructType schema = new StructType();
    for (Field field : fields) {
      schema = schema.add(field.name(), SparkDatastore.classToDatatype(field.type()));
    }
    return schema;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public List<Field> getFields() {
    Dataset<Row> base = this.m.get(MAIN_SCENARIO_NAME);
    return Arrays
            .stream(base.schema().fields())
            .map(f -> new Field(f.name(), SparkDatastore.datatypeToClass(f.dataType())))
            .collect(Collectors.toList());
  }

  @Override
  public void load(String scenario, List<Object[]> tuples) {
    List<Row> rows = tuples.stream().map(RowFactory::create).toList();
    Dataset<Row> dataFrame = this.spark.createDataFrame(rows, this.schema);// to load pojo
    for (Column column : this.columns) {
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
}
