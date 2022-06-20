package me.paulbares.query;

import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static me.paulbares.SparkUtil.datatypeToClass;

public class DatasetTable implements Table {

  protected final Dataset<Row> dataset;

  protected final List<Field> fields;

  protected final List<? extends Measure> measures;

  protected final int[] measureIndices;

  public DatasetTable(Dataset<Row> dataset,
                      List<? extends Measure> measures,
                      int[] measureIndices,
                      String scenarioFieldName) {
    this.dataset = dataset;
    this.fields = Arrays
            .stream(this.dataset.schema().fields())
            .map(f -> {
              if (f.name().equals(scenarioFieldName)) {
                return new Field(Datastore.SCENARIO_FIELD_NAME, String.class);
              } else {
                return new Field(f.name(), datatypeToClass(f.dataType()));
              }
            })
            .collect(Collectors.toList());
    this.measures = measures;
    this.measureIndices = measureIndices;
  }

  @Override
  public List<? extends Measure> measures() {
    return this.measures;
  }

  @Override
  public int[] measureIndices() {
    return this.measureIndices;
  }

  @Override
  public long count() {
    return this.dataset.count();
  }

  @Override
  public List<Field> headers() {
    return this.fields;
  }

  @Override
  public Iterator<List<Object>> iterator() {
    Iterator<Row> it = this.dataset.toLocalIterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public List<Object> next() {
        return JavaConverters.seqAsJavaList(it.next().toSeq());
      }
    };
  }

  @Override
  public void show(int numRows) {
    this.dataset.show(numRows);
  }
}
