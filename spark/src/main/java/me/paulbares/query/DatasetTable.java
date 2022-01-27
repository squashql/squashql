package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.jdk.javaapi.CollectionConverters;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class DatasetTable implements Table {

  protected final Dataset<Row> dataset;
  protected final List<Field> fields;

  public DatasetTable(Dataset<Row> dataset) {
    this.dataset = dataset;
    this.fields = Arrays
            .stream(this.dataset.schema().fields())
            .map(f -> new Field(f.name(), SparkDatastore.datatypeToClass(f.dataType())))
            .collect(Collectors.toList());
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
        return CollectionConverters.asJava(it.next().toSeq());
      }
    };
  }

  @Override
  public void show(int numRows) {
    this.dataset.show(numRows);
  }
}
