package me.paulbares.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.jdk.javaapi.CollectionConverters;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DatasetTable implements Table {

  protected final Dataset<Row> dataset;

  public DatasetTable(Dataset<Row> dataset) {
    this.dataset = dataset;
  }

  @Override
  public long rowCount() {
    return this.dataset.count();
  }

  @Override
  public List<String> headers() {
    return Arrays.asList(this.dataset.columns());
  }

  @Override
  public Iterator<List<Object>> rowIterator() {
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
}
