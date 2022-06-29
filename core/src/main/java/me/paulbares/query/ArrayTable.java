package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.Iterator;
import java.util.List;

public class ArrayTable extends ATable {

  protected final List<List<Object>> rows;

  public ArrayTable(List<Field> headers,
                    List<Measure> measures,
                    int[] measureIndices,
                    int[] columnsIndices,
                    List<List<Object>> rows) {
    super(headers, measures, measureIndices, columnsIndices);
    this.rows = rows;
  }

  @Override
  public void addAggregates(Field field, Measure measure, List<Object> values) {
    throw new RuntimeException("not right path");
  }

  @Override
  public long count() {
    return this.rows.size();
  }

  @Override
  public Iterator<List<Object>> iterator() {
    return this.rows.iterator();
  }
}
