package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.*;

public class ColumnarTable extends ATable {

  private final List<List<Object>> values;

  public ColumnarTable(List<Field> headers,
                       List<Measure> measures,
                       int[] measureIndices,
                       int[] columnsIndices,
                       List<List<Object>> values) {
    super(new ArrayList<>(headers), new ArrayList<>(measures), measureIndices, columnsIndices);
    this.values = new ArrayList<>(values);
  }

  @Override
  public void addAggregates(Field field, Measure measure, List<Object> values) {
    this.headers.add(field);
    this.measures.add(measure);
    this.measureIndices = Arrays.copyOf(this.measureIndices, this.measureIndices.length + 1);
    this.measureIndices[this.measureIndices.length - 1] = this.headers.size() - 1;
    this.values.add(values);
  }

  @Override
  public long count() {
    return this.values.get(0).size();
  }

  @Override
  public Iterator<List<Object>> iterator() {
    return new ColumnarTableIterator();
  }

  private class ColumnarTableIterator implements Iterator<List<Object>> {

    int current = 0;
    long size = count();

    @Override
    public boolean hasNext() {
      return this.current != this.size;
    }

    @Override
    public List<Object> next() {
      if (this.current >= this.size) {
        throw new NoSuchElementException();
      }
      int rowSize = ColumnarTable.this.headers.size();
      List<Object> r = new ArrayList<>(rowSize);
      for (int i = 0; i < rowSize; i++) {
        r.add(ColumnarTable.this.values.get(i).get(this.current));
      }
      this.current++;
      return r;
    }
  }
}
