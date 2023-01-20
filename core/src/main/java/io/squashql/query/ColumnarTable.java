package io.squashql.query;

import com.google.common.base.Suppliers;
import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.store.Field;

import java.util.*;
import java.util.function.Supplier;

public class ColumnarTable implements Table {

  protected final List<Field> headers;
  protected final List<Measure> measures;
  protected final int[] columnsIndices;
  protected int[] measureIndices;

  protected final Supplier<ObjectArrayDictionary> pointDictionary;
  protected final List<List<Object>> values;

  public ColumnarTable(List<Field> headers,
                       List<Measure> measures,
                       int[] measureIndices,
                       int[] columnsIndices,
                       List<List<Object>> values) {
    this.headers = new ArrayList<>(headers);
    this.measures = new ArrayList<>(measures);
    this.values = new ArrayList<>(values);
    this.measureIndices = measureIndices;
    this.columnsIndices = columnsIndices;
    this.pointDictionary = Suppliers.memoize(() -> createPointDictionary(this));
  }

  public static ObjectArrayDictionary createPointDictionary(Table table) {
    int[] columnIndices = table.columnIndices();
    int pointLength = columnIndices.length;
    ObjectArrayDictionary dictionary = new ObjectArrayDictionary(pointLength);
    table.forEach(row -> {
      Object[] columnValues = new Object[pointLength];
      int i = 0;
      for (int columnIndex : columnIndices) {
        columnValues[i++] = row.get(columnIndex);
      }
      dictionary.map(columnValues);
    });
    return dictionary;
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
  public List<Object> getColumn(int columnIndex) {
    return this.values.get(columnIndex);
  }

  public List<List<Object>> getColumns() {
    return this.values;
  }

  @Override
  public long count() {
    return this.values.get(0).size();
  }

  @Override
  public ObjectArrayDictionary pointDictionary() {
    return this.pointDictionary.get();
  }

  @Override
  public List<Measure> measures() {
    return this.measures;
  }

  @Override
  public int[] measureIndices() {
    return this.measureIndices;
  }

  @Override
  public int[] columnIndices() {
    return this.columnsIndices;
  }

  @Override
  public List<Field> headers() {
    return this.headers;
  }

  @Override
  public void show(int numRows) {
    System.out.println(this);
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.headers, this, f -> ((Field) f).name(), String::valueOf);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnarTable lists = (ColumnarTable) o;
    return headers.equals(lists.headers) && measures.equals(lists.measures) && Arrays.equals(columnsIndices,
            lists.columnsIndices) && Arrays.equals(measureIndices, lists.measureIndices) && values.equals(
            lists.values);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(headers, measures, values);
    result = 31 * result + Arrays.hashCode(columnsIndices);
    result = 31 * result + Arrays.hashCode(measureIndices);
    return result;
  }
}
