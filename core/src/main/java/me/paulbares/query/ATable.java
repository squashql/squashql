package me.paulbares.query;

import com.google.common.base.Suppliers;
import me.paulbares.query.dictionary.ObjectArrayDictionary;
import me.paulbares.store.Field;

import java.util.List;
import java.util.function.Supplier;

public abstract class ATable implements Table {

  protected final List<Field> headers;
  protected final List<Measure> measures;
  protected final int[] columnsIndices;
  protected int[] measureIndices;

  protected final Supplier<ObjectArrayDictionary> pointDictionary;

  public ATable(List<Field> headers,
                List<Measure> measures,
                int[] measureIndices,
                int[] columnsIndices) {
    this.headers = headers;
    this.measures = measures;
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
}
