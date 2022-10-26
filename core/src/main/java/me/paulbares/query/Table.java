package me.paulbares.query;

import me.paulbares.query.dictionary.ObjectArrayDictionary;
import me.paulbares.store.TypedField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface Table extends Iterable<List<Object>> {

  ObjectArrayDictionary pointDictionary();

  List<TypedField> headers();

  void addAggregates(TypedField field, Measure measure, List<Object> values);

  default List<Object> getColumn(int columnIndex) {
    List<Object> elements = new ArrayList<>();
    for (List<Object> objects : this) {
      elements.add(objects.get(columnIndex));
    }
    return elements;
  }

  default List<Object> getColumnValues(String column) {
    return getColumn(columnIndex(column));
  }

  default List<Object> getAggregateValues(Measure measure) {
    int index = measures().indexOf(measure);
    if (index < 0) {
      throw new IllegalArgumentException("no aggregate values for " + measure);
    }
    return getColumn(measureIndices()[index]);
  }

  default TypedField getField(Measure measure) {
    int index = measures().indexOf(measure);
    if (index < 0) {
      throw new IllegalArgumentException("no aggregate values for " + measure);
    }
    return headers().get(measureIndices()[index]);
  }

  default TypedField getField(String column) {
    return headers().get(columnIndex(column));
  }

  List<Measure> measures();

  int[] measureIndices();

  int[] columnIndices();

  default int columnIndex(String column) {
    int index = -1, i = 0;
    for (TypedField header : headers()) {
      if (header.name().equals(column)) {
        index = i;
        break;
      }
      i++;
    }
    if (index < 0) {
      throw new IllegalArgumentException("no column named " + column);
    }
    return index;
  }

  default int index(TypedField field) {
    int index = -1, i = 0;
    for (TypedField header : headers()) {
      if (header.equals(field)) {
        index = i;
        break;
      }
      i++;
    }
    if (index < 0) {
      throw new IllegalArgumentException("no field named " + field);
    }
    return index;
  }

  default boolean isMeasure(int index) {
    return Arrays.binarySearch(measureIndices(), index) >= 0;
  }

  /**
   * Returns the number of rows in the table.
   *
   * @return the number of rows
   */
  long count();

  void show(int numRows);

  default void show() {
    show(Integer.MAX_VALUE);
  }
}
