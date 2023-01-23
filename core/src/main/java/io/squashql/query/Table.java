package io.squashql.query;

import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.store.Field;
import io.squashql.util.SquashQLArrays;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

public interface Table extends Iterable<List<Object>> {

  ObjectArrayDictionary pointDictionary();

  List<Field> headers();

  void addAggregates(Field field, Measure measure, List<Object> values);

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
    int index = headers().indexOf(getField(measure));
    if (index < 0) {
      throw new IllegalArgumentException("no aggregate values for " + measure);
    }
    return getColumn(index);
  }

  default Field getField(Measure measure) {
    return headers().stream().filter(header -> header.name().equals(measure.alias()))
            .findAny().orElseThrow(() -> new IllegalArgumentException("no field for " + measure));
  }

  default Field getField(String column) {
    return headers().get(columnIndex(column));
  }

  List<Measure> measures();

  default int columnIndex(String column) {
    int index = -1, i = 0;
    for (Field header : headers()) {
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

  default int index(Field field) {
    int index = -1, i = 0;
    for (Field header : headers()) {
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

  default boolean isMeasure(Field field) {
    return measures().stream().map(Measure::alias).anyMatch(measureName -> measureName.equals(field.name()));
  }

  /**
   * Retrieve the column values for a given row index.
   *
   * @param rowIndex
   *         the index of the row to retrieve
   * @return the list of column values for this row or null if the rowIndex is outside the table
   */
  default List<Object> getFactRow(int rowIndex) {
    if (rowIndex >= count()) {
      return null;
    }
    List<Object> result = new ArrayList<>();
    headers().forEach(header -> {
      if (!isMeasure(header)) {
        result.add(getColumnValues(header.name()).get(rowIndex));
      }
    });
    return result;
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
