package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.List;

public interface Table extends Iterable<List<Object>> {

  List<Field> headers();

  default List<Object> getColumn(int columnIndex) {
    List<Object> elements = new ArrayList<>();
    for (List<Object> objects : this) {
      elements.add(objects.get(columnIndex));
    }
    return elements;
  }

  List<? extends Measure> measures();

  int[] measureIndices();

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
