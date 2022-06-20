package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.List;

public interface Table extends Iterable<List<Object>> {

  List<Field> headers();

  default List<Object> aggregates(int columnIndex) {
    List<Object> aggregates = new ArrayList<>();
    for (List<Object> objects : this) {
      aggregates.add(objects.get(columnIndex));
    }
    return aggregates;
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
