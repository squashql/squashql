package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.List;

public interface Table extends Iterable<List<Object>> {

  List<Field> headers();

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
