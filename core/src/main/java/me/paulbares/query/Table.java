package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.List;

public interface Table extends Iterable<List<Object>> {

  List<Field> fields();

  /**
   * Returns the number of rows in the table.
   * @return the number of rows
   */
  long count();
}
