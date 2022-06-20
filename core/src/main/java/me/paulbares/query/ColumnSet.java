package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.List;

/**
 * Marker interface.
 */
public interface ColumnSet {

  List<String> getColumnsForPrefetching();

  List<Field> getNewColumns();
}
