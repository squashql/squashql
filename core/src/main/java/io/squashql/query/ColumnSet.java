package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.store.Field;

import java.util.List;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface ColumnSet {

  @JsonIgnore
  List<String> getColumnsForPrefetching();

  @JsonIgnore
  List<Field> getNewColumns();

  @JsonIgnore
  ColumnSetKey getColumnSetKey();
}
