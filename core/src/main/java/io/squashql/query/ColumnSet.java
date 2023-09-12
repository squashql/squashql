package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.type.TypedField;

import java.util.List;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface ColumnSet {

  @JsonIgnore
  List<String> getColumnsForPrefetching();

  @JsonIgnore
  List<TypedField> getNewColumns();

  @JsonIgnore
  ColumnSetKey getColumnSetKey();
}
