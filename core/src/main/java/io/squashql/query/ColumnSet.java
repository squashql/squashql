package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface ColumnSet {

  @JsonIgnore
  List<NamedField> getColumnsForPrefetching();

  @JsonIgnore
  List<NamedField> getNewColumns();

  @JsonIgnore
  ColumnSetKey getColumnSetKey();
}
