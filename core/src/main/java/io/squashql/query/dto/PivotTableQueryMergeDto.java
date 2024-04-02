package io.squashql.query.dto;

import io.squashql.query.Field;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class PivotTableQueryMergeDto {

  public QueryMergeDto query;
  public List<Field> rows;
  public List<Field> columns;
}
