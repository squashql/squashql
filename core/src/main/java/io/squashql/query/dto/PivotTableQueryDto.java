package io.squashql.query.dto;

import io.squashql.query.field.Field;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class PivotTableQueryDto {

  public QueryDto query;
  public List<Field> rows;
  public List<Field> columns;
  public List<Field> hiddenTotals;

  public PivotTableQueryDto(QueryDto query, List<Field> rows, List<Field> columns) {
    this(query, rows, columns, Collections.emptyList());
  }
}
