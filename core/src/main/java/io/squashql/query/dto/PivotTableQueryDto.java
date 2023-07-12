package io.squashql.query.dto;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class PivotTableQueryDto {

  public QueryDto query;
  public List<String> rows;
  public List<String> columns;
}
