package io.squashql.query.dto;

import lombok.*;

import java.util.List;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor// For Jackson
@AllArgsConstructor
@Getter
public class PivotTableQueryResultDto {

  public QueryResultDto queryResult;
  public List<String> rows;
  public List<String> columns;
  public List<String> values;
}
