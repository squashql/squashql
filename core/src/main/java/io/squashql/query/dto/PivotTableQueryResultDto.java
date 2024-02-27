package io.squashql.query.dto;

import lombok.*;

import java.util.List;
import java.util.Map;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor// For Jackson
@AllArgsConstructor
@Getter
public class PivotTableQueryResultDto {

  public List<Map<String, Object>> cells;
  public List<String> rows;
  public List<String> columns;
  public List<String> values;
}
