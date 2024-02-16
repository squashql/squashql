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
public class QueryResultDto {

  public List<String> columns;
  public List<Map<String, Object>> cells;
  public List<MetadataItem> metadata;
  public DebugInfoDto debug;
}
