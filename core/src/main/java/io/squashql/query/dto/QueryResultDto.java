package io.squashql.query.dto;

import lombok.*;

import java.util.List;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor// For Jackson
@AllArgsConstructor
@Getter
public class QueryResultDto {

  public SimpleTableDto table;
  public List<MetadataItem> metadata;
  public DebugInfoDto debug;
}
