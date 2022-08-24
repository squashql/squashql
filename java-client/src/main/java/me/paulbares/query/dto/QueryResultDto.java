package me.paulbares.query.dto;

import lombok.*;

import java.util.List;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor// For Jackson
@Getter
public class QueryResultDto {

  public SimpleTableDto table;
  public List<MetadataItem> metadata;
  public DebugInfoDto debug;

  public QueryResultDto(SimpleTableDto table, List<MetadataItem> metadata, DebugInfoDto debug) {
    this.table = table;
    this.metadata = metadata;
    this.debug = debug;
  }
}
