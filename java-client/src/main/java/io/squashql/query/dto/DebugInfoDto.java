package io.squashql.query.dto;

import lombok.*;
import io.squashql.query.monitoring.QueryTimings;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class DebugInfoDto {

  public CacheStatsDto cache;
  public QueryTimings timings;
}
