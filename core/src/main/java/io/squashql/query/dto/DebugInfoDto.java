package io.squashql.query.dto;

import io.squashql.query.monitoring.QueryTimings;
import lombok.*;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class DebugInfoDto {

  public CacheStatsDto cache;
  public QueryTimings timings;
}
