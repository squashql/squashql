package io.squashql.query.dto;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import io.squashql.query.monitoring.QueryTimings;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class DebugInfoDto {

  public CacheStatsDto cache;
  public QueryTimings timings;

  public DebugInfoDto(CacheStatsDto cache, QueryTimings timings) {
    this.cache = cache;
    this.timings = timings;
  }
}
