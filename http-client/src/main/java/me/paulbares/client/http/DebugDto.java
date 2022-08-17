package me.paulbares.client.http;

import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.monitoring.QueryTimings;

public class DebugDto {
  public CacheStatsDto cache;
  public QueryTimings timings;
}
