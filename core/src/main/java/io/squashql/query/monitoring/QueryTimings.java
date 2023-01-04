package io.squashql.query.monitoring;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@Getter
public class QueryTimings {
  public long total;
  public long prefetch;
  public long bucket;
  public long order;
  public ExecutePlanTimings execute = new ExecutePlanTimings();
  public PreparePlanTimings prepare = new PreparePlanTimings();
}
