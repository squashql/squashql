package io.squashql.query.monitoring;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class PreparePlanTimings {
  public long total;
  public Map<String, Long> detail = new HashMap<>();
}
