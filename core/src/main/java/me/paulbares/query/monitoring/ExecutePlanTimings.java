package me.paulbares.query.monitoring;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ExecutePlanTimings {
  public long total;
  public Map<String, Long> detail = new HashMap<>();
}