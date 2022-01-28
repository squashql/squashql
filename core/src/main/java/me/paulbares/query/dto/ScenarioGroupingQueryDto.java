package me.paulbares.query.dto;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ScenarioGroupingQueryDto {

  public Map<String, List<String>> groups = new LinkedHashMap<>();

  public List<ScenarioComparisonDto> comparisons = new ArrayList<>();

  public TableDto table;

  public ScenarioGroupingQueryDto addScenarioComparison(ScenarioComparisonDto comparison) {
    this.comparisons.add(comparison);
    return this;
  }

  public ScenarioGroupingQueryDto table(String tableName) {
    this.table = new TableDto(tableName);
    return this;
  }

  public ScenarioGroupingQueryDto groups(Map<String, List<String>> groups) {
    this.groups = groups;
    return this;
  }
}
