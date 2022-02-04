package me.paulbares.query.dto;

import me.paulbares.jackson.JacksonUtil;

import java.util.ArrayList;
import java.util.Arrays;
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

  public ScenarioGroupingQueryDto table(TableDto table) {
    this.table = table;
    return this;
  }

  public ScenarioGroupingQueryDto groups(Map<String, List<String>> groups) {
    this.groups = groups;
    return this;
  }

  public ScenarioGroupingQueryDto defineNewGroup(String groupName, String firstScenario, String... others) {
    List<String> l = new ArrayList<>();
    l.add(firstScenario);
    if (others != null) {
      l.addAll(Arrays.asList(others));
    }
    this.groups.put(groupName, l);
    return this;
  }

  @Override
  public String toString() {
    return "ScenarioGroupingQueryDto{" +
            "groups=" + groups +
            ", comparisons=" + comparisons +
            ", table=" + table +
            '}';
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }
}
