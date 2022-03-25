package me.paulbares.query.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.jackson.deserializer.ContextValueDeserializer;
import me.paulbares.query.context.ContextValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ScenarioGroupingQueryDto {

  public Map<String, List<String>> groups = new LinkedHashMap<>();

  public List<ScenarioComparisonDto> comparisons = new ArrayList<>();

  public TableDto table;

  @JsonDeserialize(contentUsing = ContextValueDeserializer.class)
  public Map<String, ContextValue> context = new HashMap<>();

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

  public ScenarioGroupingQueryDto context(String key, ContextValue value) {
    this.context.put(key, value);
    return this;
  }

  @Override
  public String toString() {
    return "ScenarioGroupingQueryDto{" +
            "groups=" + groups +
            ", comparisons=" + comparisons +
            ", table=" + table +
            ", context=" + context +
            '}';
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }
}
