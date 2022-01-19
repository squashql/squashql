package me.paulbares.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ScenarioGroupingQuery {

  public Map<String, List<String>> groups = new LinkedHashMap<>();

  public List<ScenarioComparison> comparisons = new ArrayList<>();

  public ScenarioGroupingQuery addScenarioComparison(ScenarioComparison comparison) {
    this.comparisons.add(comparison);
    return this;
  }

  public ScenarioGroupingQuery groups(Map<String, List<String>> groups) {
    this.groups = groups;
    return this;
  }
}
