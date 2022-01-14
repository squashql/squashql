package me.paulbares.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ScenarioGroupingQuery {

  public Map<String, List<String>> groups = new LinkedHashMap<>();

  public List<Measure> measures = new ArrayList<>();

  public ComparisonMethod comparisonMethod;

  public ScenarioGroupingQuery addAggregatedMeasure(String field, String agg) {
    this.measures.add(new AggregatedMeasure(field, agg));
    return this;
  }

  public ScenarioGroupingQuery addExpressionMeasure(String alias, String expression) {
    this.measures.add(new ExpressionMeasure(alias, expression));
    return this;
  }

  public ScenarioGroupingQuery groups(Map<String, List<String>> groups) {
    this.groups = groups;
    return this;
  }

  public ScenarioGroupingQuery comparisonMethod(ComparisonMethod comparisonMethod) {
    this.comparisonMethod = comparisonMethod;
    return this;
  }
}
