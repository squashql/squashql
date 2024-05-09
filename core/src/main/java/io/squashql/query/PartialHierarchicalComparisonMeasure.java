package io.squashql.query;

import io.squashql.query.measure.visitor.MeasureVisitor;
import lombok.*;

import java.util.function.BiFunction;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class PartialHierarchicalComparisonMeasure implements Measure {

  public String alias;
  @With
  public String expression;
  @Getter
  public ComparisonMethod comparisonMethod;
  public BiFunction<Object, Object, Object> comparisonOperator;
  public boolean clearFilters;
  @Getter
  public Measure measure;
  public Axis axis;
  public boolean grandTotalAlongAncestors;

  public PartialHierarchicalComparisonMeasure(String alias,
                                              ComparisonMethod comparisonMethod,
                                              boolean clearFilters,
                                              Measure measure,
                                              Axis axis,
                                              boolean grandTotalAlongAncestors) {
    this.alias = alias;
    this.comparisonMethod = comparisonMethod;
    this.clearFilters = clearFilters;
    this.measure = measure;
    this.axis = axis;
    this.grandTotalAlongAncestors = grandTotalAlongAncestors;
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.expression;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
