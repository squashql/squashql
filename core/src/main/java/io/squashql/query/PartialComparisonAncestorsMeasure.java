package io.squashql.query;

import io.squashql.query.measure.visitor.MeasureVisitor;
import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor // TODO rename
public class PartialComparisonAncestorsMeasure implements Measure {

  public String alias;
  @With
  public String expression;
  @Getter
  public ComparisonMethod comparisonMethod;
  @Getter
  public Measure measure;
  public Axis axis;
  public boolean grandTotalAlongAncestors;

  public PartialComparisonAncestorsMeasure(String alias,
                                           ComparisonMethod comparisonMethod,
                                           Measure measure,
                                           Axis axis,
                                           boolean grandTotalAlongAncestors) {
    this.alias = alias;
    this.comparisonMethod = comparisonMethod;
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
