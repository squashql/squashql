package io.squashql.query;

import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class PartialComparisonMeasureReferencePosition implements PartialMeasure {

  public String alias;
  @With
  public String expression;
  @Getter
  public ComparisonMethod comparisonMethod;
  @Getter
  public Measure measure;
  public Axis axis;
  public boolean grandTotalAlongAncestors;

  public PartialComparisonMeasureReferencePosition(String alias,
                                                   ComparisonMethod comparisonMethod,
                                                   Measure measure,
                                                   Axis axis) {
    this(alias, comparisonMethod, measure, axis, false);
  }

  private PartialComparisonMeasureReferencePosition(String alias,
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
}
