package io.squashql.query;

import io.squashql.query.measure.visitor.MeasureVisitor;
import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class ComparisonMeasureGrandTotal implements Measure {

  public String alias;
  @With
  public String expression;
  @Getter
  public ComparisonMethod comparisonMethod;
  public boolean clearFilters;
  @Getter
  public Measure measure;

  public ComparisonMeasureGrandTotal(@NonNull String alias,
                                     @NonNull ComparisonMethod comparisonMethod,
                                     @NonNull Measure measure) {
    this(alias, null, comparisonMethod, true, measure);
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
