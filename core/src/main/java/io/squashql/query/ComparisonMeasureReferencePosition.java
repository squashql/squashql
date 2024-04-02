package io.squashql.query;

import io.squashql.query.dto.Period;
import io.squashql.query.field.Field;
import io.squashql.query.measure.Measure;
import io.squashql.query.measure.visitor.MeasureVisitor;
import lombok.*;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class ComparisonMeasureReferencePosition implements Measure {

  public String alias;
  @With
  public String expression;
  @Getter
  public ComparisonMethod comparisonMethod;
  public BiFunction<Object, Object, Object> comparisonOperator;
  @Getter
  public Measure measure;
  public ColumnSetKey columnSetKey;
  public Map<Field, String> referencePosition;
  public Period period;
  public List<Field> ancestors;
  public boolean grandTotalAlongAncestors;

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<Field, String> referencePosition,
                                            @NonNull Period period) {
    this(alias, comparisonMethod, null, measure, referencePosition, period, null, null, false);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<Field, String> referencePosition,
                                            @NonNull ColumnSetKey columnSetKey) {
    this(alias, comparisonMethod, null, measure, referencePosition, null, columnSetKey, null, false);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull List<Field> ancestors) {
    this(alias, comparisonMethod, null, measure, null, null, null, ancestors, false);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull List<Field> ancestors,
                                            boolean grandTotalAlongAncestors) {
    this(alias, comparisonMethod, null, measure, null, null, null, ancestors, grandTotalAlongAncestors);
  }

  /**
   * For internal use.
   */
  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull BiFunction<Object, Object, Object> comparisonOperator,
                                            @NonNull Measure measure,
                                            @NonNull List<Field> ancestors) {
    this(alias, null, comparisonOperator, measure, null, null, null, ancestors, false);
  }

  private ComparisonMeasureReferencePosition(String alias,
                                             ComparisonMethod comparisonMethod,
                                             BiFunction<Object, Object, Object> comparisonOperator,
                                             Measure measure,
                                             Map<Field, String> referencePosition,
                                             Period period,
                                             ColumnSetKey columnSetKey,
                                             List<Field> ancestors,
                                             boolean grandTotalAlongAncestors) {
    this.alias = alias;
    this.comparisonMethod = comparisonMethod;
    this.comparisonOperator = comparisonOperator;
    this.measure = measure;
    this.referencePosition = referencePosition;
    this.period = period;
    this.columnSetKey = columnSetKey;
    this.ancestors = ancestors;
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
