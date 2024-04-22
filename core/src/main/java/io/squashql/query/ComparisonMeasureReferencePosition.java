package io.squashql.query;

import io.squashql.query.dto.Period;
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
  public boolean clearFilters;
  @Getter
  public Measure measure;
  public ColumnSetKey columnSetKey;
  public List<?> elements;
  public Map<Field, String> referencePosition;
  public Period period;
  public List<Field> ancestors;
  public boolean grandTotalAlongAncestors;

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<Field, String> referencePosition,
                                            @NonNull Period period) {
    this(alias, comparisonMethod, null, true, measure, referencePosition, period, null, null, null, false);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<Field, String> referencePosition,
                                            @NonNull ColumnSetKey columnSetKey) {
    this(alias, comparisonMethod, null, true, measure, referencePosition, null, columnSetKey, null, null, false);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<Field, String> referencePosition) {
    this(alias, comparisonMethod, null, true, measure, referencePosition, null, null, null, null, false);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<Field, String> referencePosition,
                                            @NonNull List<?> elements) {
    this(alias, comparisonMethod, null, true, measure, referencePosition, null, null, elements, null, false);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull List<Field> ancestors) {
    this(alias, comparisonMethod, null, false, measure, null, null, null, null, ancestors, false); // FIXME
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull List<Field> ancestors,
                                            boolean grandTotalAlongAncestors) {
    this(alias, comparisonMethod, null, false, measure, null, null, null, null, ancestors, grandTotalAlongAncestors); // FIXME
  }

  /**
   * For internal use.
   */
  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull BiFunction<Object, Object, Object> comparisonOperator,
                                            @NonNull Measure measure,
                                            @NonNull List<Field> ancestors) {
    this(alias, null, comparisonOperator, false, measure, null, null, null, null, ancestors, false);
  }

  private ComparisonMeasureReferencePosition(String alias,
                                             ComparisonMethod comparisonMethod,
                                             BiFunction<Object, Object, Object> comparisonOperator,
                                             boolean clearFilters,
                                             Measure measure,
                                             Map<Field, String> referencePosition,
                                             Period period,
                                             ColumnSetKey columnSetKey,
                                             List<?> elements,
                                             List<Field> ancestors,
                                             boolean grandTotalAlongAncestors) {
    this.alias = alias;
    this.comparisonMethod = comparisonMethod;
    this.comparisonOperator = comparisonOperator;
    this.clearFilters = clearFilters;
    this.measure = measure;
    this.referencePosition = referencePosition;
    this.period = period;
    this.columnSetKey = columnSetKey;
    this.elements = elements;
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
