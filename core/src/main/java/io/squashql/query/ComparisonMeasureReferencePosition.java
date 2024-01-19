package io.squashql.query;

import io.squashql.query.dto.Period;
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
  public Map<NamedField, String> referencePosition;
  public Period period;
  public List<NamedField> ancestors;

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<NamedField, String> referencePosition,
                                            @NonNull Period period) {
    this(alias, comparisonMethod, null, measure, referencePosition, period, null, null);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<NamedField, String> referencePosition,
                                            @NonNull ColumnSetKey columnSetKey) {
    this(alias, comparisonMethod, null, measure, referencePosition, null, columnSetKey, null);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull List<NamedField> ancestors) {
    this(alias, comparisonMethod, null, measure, null, null, null, ancestors);
  }

  /**
   * For internal use.
   */
  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull BiFunction<Object, Object, Object> comparisonOperator,
                                            @NonNull Measure measure,
                                            @NonNull List<NamedField> ancestors) {
    this(alias, null, comparisonOperator, measure, null, null, null, ancestors);
  }

  private ComparisonMeasureReferencePosition(String alias,
                                             ComparisonMethod comparisonMethod,
                                             BiFunction<Object, Object, Object> comparisonOperator,
                                             Measure measure,
                                             Map<NamedField, String> referencePosition,
                                             Period period,
                                             ColumnSetKey columnSetKey,
                                             List<NamedField> ancestors) {
    this.alias = alias;
    this.comparisonMethod = comparisonMethod;
    this.comparisonOperator = comparisonOperator;
    this.measure = measure;
    this.referencePosition = referencePosition;
    this.period = period;
    this.columnSetKey = columnSetKey;
    this.ancestors = ancestors;
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
