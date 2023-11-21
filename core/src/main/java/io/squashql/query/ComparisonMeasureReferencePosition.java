package io.squashql.query;

import io.squashql.query.dto.Period;
import lombok.*;

import java.util.List;
import java.util.Map;

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
  @Getter
  public Measure measure;
  public ColumnSetKey columnSetKey;
  public Map<Field, String> referencePosition;
  public Period period;
  public List<Field> ancestors;

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<Field, String> referencePosition,
                                            @NonNull Period period) {
    this(alias, comparisonMethod, measure, referencePosition, period, null, null);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<Field, String> referencePosition,
                                            @NonNull ColumnSetKey columnSetKey) {
    this(alias, comparisonMethod, measure, referencePosition, null, columnSetKey, null);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull List<Field> ancestors) {
    this(alias, comparisonMethod, measure, null, null, null, ancestors);
  }

  private ComparisonMeasureReferencePosition(@NonNull String alias,
                                             @NonNull ComparisonMethod comparisonMethod,
                                             @NonNull Measure measure,
                                             Map<Field, String> referencePosition,
                                             Period period,
                                             ColumnSetKey columnSetKey,
                                             List<Field> ancestors) {
    this.alias = alias;
    this.comparisonMethod = comparisonMethod;
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
