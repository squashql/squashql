package io.squashql.query;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.Period;
import io.squashql.query.dto.QueryDto.KeyFieldDeserializer;
import io.squashql.query.dto.QueryDto.KeyFieldSerializer;
import io.squashql.type.TypedField;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.With;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class ComparisonMeasureReferencePosition implements Measure {

  public String alias;
  @With
  public String expression;
  public ComparisonMethod comparisonMethod;
  public Measure measure;
  public ColumnSetKey columnSetKey;
  @JsonSerialize(keyUsing = KeyFieldSerializer.class)
  @JsonDeserialize(keyUsing = KeyFieldDeserializer.class)
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

  public Measure getMeasure() {
    return this.measure;
  }

  public ComparisonMethod getComparisonMethod() {
    return this.comparisonMethod;
  }

  @Override
  public String sqlExpression(Function<Field, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException();
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
