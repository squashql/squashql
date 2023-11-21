package io.squashql.query;

import io.squashql.query.dto.CriteriaDto;
import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class AggregatedMeasure implements BasicMeasure {

  public String alias;
  @With
  public String expression;
  public Field field;
  public String aggregationFunction;
  public CriteriaDto criteria;

  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction) {
    this(alias, field, aggregationFunction, null);
  }

  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction, CriteriaDto criteria) {
    this(alias, new TableField(field), aggregationFunction, criteria);
  }

  public AggregatedMeasure(@NonNull String alias, @NonNull Field field, @NonNull String aggregationFunction, CriteriaDto criteria) {
    this.alias = alias;
    this.field = field;
    this.aggregationFunction = aggregationFunction;
    this.criteria = criteria;
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
