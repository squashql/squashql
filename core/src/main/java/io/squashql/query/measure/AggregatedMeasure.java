package io.squashql.query.measure;

import io.squashql.query.field.Field;
import io.squashql.query.measure.visitor.MeasureVisitor;
import io.squashql.query.field.TableField;
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
  public boolean distinct;
  public CriteriaDto criteria;

  public AggregatedMeasure(@NonNull String alias, @NonNull Field field, @NonNull String aggregationFunction, boolean distinct, CriteriaDto criteria) {
    this.alias = alias;
    this.field = field;
    this.aggregationFunction = aggregationFunction;
    this.distinct = distinct;
    this.criteria = criteria;
  }
  public AggregatedMeasure(@NonNull String alias, @NonNull Field field, @NonNull String aggregationFunction, boolean distinct) {
    this(alias, field, aggregationFunction, distinct, null);
  }
  public AggregatedMeasure(@NonNull String alias, @NonNull Field field, @NonNull String aggregationFunction, CriteriaDto criteria) {
    this(alias, field, aggregationFunction, false, criteria);
  }

  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction, boolean distinct, CriteriaDto criteria) {
    this(alias, new TableField(field), aggregationFunction, distinct, criteria);
  }
  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction, boolean distinct) {
    this(alias, field, aggregationFunction, distinct, null);
  }
  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction, CriteriaDto criteria) {
    this(alias, field, aggregationFunction, false, criteria);
  }

  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction) {
    this(alias, field, aggregationFunction, null);
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
