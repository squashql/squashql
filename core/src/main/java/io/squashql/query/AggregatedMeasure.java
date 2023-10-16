package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.type.TypedField;
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
  public String sqlExpression(Function<Field, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    String fieldExpression = this.field.sqlExpression(fieldProvider, queryRewriter);
    String valuesToAggregate;
    if (this.criteria != null) {
      Function<Field, TypedField> fp = MeasureUtils.withFallback(fieldProvider, Number.class);
      String conditionSt = SQLTranslator.toSql(fp, this.criteria, queryRewriter);
      valuesToAggregate = "case when " + conditionSt + " then " + fieldExpression + " end";
    } else {
      valuesToAggregate = fieldExpression;
    }
    if (distinct) {
      valuesToAggregate = "distinct(" + valuesToAggregate + ")";
    }
    String sql = this.aggregationFunction + "(" + valuesToAggregate + ")";
    return withAlias ? SqlUtils.appendAlias(sql, queryRewriter, this.alias) : sql;
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
