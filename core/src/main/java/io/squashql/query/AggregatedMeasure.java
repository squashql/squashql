package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.type.TypedField;
import lombok.*;

import java.util.function.Function;

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
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    String sql;
    String fieldExpression = this.field.sqlExpression(fieldProvider, queryRewriter);
    if (this.criteria != null) {
      Function<String, TypedField> fp = MeasureUtils.withFallback(fieldProvider, Number.class);
      String conditionSt = SQLTranslator.toSql(fp, this.criteria, queryRewriter);
      sql = this.aggregationFunction + "(case when " + conditionSt + " then " + fieldExpression + " end)";
    } else {
      sql = this.aggregationFunction + "(" + fieldExpression + ")";
    }
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
