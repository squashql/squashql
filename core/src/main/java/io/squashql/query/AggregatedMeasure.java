package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.store.FieldWithStore;
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
  public String field;
  public String aggregationFunction;
  public CriteriaDto criteria;

  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction) {
    this(alias, field, aggregationFunction, null);
  }

  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction, CriteriaDto criteria) {
    this.alias = alias;
    this.field = field;
    this.aggregationFunction = aggregationFunction;
    this.criteria = criteria;
  }

  @Override
  public String sqlExpression(Function<String, FieldWithStore> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    // FIXME field should belongs to the a table or subquery. Need to check?
    String sql;
    if (this.criteria != null) {
      String conditionSt = SQLTranslator.toSql(QueryExecutor.withFallback(fieldProvider, Number.class), this.criteria, queryRewriter);
      sql = this.aggregationFunction + "(case when " + conditionSt + " then " + queryRewriter.fieldName(this.field) + " end)";
    } else {
      sql = this.aggregationFunction + "(" + (this.field.equals("*") ? this.field : queryRewriter.fieldName(this.field)) + ")";
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
