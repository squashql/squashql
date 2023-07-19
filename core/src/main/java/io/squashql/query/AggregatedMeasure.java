package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.store.TypedField;
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
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    String sql;
    Function<String, TypedField> fp = MeasureUtils.withFallback(fieldProvider, Number.class);
    TypedField f = fp.apply(this.field);
    String fieldFullName = queryRewriter.getFieldFullName(f);
    if (this.criteria != null) {
      String conditionSt = SQLTranslator.toSql(fp, this.criteria, queryRewriter);
      sql = this.aggregationFunction + "(case when " + conditionSt + " then " + fieldFullName + " end)";
    } else if (this.field.equals("*")) {
      sql = this.aggregationFunction + "(*)";
    } else {
      sql = this.aggregationFunction + "(" + fieldFullName + ")";
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
