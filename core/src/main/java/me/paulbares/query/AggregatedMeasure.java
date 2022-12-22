package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.query.database.SqlUtils;
import me.paulbares.query.dto.CriteriaDto;
import me.paulbares.store.Field;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@Slf4j
public class AggregatedMeasure implements Measure {

  public String alias;
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
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
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
  public void setExpression(String expression) {
    this.expression = expression;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
