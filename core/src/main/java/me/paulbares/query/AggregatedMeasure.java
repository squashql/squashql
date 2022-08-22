package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.store.Field;

import java.util.function.Function;

import static me.paulbares.query.database.SqlUtils.escape;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class AggregatedMeasure implements Measure {

  public String alias;
  public String expression;
  public String field;
  public String aggregationFunction;
  public String conditionField;
  public ConditionDto conditionDto;

  public AggregatedMeasure(String field, String aggregationFunction) {
    this(null, field, aggregationFunction, null, null);
  }

  public AggregatedMeasure(String alias, String field, String aggregationFunction) {
    this(alias, field, aggregationFunction, null, null);
  }

  public AggregatedMeasure(String field, String aggregationFunction, String conditionField, ConditionDto conditionDto) {
    this(null, field, aggregationFunction, conditionField, conditionDto);
  }

  public AggregatedMeasure(String alias, @NonNull String field, @NonNull String aggregationFunction, String conditionField, ConditionDto conditionDto) {
    this.field = field;
    this.aggregationFunction = aggregationFunction;
    this.conditionField = conditionField;
    this.conditionDto = conditionDto;
    this.expression = MeasureUtils.createExpression(this);
    this.alias = alias == null ? this.expression : alias;
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
    String sql;
    if (this.conditionDto != null) {
      String conditionSt = SQLTranslator.toSql(fieldProvider.apply(this.conditionField), this.conditionDto);
      sql = this.aggregationFunction + "(case when " + conditionSt + " then " + this.field + " end)";
    } else {
      sql = this.aggregationFunction + "(" + (this.field.equals("*") ? this.field : escape(this.field)) + ")";
    }
    return sql + " as " + queryRewriter.measureAlias(escape(this.alias), this);
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
