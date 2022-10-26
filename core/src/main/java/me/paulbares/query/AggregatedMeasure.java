package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.query.database.SqlUtils;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.store.TypedField;

import java.util.function.Function;

import static me.paulbares.query.database.SqlUtils.escape;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@Slf4j
public class AggregatedMeasure implements Measure {

  public String alias;
  public String expression;
  public String field;
  public String aggregationFunction;
  public String conditionField;
  public ConditionDto conditionDto;

  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction) {
    this(alias, field, aggregationFunction, null, null);
  }

  public AggregatedMeasure(@NonNull String alias, @NonNull String field, @NonNull String aggregationFunction, String conditionField, ConditionDto conditionDto) {
    this.alias = alias;
    this.field = field;
    this.aggregationFunction = aggregationFunction;
    this.conditionField = conditionField;
    this.conditionDto = conditionDto;
  }

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    String sql;
    if (this.conditionDto != null) {
      TypedField f;
      try {
        f = fieldProvider.apply(this.conditionField);
      } catch (Exception e) {
        // This can happen if the using a "field" coming from the calculation of a subquery. Since the field provider
        // contains only "raw" fields, it will throw an exception.
        log.info("Cannot find field " + this.conditionField + " with default field provider, fallback to default type: " + Number.class.getSimpleName());
        f = new TypedField(this.conditionField, Number.class);
      }
      String conditionSt = SQLTranslator.toSql(f, this.conditionDto);
      sql = this.aggregationFunction + "(case when " + conditionSt + " then " + this.field + " end)";
    } else {
      sql = this.aggregationFunction + "(" + (this.field.equals("*") ? this.field : escape(this.field)) + ")";
    }
    return withAlias ? SqlUtils.appendAlias(sql, queryRewriter, this.alias, this) : sql;
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
