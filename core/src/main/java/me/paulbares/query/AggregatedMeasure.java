package me.paulbares.query;

import me.paulbares.query.database.SQLTranslator;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.store.Field;

import java.util.Objects;
import java.util.function.Function;

import static me.paulbares.query.database.SqlUtils.escape;

public class AggregatedMeasure implements Measure {

  public String alias;
  public String field;
  public String aggregationFunction;
  public String conditionField;
  public ConditionDto conditionDto;

  /**
   * For jackson.
   */
  public AggregatedMeasure() {
  }

  public AggregatedMeasure(String field, String aggregationFunction) {
    this(null, field, aggregationFunction, null, null);
  }

  public AggregatedMeasure(String alias, String field, String aggregationFunction) {
    this(alias, field, aggregationFunction, null, null);
  }

  public AggregatedMeasure(String alias, String field, String aggregationFunction, String conditionField, ConditionDto conditionDto) {
    this.field = Objects.requireNonNull(field);
    this.aggregationFunction = Objects.requireNonNull(aggregationFunction);
    this.conditionField = conditionField;
    this.conditionDto = conditionDto;
    if (alias == null) {
      if (this.conditionDto != null) {
        String conditionSt = SQLTranslator.toSql(new Field(this.conditionField, String.class), this.conditionDto);
        this.alias = this.aggregationFunction + "If(" + this.field + ", " + conditionSt + ")";
      } else {
        this.alias = this.aggregationFunction + "(" + this.field + ")";
      }
    } else {
      this.alias = alias;
    }
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider) {
    String sql;
    if (this.conditionDto != null) {
      String conditionSt = SQLTranslator.toSql(fieldProvider.apply(this.conditionField), this.conditionDto);
      sql = this.aggregationFunction + "(case when " + conditionSt + " then " + this.field + " end)";
    } else {
      sql = this.aggregationFunction + "(" + (this.field.equals("*") ? this.field : escape(this.field)) + ")";
    }
    return sql + " as " + escape(this.alias);
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AggregatedMeasure that = (AggregatedMeasure) o;
    return Objects.equals(this.alias, that.alias) && Objects.equals(this.field, that.field) && Objects.equals(this.aggregationFunction, that.aggregationFunction) && Objects.equals(this.conditionField, that.conditionField) && Objects.equals(this.conditionDto, that.conditionDto);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.alias, this.field, this.aggregationFunction, this.conditionField, this.conditionDto);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
            '{' +
            "alias='" + alias + '\'' +
            ", field='" + field + '\'' +
            ", aggregationFunction='" + aggregationFunction + '\'' +
            ", conditionField='" + conditionField + '\'' +
            ", conditionDto=" + conditionDto +
            '}';
  }
}
