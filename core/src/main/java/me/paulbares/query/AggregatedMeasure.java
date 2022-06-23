package me.paulbares.query;

import me.paulbares.query.dto.ConditionDto;
import me.paulbares.store.Field;

import java.util.Objects;
import java.util.function.Function;

import static me.paulbares.query.SqlUtils.escape;

public class AggregatedMeasure implements Measure {

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
    this(field, aggregationFunction, null, null);
  }

  public AggregatedMeasure(String field, String aggregationFunction, String conditionField, ConditionDto conditionDto) {
    this.field = Objects.requireNonNull(field);
    this.aggregationFunction = Objects.requireNonNull(aggregationFunction);
    this.conditionField = conditionField;
    this.conditionDto = conditionDto;
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider) {
    if (this.conditionDto != null) {
      String conditionSt = SQLTranslator.toSql(fieldProvider.apply(this.conditionField), this.conditionDto);
      return this.aggregationFunction + "(case when " + conditionSt + " then " + this.field + " end)";
    } else {
      return this.aggregationFunction + "(" + (this.field.equals("*") ? this.field : escape(this.field)) + ")";
    }
  }

  @Override
  public String alias() {
    if (this.conditionDto != null) {
      String conditionSt = SQLTranslator.toSql(new Field(this.conditionField, String.class), this.conditionDto);
      return this.aggregationFunction + "If(" + this.field + ", " + conditionSt + ")";
    } else {
      return this.aggregationFunction + "(" + this.field + ")";
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AggregatedMeasure that = (AggregatedMeasure) o;
    return Objects.equals(this.field, that.field) && Objects.equals(this.aggregationFunction, that.aggregationFunction) && Objects.equals(this.conditionField, that.conditionField) && Objects.equals(this.conditionDto, that.conditionDto);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.field, this.aggregationFunction, this.conditionField, this.conditionDto);
  }

  @Override
  public String toString() {
    return "AggregatedMeasure{" +
            "field='" + field + '\'' +
            ", aggregationFunction='" + aggregationFunction + '\'' +
            ", conditionField='" + conditionField + '\'' +
            ", conditionDto=" + conditionDto +
            '}';
  }
}
