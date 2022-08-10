package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;

import java.util.Map;
import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ComparisonMeasure implements Measure {

  public String alias;
  public ComparisonMethod method;
  public Measure measure;
  public String columnSet;
  public Map<String, String> referencePosition;

  public ComparisonMeasure(String alias,
                           ComparisonMethod method,
                           Measure measure,
                           String columnSet,
                           Map<String, String> referencePosition) {
    this.alias = alias == null
            ? String.format("%s(%s, %s)", method, measure.alias(), referencePosition)
            : alias;
    this.method = method;
    this.columnSet = columnSet;
    this.measure = measure;
    this.referencePosition = referencePosition;
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
    throw new IllegalStateException();
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    String alias = this.measure.alias();
    if (this.columnSet.equals(QueryDto.PERIOD)) {
      String formula = this.method.expressionGenerator.apply(alias + "(current period)", alias + "(reference period)");
      return formula + ", reference = " + this.referencePosition;
    } else if (this.columnSet.equals(QueryDto.BUCKET)) {
      String formula = this.method.expressionGenerator.apply(alias + "(current bucket)", alias + "(reference bucket)");
      return formula + ", reference = " + this.referencePosition;
    } else {
      return "unknown";
    }
  }
}
