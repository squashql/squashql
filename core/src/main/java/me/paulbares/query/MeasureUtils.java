package me.paulbares.query;

import lombok.NoArgsConstructor;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;

@NoArgsConstructor
public final class MeasureUtils {

  public static String createExpression(Measure m) {
    if (m instanceof AggregatedMeasure a) {
      if (a.conditionDto != null) {
        String conditionSt = SQLTranslator.toSql(new Field(a.conditionField, String.class), a.conditionDto);
        return a.aggregationFunction + "If(" + a.field + ", " + conditionSt + ")";
      } else {
        return a.aggregationFunction + "(" + a.field + ")";
      }
    } else if (m instanceof BinaryOperationMeasure bom) {
      return quoteExpression(bom.leftOperand) + " " + bom.operator.infix + " " + quoteExpression(bom.rightOperand);
    } else if (m instanceof ComparisonMeasure cm) {
      String alias = cm.measure.alias();
      if (cm.columnSet.equals(QueryDto.PERIOD)) {
        String formula = cm.method.expressionGenerator.apply(alias + "(current period)", alias + "(reference period)");
        return formula + ", reference = " + cm.referencePosition;
      } else if (cm.columnSet.equals(QueryDto.BUCKET)) {
        String formula = cm.method.expressionGenerator.apply(alias + "(current bucket)", alias + "(reference bucket)");
        return formula + ", reference = " + cm.referencePosition;
      } else {
        return "unknown";
      }
    } else if (m instanceof ExpressionMeasure em) {
      return em.expression;
    } else {
      throw new IllegalArgumentException("Unexpected type " + m.getClass());
    }
  }

  private static String quoteExpression(Measure m) {
    if (m.alias() != null) {
      return m.alias();
    }
    String expression = m.expression();
    if (!(m instanceof AggregatedMeasure)) {
      return '(' + expression + ')';
    } else {
      return expression;
    }
  }
}
