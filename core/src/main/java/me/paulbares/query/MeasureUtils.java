package me.paulbares.query;

import lombok.NoArgsConstructor;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

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
    } else if (m instanceof ComparisonMeasureReferencePosition cm) {
      String alias = cm.measure.alias();
      return switch (cm.columnSetKey) {
        case BUCKET -> {
          String formula = cm.comparisonMethod.expressionGenerator.apply(alias + "(current bucket)", alias + "(reference bucket)");
          yield formula + ", reference = " + cm.referencePosition;
        }
        case PERIOD -> {
          String formula = cm.comparisonMethod.expressionGenerator.apply(alias + "(current period)", alias + "(reference period)");
          yield formula + ", reference = " + cm.referencePosition;
        }
        case PARENT -> "unknown";
      };
    } else if (m instanceof ExpressionMeasure em) {
      return em.expression;
    } else if (m instanceof ConstantMeasure cm) {
      return String.valueOf(cm.value);
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

  public static List<QueryExecutor.QueryScope> getParentScopes(QueryExecutor.QueryScope queryScope, ParentComparisonMeasure pcm, Function<String, Field> fieldSupplier) {
    List<QueryExecutor.QueryScope> requiredScopes = new ArrayList<>();
    for (int i = 1; i < pcm.ancestors.size(); i++) {
      List<Field> copy = new ArrayList<>(queryScope.columns());
      List<Field> toRemove = pcm.ancestors.subList(0, i).stream().map(fieldSupplier).toList();
      copy.removeAll(toRemove);
      requiredScopes.add(new QueryExecutor.QueryScope(queryScope.tableDto(), queryScope.subQuery(), copy, queryScope.conditions()));
    }
    return requiredScopes;
  }

  public static boolean isPrimitive(Measure m) {
    return m instanceof AggregatedMeasure || m instanceof ExpressionMeasure;
  }
}
