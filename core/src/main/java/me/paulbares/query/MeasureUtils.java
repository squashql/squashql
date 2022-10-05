package me.paulbares.query;

import lombok.NoArgsConstructor;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.store.Field;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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
      String alias = cm.getMeasure().alias();
      return switch (cm.getColumnSetKey()) {
        case BUCKET -> {
          String formula = cm.getComparisonMethod().expressionGenerator.apply(alias + "(current bucket)", alias + "(reference bucket)");
          yield formula + ", reference = " + ((ComparisonMeasureReferencePosition) cm).referencePosition;
        }
        case PERIOD -> {
          String formula = cm.getComparisonMethod().expressionGenerator.apply(alias + "(current period)", alias + "(reference period)");
          yield formula + ", reference = " + ((ComparisonMeasureReferencePosition) cm).referencePosition;
        }
        case PARENT -> {
          String formula = cm.getComparisonMethod().expressionGenerator.apply(alias, alias + "(parent)");
          yield formula + ", ancestors = " + ((ParentComparisonMeasure) cm).ancestors;
        }
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

  public static QueryExecutor.QueryScope getParentScopeWithClearedConditions(QueryExecutor.QueryScope queryScope, ParentComparisonMeasure pcm, Function<String, Field> fieldSupplier) {
    int lowestColumnIndex = -1;
    Set<String> cols = queryScope.columns().stream().map(Field::name).collect(Collectors.toSet());
    for (int i = 0; i < pcm.ancestors.size(); i++) {
      String ancestor = pcm.ancestors.get(i);
      if (cols.contains(ancestor)) {
        lowestColumnIndex = i;
        break;
      }
    }

    Map<String, ConditionDto> newConditions = new HashMap<>(queryScope.conditions());
    for (String ancestor : pcm.ancestors) {
      newConditions.remove(ancestor);
    }

    List<Field> copy = new ArrayList<>(queryScope.columns());
    List<Field> toRemove = pcm.ancestors.subList(0, lowestColumnIndex + 1).stream().map(fieldSupplier).toList();
    copy.removeAll(toRemove);
    return new QueryExecutor.QueryScope(queryScope.tableDto(), queryScope.subQuery(), copy, newConditions);
  }

  public static boolean isPrimitive(Measure m) {
    return m instanceof AggregatedMeasure || m instanceof ExpressionMeasure;
  }
}
