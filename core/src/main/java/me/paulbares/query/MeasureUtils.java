package me.paulbares.query;

import lombok.NoArgsConstructor;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

//  public static List<QueryExecutor.QueryScope> getParentScopes(QueryExecutor.QueryScope queryScope, ParentComparisonMeasure pcm, Function<String, Field> fieldSupplier) {
//    // We must make sure all ancestors are in the scope of the query.
//    List<QueryExecutor.QueryScope> requiredScopes = new ArrayList<>();
//    for (int i = 1; i < pcm.ancestors.size(); i++) {
//      List<Field> copy = new ArrayList<>(queryScope.columns());
//      List<Field> toRemove = pcm.ancestors.subList(0, i).stream().map(fieldSupplier).toList();
//      copy.removeAll(toRemove);
//      requiredScopes.add(new QueryExecutor.QueryScope(queryScope.tableDto(), queryScope.subQuery(), copy, queryScope.conditions()));
//    }
//    return requiredScopes;
//  }

  public static void checkQueryScopeForParentComparison(Set<Field> queriedFields, List<Field> ancestors) {
    Field parent = null;
    for (int i = 0; i < ancestors.size(); i++) {
      Field ancestor = ancestors.get(i);
      if (queriedFields.contains(ancestor)) {
        parent = ancestors.get(i + 1);
        break;
      }
//      else {
//        // throw only if aggregation is higher than
//        throw new IllegalArgumentException(ancestor + " should be in the query");
//      }
    }
    if (!queriedFields.contains(parent)) {
      throw new IllegalArgumentException(parent + " field is used in a parent comparison. It should be set as column in the query.");
    }
  }

  public static QueryExecutor.QueryScope getParentScope(QueryExecutor.QueryScope queryScope, ParentComparisonMeasure pcm, Function<String, Field> fieldSupplier) {
    int lowestColumnIndex = -1;
    Set<String> cols = queryScope.columns().stream().map(Field::name).collect(Collectors.toSet());
    for (int i = 0; i < pcm.ancestors.size(); i++) {
      if (cols.contains(pcm.ancestors.get(i))) {
        lowestColumnIndex = i;
        break;
      }
    }
    List<Field> copy = new ArrayList<>(queryScope.columns());
    List<Field> toRemove = pcm.ancestors.subList(0, lowestColumnIndex + 1).stream().map(fieldSupplier).toList();
    copy.removeAll(toRemove);
    return new QueryExecutor.QueryScope(queryScope.tableDto(), queryScope.subQuery(), copy, queryScope.conditions());
  }

  public static int zob(QueryExecutor.QueryScope queryScope, ParentComparisonMeasure pcm) {
    List<String> cols = queryScope.columns().stream().map(Field::name).collect(Collectors.toList());
    for (int i = 0; i < pcm.ancestors.size(); i++) {
      int index = cols.indexOf(pcm.ancestors.get(i));
      if (index >= 0) {
        return index;
      }
    }
    return -1;
  }

  public static boolean isPrimitive(Measure m) {
    return m instanceof AggregatedMeasure || m instanceof ExpressionMeasure;
  }
}
