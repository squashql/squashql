package me.paulbares.query;

import lombok.NoArgsConstructor;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.query.dto.CriteriaDto;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
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
      String alias = cm.getMeasure().alias();
      if (cm.ancestors != null) {
        String formula = cm.getComparisonMethod().expressionGenerator.apply(alias, alias + "(parent)");
        return formula + ", ancestors = " + cm.ancestors;
      } else {
        String formula = cm.getComparisonMethod().expressionGenerator.apply(alias + "(current)", alias + "(reference)");
        return formula + ", reference = " + cm.referencePosition;
      }
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

  public static QueryExecutor.QueryScope getReadScopeComparisonMeasureReferencePosition(
          QueryDto query,
          ComparisonMeasureReferencePosition cm,
          QueryExecutor.QueryScope queryScope,
          Function<String, Field> fieldSupplier) {
    CriteriaDto copy = CriteriaDto.deepCopy(queryScope.criteriaDto());
    Consumer<String> criteriaRemover = field -> removeCriteriaOnField(field, copy);
    Optional.ofNullable(query.columnSets.get(ColumnSetKey.BUCKET))
            .ifPresent(cs -> cs.getColumnsForPrefetching().forEach(criteriaRemover::accept));
    Optional.ofNullable(cm.period)
            .ifPresent(p -> getColumnsForPrefetching(p).forEach(criteriaRemover::accept));
    List<Field> rollupColumns = new ArrayList<>(queryScope.rollupColumns());
    Optional.ofNullable(cm.ancestors)
            .ifPresent(p -> {
              p.forEach(criteriaRemover::accept);
              p.forEach(c -> {
                if (query.columns.contains(c)) {
                  rollupColumns.add(fieldSupplier.apply(c));
                }
              });
            });
    return new QueryExecutor.QueryScope(queryScope.tableDto(), queryScope.subQuery(), queryScope.columns(), copy, rollupColumns);
  }

  private static CriteriaDto removeCriteriaOnField(String field, CriteriaDto root) {
    if (root.isCriterion()) {
      return CriteriaDto.NO_CRITERIA;
    } else {
      removeCriteriaOnField(field, root.children);
      return root;
    }
  }

  private static void removeCriteriaOnField(String field, List<CriteriaDto> children) {
    Iterator<CriteriaDto> iterator = children.iterator();
    while (iterator.hasNext()) {
      CriteriaDto criteriaDto = iterator.next();
      if (criteriaDto.isCriterion()) {
        if (criteriaDto.field.equals(field)) {
          iterator.remove();
        }
      } else {
        removeCriteriaOnField(field, criteriaDto.children);
      }
    }
  }

  public static boolean isPrimitive(Measure m) {
    return m instanceof AggregatedMeasure || m instanceof ExpressionMeasure;
  }

  public static List<String> getColumnsForPrefetching(Period period) {
    if (period instanceof Period.Quarter q) {
      return List.of(q.year(), q.quarter());
    } else if (period instanceof Period.Year y) {
      return List.of(y.year());
    } else if (period instanceof Period.Month m) {
      return List.of(m.year(), m.month());
    } else if (period instanceof Period.Semester s) {
      return List.of(s.year(), s.semester());
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }
}
