package io.squashql.query;

import lombok.NoArgsConstructor;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.Period;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@NoArgsConstructor
public final class MeasureUtils {

  private static final QueryRewriter BASIC = new QueryRewriter() {
    @Override
    public boolean usePartialRollupSyntax() {
      return false;
    }

    @Override
    public boolean useGroupingFunction() {
      return false;
    }
  };

  public static String createExpression(Measure m) {
    if (m instanceof AggregatedMeasure a) {
      if (a.criteria != null) {
        String conditionSt = SQLTranslator.toSql(f -> new Field(f, String.class), a.criteria, BASIC);
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
    AtomicReference<CriteriaDto> copy = new AtomicReference<>(queryScope.criteriaDto() == null ? null : CriteriaDto.deepCopy(queryScope.criteriaDto()));
    Consumer<String> criteriaRemover = field -> copy.set(removeCriteriaOnField(field, copy.get()));
    Optional.ofNullable(query.columnSets.get(ColumnSetKey.BUCKET))
            .ifPresent(cs -> cs.getColumnsForPrefetching().forEach(criteriaRemover::accept));
    Optional.ofNullable(cm.period)
            .ifPresent(p -> getColumnsForPrefetching(p).forEach(criteriaRemover::accept));
    Set<Field> rollupColumns = new LinkedHashSet<>(queryScope.rollupColumns()); // order does matter
    Optional.ofNullable(cm.ancestors)
            .ifPresent(ancestors -> {
              ancestors.forEach(criteriaRemover::accept);
              List<Field> ancestorFields = ancestors.stream().filter(ancestor -> query.columns.contains(ancestor)).map(fieldSupplier::apply).collect(Collectors.toList());
              Collections.reverse(ancestorFields); // Order does matter. By design, ancestors is a list of column names in "lineage order".
              rollupColumns.addAll(ancestorFields);
            });
    return new QueryExecutor.QueryScope(queryScope.tableDto(), queryScope.subQuery(), queryScope.columns(), copy.get(), new ArrayList<>(rollupColumns));
  }

  private static CriteriaDto removeCriteriaOnField(String field, CriteriaDto root) {
    if (root == null) {
      return null;
    } else if (root.isCriterion()) {
      return root.field.equals(field) ? null : root;
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