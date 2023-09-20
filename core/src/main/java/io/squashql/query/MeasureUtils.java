package io.squashql.query;

import io.squashql.PrimitiveMeasureVisitor;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.Period;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public final class MeasureUtils {

  public static final QueryRewriter BASIC = new QueryRewriter() {
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
      Function<Field, TypedField> fieldProvider = s -> new TableTypedField(null, s.name(), String.class);
      if (a.criteria != null) {
        String conditionSt = SQLTranslator.toSql(fieldProvider, a.criteria, BASIC);
        return a.aggregationFunction + "If(" + a.field.sqlExpression(fieldProvider, BASIC) + ", " + conditionSt + ")";
      } else {
        return a.aggregationFunction + "(" + a.field.sqlExpression(fieldProvider, BASIC) + ")";
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
    } else if (m instanceof ConstantMeasure<?> cm) {
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
          Function<Field, TypedField> fieldSupplier) {
    AtomicReference<CriteriaDto> copy = new AtomicReference<>(queryScope.whereCriteriaDto() == null ? null : CriteriaDto.deepCopy(queryScope.whereCriteriaDto()));
    Consumer<Field> criteriaRemover = field -> copy.set(removeCriteriaOnField(field, copy.get()));
    Optional.ofNullable(query.columnSets.get(ColumnSetKey.BUCKET))
            .ifPresent(cs -> cs.getColumnsForPrefetching().forEach(criteriaRemover));
    Optional.ofNullable(cm.period)
            .ifPresent(p -> getColumnsForPrefetching(p).forEach(criteriaRemover));
    Set<TypedField> rollupColumns = new LinkedHashSet<>(queryScope.rollupColumns()); // order does matter
    Optional.ofNullable(cm.ancestors)
            .ifPresent(ancestors -> {
              ancestors.forEach(criteriaRemover);
              List<TypedField> ancestorFields = ancestors.stream().filter(ancestor -> query.columns.contains(ancestor)).map(fieldSupplier).collect(Collectors.toList());
              Collections.reverse(ancestorFields); // Order does matter. By design, ancestors is a list of column names in "lineage order".
              rollupColumns.addAll(ancestorFields);
            });
    return new QueryExecutor.QueryScope(queryScope.tableDto(),
            queryScope.subQuery(),
            queryScope.columns(),
            copy.get(),
            queryScope.havingCriteriaDto(),
            new ArrayList<>(rollupColumns),
            new ArrayList<>(queryScope.groupingSets()), // FIXME should handle groupingSets
            queryScope.virtualTableDto());
  }

  private static CriteriaDto removeCriteriaOnField(Field field, CriteriaDto root) {
    if (root == null) {
      return null;
    } else if (root.isWhereCriterion()) {
      return root.field.equals(field) ? null : root;
    } else {
      removeCriteriaOnField(field, root.children);
      return root;
    }
  }

  private static void removeCriteriaOnField(Field field, List<CriteriaDto> children) {
    Iterator<CriteriaDto> iterator = children.iterator();
    while (iterator.hasNext()) {
      CriteriaDto criteriaDto = iterator.next();
      if (criteriaDto.isWhereCriterion()) {
        if (criteriaDto.field.equals(field)) {
          iterator.remove();
        }
      } else {
        removeCriteriaOnField(field, criteriaDto.children);
      }
    }
  }

  public static boolean isPrimitive(Measure m) {
    return m.accept(new PrimitiveMeasureVisitor());
  }

  public static List<Field> getColumnsForPrefetching(Period period) {
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

  public static Function<Field, TypedField> withFallback(Function<Field, TypedField> fieldProvider, Class<?> fallbackType) {
    return field -> {
      try {
        return fieldProvider.apply(field);
      } catch (FieldNotFoundException e) {
        // This can happen if the using a "field" coming from the calculation of a subquery. Since the field provider
        // contains only "raw" fields, it will throw an exception.
        return new TableTypedField(null, field.name(), fallbackType);
      }
    };
  }
}
