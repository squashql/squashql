package io.squashql.query;

import io.squashql.query.compiled.*;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.QueryScope;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.QueryDto;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import lombok.NoArgsConstructor;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

@NoArgsConstructor
public final class MeasureUtils {

  public static final QueryRewriter BASIC = new QueryRewriter() {
  };

  public static String createExpression(Measure m) {
    if (m instanceof AggregatedMeasure a) {
      final CompiledAggregatedMeasure compiled = (CompiledAggregatedMeasure) new ExpressionResolver(m).getMeasures().values().iterator().next();
      final String fieldExpression = compiled.field().sqlExpression(BASIC);
      if (compiled.criteria() == null) {
        return a.aggregationFunction + "(" + fieldExpression + ")";
      } else {
        return a.aggregationFunction + "If(" + fieldExpression + ", " + compiled.criteria().sqlExpression(BASIC) + ")";
      }
    } else if (m instanceof BinaryOperationMeasure bom) {
      return quoteExpression(bom.leftOperand) + " " + bom.operator.infix + " " + quoteExpression(bom.rightOperand);
    } else if (m instanceof ComparisonMeasureReferencePosition cm) {
      String alias = cm.getMeasure().alias();
      if (cm.ancestors != null) {
        String formula = cm.getComparisonMethod().expressionGenerator.apply(alias, alias + "(parent)");
        return formula + ", ancestors = " + cm.ancestors.stream().map(SqlUtils::squashqlExpression).toList();
      } else {
        String formula = cm.getComparisonMethod().expressionGenerator.apply(alias + "(current)", alias + "(reference)");
        return formula + ", reference = " + cm.referencePosition.entrySet().stream().map(e -> String.join("=", SqlUtils.squashqlExpression(e.getKey()), e.getValue())).toList();
      }
    } else if (m instanceof ExpressionMeasure em) {
      return em.expression;
    } else if (m instanceof ConstantMeasure<?> cm) {
      return String.valueOf(cm.value);
    } else {
      throw new IllegalArgumentException("Unexpected type " + m.getClass());
    }
  }

  private static class ExpressionResolver extends QueryResolver {

    public ExpressionResolver(Measure m) {
      super(new QueryDto().table("fake").withMeasure(m), Collections.emptyMap());
    }

    @Override
    public TypedField resolveField(Field field) {
      return new TableTypedField(null, ((TableField) field).fullName, String.class);
    }

    @Override
    protected void checkQuery(QueryDto query) {
      // nothing to do
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

  public static QueryScope getReadScopeComparisonMeasureReferencePosition(
          List<TypedField> columns,
          List<TypedField> groupColumns,
          CompiledComparisonMeasureReferencePosition cm,
          QueryScope queryScope) {
    AtomicReference<CompiledCriteria> copy = new AtomicReference<>(queryScope.whereCriteria() == null ? null : CompiledCriteria.deepCopy(queryScope.whereCriteria()));
    Consumer<TypedField> criteriaRemover = cm.clearFilters() ? field -> copy.set(removeCriteriaOnField(field, copy.get())) : Function.identity()::apply;
    groupColumns.forEach(criteriaRemover);
    Optional.ofNullable(cm.referencePosition())
            // To handle ComparisonMeasure with no GroupColumn => previous member, first member...
            .ifPresent(ref -> ref.keySet().forEach(criteriaRemover));
    Optional.ofNullable(cm.period())
            .ifPresent(p -> getColumnsForPrefetching(p).forEach(criteriaRemover));
    Set<TypedField> rollupColumns = new LinkedHashSet<>(queryScope.rollup()); // order does matter
    Set<Set<TypedField>> groupingSets = new HashSet<>(queryScope.groupingSets());
    Optional.ofNullable(cm.ancestors())
            .ifPresent(ancestors -> {
              ancestors.forEach(criteriaRemover);
              List<TypedField> ancestorFields = ancestors.stream().filter(columns::contains).toList();
              List<TypedField> copyColumns = new ArrayList<>(columns);
              copyColumns.removeAll(ancestors);
              rollupColumns.addAll(ancestorFields); // Order does matter. By design, ancestors is a list of column names in "lineage reverse order".

              Set<Set<TypedField>> additionalGroupingSets = new HashSet<>();
              Set<TypedField> current = new HashSet<>(ancestorFields);
              additionalGroupingSets.add(current);
              ListIterator<TypedField> it = ancestorFields.listIterator(ancestorFields.size());
              while (it.hasPrevious()) {
                current = new HashSet<>(current); // copy
                current.remove(it.previous());
                additionalGroupingSets.add(current);
              }

              // column from select must be in the grouping sets to avoid error such as:
              // column "city" must appear in the GROUP BY clause or must be part of an aggregate function.
              for (Set<TypedField> additionalGroupingSet : additionalGroupingSets) {
                additionalGroupingSet.addAll(copyColumns);
              }
              additionalGroupingSets.add(new HashSet<>()); // add empty set for GT that might not exist anymore after the
              // addition of the additionalGroupingSets

              groupingSets.addAll(additionalGroupingSets);
            });
    // We might have grouping sets from pivot table. In that, rollups are ignored by SqlTranslator so that we need to fallback
    // to use grouping sets. So for all ancestorFields, we need to create the appropriate grouping sets equivalent to rollup(ancestorFields)
    return new QueryScope(queryScope.table(),
            queryScope.columns(),
            copy.get(),
            queryScope.havingCriteria(),
            new ArrayList<>(rollupColumns),
            groupingSets,
            queryScope.cteRecordTables(),
            Collections.emptyList(),
            queryScope.limit());
  }

  public static QueryScope getReadScopeComparisonGrandTotalMeasure(QueryScope queryScope) {
    return new QueryScope(queryScope.table(),
            queryScope.columns(),
            queryScope.whereCriteria(),
            queryScope.havingCriteria(),
            Collections.emptyList(),
            Set.of(new HashSet<>(queryScope.columns()), Set.of()), // list of empty list => GT
            queryScope.cteRecordTables(),
            queryScope.orderBy(),
            queryScope.limit());
  }

  private static CompiledCriteria removeCriteriaOnField(TypedField field, CompiledCriteria root) {
    if (root == null) {
      return null;
    } else if (root.field() != null && root.condition() != null) { // where clause condition
      return root.field().equals(field) ? null : root;
    } else {
      removeCriteriaOnField(field, root.children());
      return root;
    }
  }

  private static void removeCriteriaOnField(TypedField field, List<CompiledCriteria> children) {
    Iterator<CompiledCriteria> iterator = children.iterator();
    while (iterator.hasNext()) {
      CompiledCriteria criteriaDto = iterator.next();
      if (criteriaDto.field() != null && criteriaDto.condition() != null) { // where clause condition
        if (criteriaDto.field().equals(field)) {
          iterator.remove();
        }
      } else {
        removeCriteriaOnField(field, criteriaDto.children());
      }
    }
  }

  public static boolean isPrimitive(CompiledMeasure m) {
    return m.accept(new PrimitiveMeasureVisitor());
  }

  public static List<TypedField> getColumnsForPrefetching(CompiledPeriod period) {
    if (period instanceof CompiledPeriod.Quarter q) {
      return List.of(q.year(), q.quarter());
    } else if (period instanceof CompiledPeriod.Year y) {
      return List.of(y.year());
    } else if (period instanceof CompiledPeriod.Month m) {
      return List.of(m.year(), m.month());
    } else if (period instanceof CompiledPeriod.Semester s) {
      return List.of(s.year(), s.semester());
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

}
