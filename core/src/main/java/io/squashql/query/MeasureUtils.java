package io.squashql.query;

import io.squashql.PrimitiveMeasureVisitor;
import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.Period;
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
        return formula + ", ancestors = " + cm.ancestors.stream().map(Field::name).toList();
      } else {
        String formula = cm.getComparisonMethod().expressionGenerator.apply(alias + "(current)", alias + "(reference)");
        return formula + ", reference = " + cm.referencePosition.entrySet().stream().map(e -> String.join("=", e.getKey().name(), e.getValue())).toList();
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
          QueryExecutor.QueryScope queryScope) {
    AtomicReference<CompiledCriteria> copy = new AtomicReference<>(queryScope.whereCriteria() == null ? null : CriteriaDto.deepCopy(queryScope.whereCriteria()));
    Consumer<TypedField> criteriaRemover = field -> copy.set(removeCriteriaOnField(field, copy.get()));
    Optional.ofNullable(query.columnSets.get(ColumnSetKey.BUCKET))
            .ifPresent(cs -> cs.getColumnsForPrefetching().forEach(criteriaRemover));
    Optional.ofNullable(cm.period)
            .ifPresent(p -> getColumnsForPrefetching(p).forEach(criteriaRemover));
    Set<TypedField> rollupColumns = new LinkedHashSet<>(queryScope.rollupColumns()); // order does matter
    Optional.ofNullable(cm.ancestors)
            .ifPresent(ancestors -> {
              ancestors.forEach(criteriaRemover);
              List<TypedField> ancestorFields = ancestors.stream().filter(ancestor -> query.columns.contains(ancestor)).map(queryResolver::resolveField).toList();
              rollupColumns.addAll(ancestorFields); // Order does matter. By design, ancestors is a list of column names in "lineage reverse order".
            });
    return new QueryExecutor.QueryScope(queryScope.table(),
            queryScope.subQuery(),
            queryScope.columns(),
            copy.get(),
            queryScope.havingCriteria(),
            new ArrayList<>(rollupColumns),
            new ArrayList<>(queryScope.groupingSets()), // FIXME should handle groupingSets
            queryScope.virtualTable());
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

}
