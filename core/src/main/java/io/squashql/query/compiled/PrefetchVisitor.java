package io.squashql.query.compiled;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.AliasedField;
import io.squashql.query.MeasureUtils;
import io.squashql.query.QueryExecutor.QueryScope;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.AliasedTypedField;
import io.squashql.type.TypedField;
import lombok.RequiredArgsConstructor;
import org.eclipse.collections.impl.set.mutable.MutableSetFactoryImpl;

import java.util.*;

import static io.squashql.query.agg.AggregationFunction.*;

@RequiredArgsConstructor
public class PrefetchVisitor implements MeasureVisitor<Map<QueryScope, Set<CompiledMeasure>>> {

  private final List<TypedField> columns;
  private final List<TypedField> bucketColumns;
  private final QueryScope originalQueryScope;

  private Map<QueryScope, Set<CompiledMeasure>> empty() {
    return Collections.emptyMap();
  }

  @Override
  public Map<QueryScope, Set<CompiledMeasure>> visit(CompiledAggregatedMeasure measure) {
    return empty();
  }

  @Override
  public Map<QueryScope, Set<CompiledMeasure>> visit(CompiledExpressionMeasure measure) {
    return empty();
  }

  @Override
  public Map<QueryScope, Set<CompiledMeasure>> visit(CompiledBinaryOperationMeasure measure) {
    if (new PrimitiveMeasureVisitor().visit(measure)) {
      return empty();
    } else {
      return Map.of(this.originalQueryScope, MutableSetFactoryImpl.INSTANCE.of(measure.leftOperand(), measure.rightOperand()));
    }
  }

  @Override
  public Map<QueryScope, Set<CompiledMeasure>> visit(CompiledComparisonMeasure cmrp) {
    QueryScope readScope = MeasureUtils.getReadScopeComparisonMeasureReferencePosition(this.columns, this.bucketColumns, cmrp, this.originalQueryScope);
    Map<QueryScope, Set<CompiledMeasure>> result = new HashMap<>(Map.of(this.originalQueryScope, Set.of(cmrp.reference())));
    result.put(readScope, Set.of(cmrp.reference()));
    return result;
  }

  @Override
  public Map<QueryScope, Set<CompiledMeasure>> visit(CompiledConstantMeasure measure) {
    return empty();
  }

  @Override
  public Map<QueryScope, Set<CompiledMeasure>> visit(CompiledVectorAggMeasure vectorAggMeasure) {
    /*
     * The trick here is to generate a query with a subquery that first compute the aggregate of measure.fieldToAggregate
     * with the aggregation function measure.aggregationFunction + the correct rollup and then the top query computes
     * the final aggregates to create the vector. To do that, we use aliases and grouping functions.
     * Let's say the original query looks like this: select a,b,vector(vectorAlias,c,sum,date) from table group by rollup(a,b).
     * The following db query is generated:
     *
     * subquery = select
     *                    a as alias_a,
     *                    b as alias_b,
     *                    date as alias_date,
     *                    sum(c) as c_sum,
     *                    grouping(a) as grouping_a,
     *                    grouping(b) as grouping_b
     *                    from table
     *                    group by alias_date, rollup(alias_a, alias_b)
     * (we do not care about totals on date)
     *
     * query = select
     *                    alias_a,
     *                    alias_b,
     *                    max(grouping_a) as ___grouping___grouping_a___ // to make __total__ appear
     *                    max(grouping_b) as ___grouping___grouping_b___ // to make __total__ appear
     *                    array_agg(c_sum order by alias_c) as vectorAlias
     *                    from (subquery)
     *                    group by alias_a, alias_b
     *
     * If there is no rollup, it is much simpler:
     *
     * subquery = select
     *             a as alias_a,
     *             b as alias_b,
     *             date as alias_date,
     *             sum(c) as c_sum,
     *             from table
     *             group by alias_date, alias_a, alias_b
     *
     * query = select
     *             alias_a,
     *             alias_b,
     *             array_agg(c_sum order by alias_c) as vectorAlias
     *             from (subquery)
     *             group by alias_a, alias_b
     */
    TypedField vectorAxis = vectorAggMeasure.vectorAxis();
    if (this.originalQueryScope.columns().contains(vectorAggMeasure.vectorAxis())) {
      AggregatedMeasure aggMeasure = new AggregatedMeasure(vectorAggMeasure.alias(), vectorAggMeasure.vectorAggMeasure().fieldToAggregate, vectorAggMeasure.vectorAggMeasure().aggregationFunction, false);
      return Map.of(this.originalQueryScope, Set.of(new CompiledAggregatedMeasure(aggMeasure, vectorAggMeasure.fieldToAggregate(), null)));
    } else {
//      QueryScope subQuery = new QueryScope(
//              this.originalQueryScope.table(),
//              this.originalQueryScope.subQuery(),
//              new ArrayList<>(this.originalQueryScope.columns()),
//              this.originalQueryScope.whereCriteria(),
//              this.originalQueryScope.havingCriteria(),
//              new ArrayList<>(this.originalQueryScope.rollupColumns()),
//              new ArrayList<>(this.originalQueryScope.groupingSets()),
//              this.originalQueryScope.virtualTable());

      boolean hasRollup = !this.originalQueryScope.rollupColumns().isEmpty();

      List<CompiledMeasure> subQueryMeasures = new ArrayList<>();
      List<TypedField> topQuerySelectColumns = new ArrayList<>();
      List<TypedField> subQuerySelectColumns = new ArrayList<>();
      Set<CompiledMeasure> topQueryMeasures = new HashSet<>();
      for (TypedField selectColumn : this.originalQueryScope.columns()) {
        // Here we can choose any alias but for debugging purpose, we create one from the expression.
        String expression = SqlUtils.squashqlExpression(selectColumn);
        String alias = SqlUtils.columnAlias(expression).replace(".", "_");
        subQuerySelectColumns.add(selectColumn.as(alias));
        topQuerySelectColumns.add(new AliasedTypedField(alias));
        if (hasRollup) {
          String groupingAlias = SqlUtils.columnAlias("grouping_" + expression);
//          // groupingAlias should not contain '.' !! this is a defect that will be fixed in the future
          groupingAlias = groupingAlias.replace(".", "_");
          subQueryMeasures.add(new CompiledAggregatedMeasure(new AggregatedMeasure(groupingAlias, expression, GROUPING), selectColumn, null));
          topQueryMeasures.add(new CompiledAggregatedMeasure(new AggregatedMeasure(SqlUtils.groupingAlias(alias), groupingAlias, MAX), new AliasedTypedField(groupingAlias), null));
        }
      }
      String vectorAxisAlias = SqlUtils.columnAlias(SqlUtils.squashqlExpression(vectorAxis)).replace(".", "_");
      subQuerySelectColumns.add(vectorAxis.as(vectorAxisAlias));

      String subQueryMeasureAlias = (vectorAggMeasure.fieldToAggregate().name() + "_" + vectorAggMeasure.vectorAggMeasure().aggregationFunction).replace(".", "_");
      AggregatedMeasure aggregatedMeasure = new AggregatedMeasure(subQueryMeasureAlias, vectorAggMeasure.fieldToAggregate().name(), vectorAggMeasure.vectorAggMeasure().aggregationFunction);
      subQueryMeasures.add(new CompiledAggregatedMeasure(aggregatedMeasure, vectorAggMeasure.fieldToAggregate(), null));

      List<TypedField> subQueryRollupColumns = new ArrayList<>();
      for (TypedField r : this.originalQueryScope.rollupColumns()) {
        // Here we can choose any alias but for debugging purpose, we create one from the expression.
        String expression = SqlUtils.squashqlExpression(r);
        String alias = SqlUtils.columnAlias(expression).replace(".", "_");
        subQueryRollupColumns.add(r.as(alias));
      }

      DatabaseQuery sq = new DatabaseQuery(this.originalQueryScope.virtualTable(),
              this.originalQueryScope.table(),
              null, // FIXME should be subquery
              new HashSet<>(subQuerySelectColumns),
              this.originalQueryScope.whereCriteria(),
              this.originalQueryScope.havingCriteria(),
              subQueryRollupColumns,
              this.originalQueryScope.groupingSets(),
              -1); // FIXME issue with limit, what value to put?
      subQueryMeasures.forEach(sq::withMeasure);


      QueryScope topQueryScope = new QueryScope(
              this.originalQueryScope.table(),
              sq,
              topQuerySelectColumns,
              this.originalQueryScope.whereCriteria(),
              this.originalQueryScope.havingCriteria(),
              Collections.emptyList(), // remove rollup, it has been computed in the subquery
              Collections.emptyList(),
              this.originalQueryScope.virtualTable());

      AggregatedMeasure m = new AggregatedMeasure(vectorAggMeasure.alias(), new AliasedField(subQueryMeasureAlias), ARRAY_AGG, null);
      topQueryMeasures.add(new CompiledAggregatedMeasure(m, new AliasedTypedField(subQueryMeasureAlias), null));
      return Map.of(topQueryScope, topQueryMeasures);
    }
  }
}
