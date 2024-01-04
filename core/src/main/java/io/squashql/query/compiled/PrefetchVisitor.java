package io.squashql.query.compiled;

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
    Map<QueryScope, Set<CompiledMeasure>> result = new HashMap<>(Map.of(this.originalQueryScope, Set.of(cmrp.measure())));
    result.put(readScope, Set.of(cmrp.measure()));
    return result;
  }

  @Override
  public Map<QueryScope, Set<CompiledMeasure>> visit(CompiledDoubleConstantMeasure measure) {
    return empty();
  }

  @Override
  public Map<QueryScope, Set<CompiledMeasure>> visit(CompiledLongConstantMeasure measure) {
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
     *                   ticker AS ___alias___ticker___,
     *                   date AS ___alias___date___,
     *                   riskType AS ___alias___riskType___,
     *                   grouping(ticker) as ___alias___grouping_ticker___,
     *                   grouping(riskType) as ___alias___grouping_riskType___,
     *                   sum(value) as value_sum
     *                   from MYTABLE
     *                   group by ___alias___date___, rollup(___alias___ticker___, ___alias___riskType___)
     * (we do not care about totals on date)
     *
     * query = select
     *          			___alias___ticker___,
     *          			___alias___riskType___,
     *          			array_agg(value_sum) as vector,
     *          			max(___alias___grouping_riskType___) as ___grouping______alias___riskType______, // To make __total__ appear
     *          			max(___alias___grouping_ticker___) as ___grouping______alias___ticker______ // To make __total__ appear
     *          			from (subquery)
     *          			group by ___alias___ticker___, ___alias___riskType___
     *
     * If there is no rollup, it is much simpler:
     *
     * subquery = select
     *                  ticker AS ___alias___ticker___,
     *                  date AS ___alias___date___,
     *                  riskType AS ___alias___riskType___,
     *                  sum(value) as value_sum
     *                  from MYTABLE
     *                  group by ___alias___ticker___, ___alias___date___, ___alias___riskType___
     *
     * query = select
		 *               ___alias___ticker___,
		 *               ___alias___riskType___,
		 *               array_agg(value_sum) as vector,
		 *               from (subquery)
		 *               group by ___alias___ticker___, ___alias___riskType___
     */
    TypedField vectorAxis = vectorAggMeasure.vectorAxis();
    if (this.originalQueryScope.columns().contains(vectorAggMeasure.vectorAxis())) {
      var m = new CompiledAggregatedMeasure(vectorAggMeasure.alias(), vectorAggMeasure.fieldToAggregate(), vectorAggMeasure.aggregationFunction(), null, false);
      return Map.of(this.originalQueryScope, Set.of(m));
    } else {
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
          // groupingAlias should not contain '.' !! this is a defect that will be fixed in the future
          groupingAlias = groupingAlias.replace(".", "_");
          subQueryMeasures.add(new CompiledAggregatedMeasure(groupingAlias, selectColumn, GROUPING, null, false));
          topQueryMeasures.add(new CompiledAggregatedMeasure(SqlUtils.groupingAlias(alias), new AliasedTypedField(groupingAlias), MAX, null, false));
        }
      }
      String vectorAxisAlias = SqlUtils.columnAlias(SqlUtils.squashqlExpression(vectorAxis)).replace(".", "_");
      subQuerySelectColumns.add(vectorAxis.as(vectorAxisAlias));

      String subQueryMeasureAlias = (vectorAggMeasure.fieldToAggregate().name() + "_" + vectorAggMeasure.aggregationFunction()).replace(".", "_");
      subQueryMeasures.add(new CompiledAggregatedMeasure(subQueryMeasureAlias, vectorAggMeasure.fieldToAggregate(), vectorAggMeasure.aggregationFunction(), null, false));

      List<TypedField> subQueryRollupColumns = new ArrayList<>();
      for (TypedField r : this.originalQueryScope.rollupColumns()) {
        // Here we can choose any alias but for debugging purpose, we create one from the expression.
        String expression = SqlUtils.squashqlExpression(r);
        String alias = SqlUtils.columnAlias(expression).replace(".", "_");
        subQueryRollupColumns.add(r.as(alias));
      }

      DatabaseQuery subQuery = new DatabaseQuery(this.originalQueryScope.virtualTable(),
              this.originalQueryScope.table(),
              this.originalQueryScope.subQuery(),
              new HashSet<>(subQuerySelectColumns),
              this.originalQueryScope.whereCriteria(),
              this.originalQueryScope.havingCriteria(),
              subQueryRollupColumns,
              this.originalQueryScope.groupingSets(),
              -1);
      subQueryMeasures.forEach(subQuery::withMeasure);

      QueryScope topQueryScope = new QueryScope(
              this.originalQueryScope.table(),
              subQuery,
              topQuerySelectColumns,
              this.originalQueryScope.whereCriteria(),
              this.originalQueryScope.havingCriteria(),
              Collections.emptyList(), // remove rollup, it has been computed in the subquery
              Collections.emptyList(),
              this.originalQueryScope.virtualTable(),
              this.originalQueryScope.limit());

      topQueryMeasures.add(new CompiledAggregatedMeasure(vectorAggMeasure.alias(), new AliasedTypedField(subQueryMeasureAlias), ARRAY_AGG, null, false));
      return Map.of(topQueryScope, topQueryMeasures);
    }
  }
}
