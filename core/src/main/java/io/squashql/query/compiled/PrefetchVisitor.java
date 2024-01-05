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
import java.util.stream.Stream;

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
    TypedField fieldToAggregate = vectorAggMeasure.fieldToAggregate();
    String vectorAggFunc = vectorAggMeasure.aggregationFunction();
    if (this.originalQueryScope.columns().contains(vectorAxis)) {
      var m = new CompiledAggregatedMeasure(vectorAggMeasure.alias(), fieldToAggregate, vectorAggFunc, null, false);
      return Map.of(this.originalQueryScope, Set.of(m));
    } else {
      Set<CompiledMeasure> subQueryMeasures = new HashSet<>();
      List<TypedField> topQuerySelectColumns = new ArrayList<>();
      List<TypedField> subQuerySelectColumns = new ArrayList<>();
      Set<CompiledMeasure> topQueryMeasures = new HashSet<>();
      for (TypedField selectColumn : this.originalQueryScope.columns()) {
        // Here we can choose any alias but for debugging purpose, we create one from the expression.
        String alias = safeColumnAlias(SqlUtils.squashqlExpression(selectColumn));
        subQuerySelectColumns.add(selectColumn.as(alias));
        topQuerySelectColumns.add(new AliasedTypedField(alias));
      }

      Stream.concat(this.originalQueryScope.rollupColumns().stream(), this.originalQueryScope.groupingSets().stream().flatMap(Collection::stream))
              .forEach(rollup -> {
                String expression = SqlUtils.squashqlExpression(rollup);
                String alias = safeColumnAlias(expression);
                String groupingAlias = safeColumnAlias("grouping_" + expression);
                subQueryMeasures.add(new CompiledAggregatedMeasure(groupingAlias, rollup, GROUPING, null, false));
                topQueryMeasures.add(new CompiledAggregatedMeasure(SqlUtils.groupingAlias(alias), new AliasedTypedField(groupingAlias), MAX, null, false));
              });


      String vectorAxisAlias = safeColumnAlias(SqlUtils.squashqlExpression(vectorAxis));
      List<TypedField> subQueryRollupColumns = new ArrayList<>();
      List<List<TypedField>> subQueryGroupingSets = new ArrayList<>();
      subQuerySelectColumns.add(vectorAxis.as(vectorAxisAlias));// it will end up in the group by (See SqlTranslator) if rollup or in the grouping sets

      if (!this.originalQueryScope.rollupColumns().isEmpty()) {
        for (TypedField r : this.originalQueryScope.rollupColumns()) {
          // Here we can choose any alias but for debugging purpose, we create one from the expression.
          subQueryRollupColumns.add(r.as(safeColumnAlias(SqlUtils.squashqlExpression(r))));
        }
      } else if (!this.originalQueryScope.groupingSets().isEmpty()) {
        for (List<TypedField> groupingSet : this.originalQueryScope.groupingSets()) {
          List<TypedField> copy = new ArrayList<>();
          for (TypedField r : groupingSet) {
            // Here we can choose any alias but for debugging purpose, we create one from the expression.
            copy.add(r.as(safeColumnAlias(SqlUtils.squashqlExpression(r))));
          }
          // vectorAxisAlias need to be put in each grouping set.
          copy.add(vectorAxis.as(vectorAxisAlias));
          subQueryGroupingSets.add(copy);
        }
      }

      String subQueryMeasureAlias = safeColumnAlias(fieldToAggregate.name() + "_" + vectorAggFunc);
      subQueryMeasures.add(new CompiledAggregatedMeasure(subQueryMeasureAlias, fieldToAggregate, vectorAggFunc, null, false));

      DatabaseQuery subQuery = new DatabaseQuery(this.originalQueryScope.virtualTable(),
              this.originalQueryScope.table(),
              this.originalQueryScope.subQuery(),
              new HashSet<>(subQuerySelectColumns),
              this.originalQueryScope.whereCriteria(),
              this.originalQueryScope.havingCriteria(),
              subQueryRollupColumns,
              subQueryGroupingSets,
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

  /**
   * Alias should not contain '.' !! because BQ does not support it !
   */
  private static String safeColumnAlias(String alias) {
    return SqlUtils.columnAlias(alias).replace(".", "_");
  }
}
