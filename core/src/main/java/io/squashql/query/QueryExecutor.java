package io.squashql.query;

import io.squashql.query.cache.CaffeineQueryCache;
import io.squashql.query.cache.EmptyQueryCache;
import io.squashql.query.cache.GlobalCache;
import io.squashql.query.cache.QueryCache;
import io.squashql.query.compiled.*;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.QueryScope;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.query.join.ExperimentalQueryJoinExecutor;
import io.squashql.query.measure.visitor.PartialMeasureVisitor;
import io.squashql.query.parameter.QueryCacheParameter;
import io.squashql.table.*;
import io.squashql.type.TypedField;
import io.squashql.util.Queries;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import static io.squashql.query.ColumnSetKey.GROUP;
import static io.squashql.query.agg.AggregationFunction.GROUPING;
import static io.squashql.query.compiled.CompiledAggregatedMeasure.COMPILED_COUNT;

@Slf4j
public class QueryExecutor {

  public static final int LIMIT_DEFAULT_VALUE = Integer.parseInt(System.getProperty("squashql.query.limit", Integer.toString(10_000)));
  public final QueryEngine<?> queryEngine;
  public final QueryCache queryCache;

  public QueryExecutor(QueryEngine<?> queryEngine) {
    this(queryEngine, new GlobalCache(CaffeineQueryCache::new));
  }

  public QueryExecutor(QueryEngine<?> queryEngine, QueryCache cache) {
    this.queryEngine = queryEngine;
    this.queryCache = cache;
  }

  private QueryCache getQueryCache(QueryCacheParameter queryCacheParameter, SquashQLUser user) {
    return switch (queryCacheParameter.action) {
      case USE -> this.queryCache;
      case NOT_USE -> EmptyQueryCache.INSTANCE;
      case INVALIDATE -> {
        this.queryCache.clear(user);
        yield this.queryCache;
      }
    };
  }

  public PivotTable executePivotQuery(PivotTableQueryDto pivotTableQueryDto) {
    return executePivotQuery(pivotTableQueryDto, CacheStatsDto.builder(), null, true, null);
  }

  public PivotTable executePivotQuery(PivotTableQueryDto pivotTableQueryDto,
                                      CacheStatsDto.CacheStatsDtoBuilder cacheStatsDtoBuilder,
                                      SquashQLUser user,
                                      boolean replaceTotalCellsAndOrderRows,
                                      IntConsumer limitNotifier) {
    if (!pivotTableQueryDto.query.rollupColumns.isEmpty()) {
      throw new IllegalArgumentException("Rollup is not supported by this API");
    }

    PivotTableContext pivotTableContext = new PivotTableContext(pivotTableQueryDto);
    QueryDto preparedQuery = PivotTableUtils.prepareQuery(pivotTableQueryDto.query, pivotTableContext);
    Table result = executeQuery(preparedQuery, cacheStatsDtoBuilder, user, false, limitNotifier, pivotTableContext);
    if (replaceTotalCellsAndOrderRows) {
      result = TableUtils.replaceTotalCellValues((ColumnarTable) result,
              pivotTableQueryDto.rows.stream().map(SqlUtils::squashqlExpression).toList(),
              pivotTableQueryDto.columns.stream().map(SqlUtils::squashqlExpression).toList());
      result = TableUtils.orderRows((ColumnarTable) result, Queries.getComparators(preparedQuery), preparedQuery.columnSets.values());
    }

    List<String> values = pivotTableQueryDto.query.measures.stream().map(Measure::alias).toList();
    return new PivotTable(result,
            pivotTableQueryDto.rows.stream().map(SqlUtils::squashqlExpression).toList(),
            pivotTableQueryDto.columns.stream().map(SqlUtils::squashqlExpression).toList(),
            values,
            pivotTableQueryDto.hiddenTotals == null ? Collections.emptyList() : pivotTableQueryDto.hiddenTotals.stream().map(SqlUtils::squashqlExpression).toList());
  }

  public Table executeRaw(String rawSqlQuery) {
    return this.queryEngine.executeRawSql(rawSqlQuery);
  }

  /**
   * Tabular API.
   */
  public Table executeQuery(QueryDto query) {
    return executeQuery(
            query,
            CacheStatsDto.builder(),
            null,
            true,
            null,
            createPivotTableContext(query));
  }

  /**
   * Tabular API. Tabular is a special case of Pivot Table
   */
  public Table executeQuery(QueryDto query,
                            CacheStatsDto.CacheStatsDtoBuilder cacheStatsDtoBuilder,
                            SquashQLUser user,
                            boolean replaceTotalCellsAndOrderRows,
                            IntConsumer limitNotifier,
                            PivotTableContext pivotTableContext) {
    QueryDto preparedQuery = prepareQuery(query, pivotTableContext);

    QueryResolver queryResolver = new QueryResolver(preparedQuery, this.queryEngine.datastore().storeByName());
    DependencyGraph<QueryPlanNodeKey> dependencyGraph = computeDependencyGraph(
            queryResolver.getColumns(), queryResolver.getGroupColumns(), queryResolver.getMeasures().values(), queryResolver.getScope());
    // Compute what needs to be prefetched
    Map<QueryScope, QueryScope> prefetchQueryScopeByQueryScope = new HashMap<>();
    Map<QueryScope, Set<CompiledMeasure>> measuresByQueryScope = new HashMap<>();
    ExecutionPlan<QueryPlanNodeKey> prefetchingPlan = new ExecutionPlan<>(dependencyGraph, (node) -> {
      QueryScope scope = node.queryScope;
      int limit = scope.equals(queryResolver.getScope()) ? preparedQuery.limit : preparedQuery.limit + 1; // limit + 1 to detect when results can be wrong
      prefetchQueryScopeByQueryScope.computeIfAbsent(scope, k -> scope.copyWithNewLimit(limit));
      measuresByQueryScope.computeIfAbsent(scope, k -> new HashSet<>()).add(node.measure);
    });
    prefetchingPlan.execute();

    Map<QueryScope, Table> tableByScope = new HashMap<>();
    for (QueryScope scope : prefetchQueryScopeByQueryScope.keySet()) {
      QueryScope prefetchQueryScope = prefetchQueryScopeByQueryScope.get(scope);
      Set<CompiledMeasure> measures = measuresByQueryScope.get(scope);
      QueryCache.QueryCacheKey queryCacheKey = new QueryCache.QueryCacheKey(scope, user);
      QueryCache queryCache = getQueryCache((QueryCacheParameter) preparedQuery.parameters.getOrDefault(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.USE)), user);

      Set<CompiledMeasure> measuresToExcludeFromCache = new HashSet<>(); // the measures not to put in cache
      Set<CompiledMeasure> cached = new HashSet<>();
      Set<CompiledMeasure> notCached = new HashSet<>();
      for (CompiledMeasure measure : measures) {
        if (MeasureUtils.isPrimitive(measure)) {
          if (!canBeCached(measure, scope)) {
            measuresToExcludeFromCache.add(measure);
          } else if (queryCache.contains(measure, queryCacheKey)) {
            cached.add(measure);
          } else {
            notCached.add(measure);
          }
        }
      }
      notCached.addAll(measuresToExcludeFromCache);

      Table result;
      if (!notCached.isEmpty()) {
        notCached.add(COMPILED_COUNT);
        result = this.queryEngine.execute(new DatabaseQuery(prefetchQueryScope, new ArrayList<>(notCached)));
        result = TableUtils.replaceNullCellsByTotal(result, scope);
      } else {
        // Create an empty result that will be populated by the query cache
        result = queryCache.createRawResult(queryCacheKey);
      }

      queryCache.contributeToResult(result, cached, queryCacheKey);
      Set<CompiledMeasure> measuresToCache = notCached.stream().filter(m -> !measuresToExcludeFromCache.contains(m)).collect(Collectors.toSet());
      queryCache.contributeToCache(result, measuresToCache, queryCacheKey);

      // The table in the cache contains null values for totals but in this map, we need to replace the nulls with totals
      tableByScope.put(scope, result);
    }

    if (preparedQuery.columnSets.containsKey(GROUP)) {
      // Apply this as it modifies the "shape" of the result
      GroupColumnSetDto columnSet = (GroupColumnSetDto) preparedQuery.columnSets.get(GROUP);
      // Reshape all results
      tableByScope.replaceAll((scope, table) -> GrouperExecutor.group(table, columnSet));
    }

    // Here we take the global plan and execute the plans for a given scope one by one, in dependency order. The order
    // is given by the graph itself.
    Set<QueryPlanNodeKey> visited = new HashSet<>();
    Evaluator evaluator = new Evaluator();
    ExecutionPlan<QueryPlanNodeKey> globalPlan = new ExecutionPlan<>(dependencyGraph, (queryNode) -> {
      if (visited.add(queryNode)) {
        ExecutionContext executionContext = new ExecutionContext(queryNode.queryScope,
                tableByScope,
                queryResolver.getColumns(),
                queryResolver.getGroupColumns(),
                queryResolver.getCompiledColumnSets(),
                preparedQuery.limit);
        evaluator.accept(queryNode, executionContext);
      }
    });
    globalPlan.execute();

    Table result = tableByScope.get(queryResolver.getScope());

    if (limitNotifier != null && result.count() == preparedQuery.limit) {
      limitNotifier.accept(preparedQuery.limit);
    }

    if (replaceTotalCellsAndOrderRows) {
      result = TableUtils.replaceTotalCellValues((ColumnarTable) result, !preparedQuery.rollupColumns.isEmpty());
      result = TableUtils.orderRows((ColumnarTable) result, Queries.getComparators(preparedQuery), preparedQuery.columnSets.values());
    }
    // Use `query` and not `preparedQuery` here because `preparedQuery` can have additional columns in case of orderBy.
    // This is also why `selectAndOrderColumns()` is executed after `orderRows()`.
    result = TableUtils.selectAndOrderColumns(query, (ColumnarTable) result);

    CacheStatsDto stats = this.queryCache.stats(user);
    cacheStatsDtoBuilder
            .hitCount(stats.hitCount)
            .evictionCount(stats.evictionCount)
            .missCount(stats.missCount);

    return result;
  }

  private static QueryDto prepareQuery(QueryDto query, PivotTableContext pivotTableContext) {
    QueryDto deepCopy = query.clone();
    deepCopy.limit = query.limit < 0 ? LIMIT_DEFAULT_VALUE : query.limit;

    if (deepCopy.orders != null) {
      // Fields in orderBy need to be in groupBy. Because orderBy is also executed by SquashQL, the column needs to be
      // also added to the select. Those additional columns will eventually be discarded later on.
      deepCopy.orders.keySet().forEach(c -> {
        Set<String> measureAliases = deepCopy.measures.stream().map(Measure::alias).collect(Collectors.toSet());
        if (!deepCopy.columns.contains(c) && !measureAliases.contains(SqlUtils.squashqlExpression(c))) {
          deepCopy.columns.add(c);
        }
      });
    }

    if (deepCopy.measures != null) {
      PartialMeasureVisitor visitor = new PartialMeasureVisitor(pivotTableContext);
      deepCopy.measures.replaceAll(m -> m.accept(visitor));
    }

    return deepCopy;
  }

  private static boolean canBeCached(CompiledMeasure measure, QueryScope scope) {
    // Make sure to never cache the grouping measures. It could cause issue in some cases because the result from the DB
    // needs to always have __total__ instead of null before transferring values from cache to result.
    if (generateGroupingMeasures(scope).containsValue(measure)) {
      return false;
    }
    // In case of vectors, we can rely only on the alias of the measure.
    return SqlUtils.extractFieldFromGroupingAlias(measure.alias()) == null;
  }

  private static DependencyGraph<QueryPlanNodeKey> computeDependencyGraph(
          List<TypedField> columns,
          List<TypedField> groupColumns,
          Collection<CompiledMeasure> measures,
          QueryScope queryScope) {
    GraphDependencyBuilder<QueryPlanNodeKey> builder = new GraphDependencyBuilder<>(nodeKey -> {
      Map<QueryScope, Set<CompiledMeasure>> dependencies = nodeKey.measure.accept(new PrefetchVisitor(columns, groupColumns, nodeKey.queryScope));
      Set<QueryPlanNodeKey> set = new HashSet<>();
      for (Map.Entry<QueryScope, Set<CompiledMeasure>> entry : dependencies.entrySet()) {
        QueryScope key = entry.getKey();
        for (CompiledMeasure measure : entry.getValue()) {
          set.add(new QueryPlanNodeKey(key, measure));
        }

        Collection<CompiledMeasure> additionalMeasures = generateGroupingMeasures(key).values();
        for (CompiledMeasure measure : additionalMeasures) {
          set.add(new QueryPlanNodeKey(key, measure));
        }
      }
      return set;
    });
    Set<CompiledMeasure> queriedMeasures = new HashSet<>(measures);
    queriedMeasures.add(COMPILED_COUNT);
    queriedMeasures.addAll(generateGroupingMeasures(queryScope).values());
    return builder.build(queriedMeasures.stream().map(m -> new QueryPlanNodeKey(queryScope, m)).toList());
  }

  public record QueryPlanNodeKey(QueryScope queryScope, CompiledMeasure measure) {
  }

  public record ExecutionContext(QueryScope queryScope,
                                 Map<QueryScope, Table> tableByScope,
                                 List<TypedField> columns,
                                 List<TypedField> groupColumns,
                                 Map<ColumnSetKey, CompiledColumnSet> columnSets,
                                 int queryLimit) {
    public Table getWriteToTable() {
      return this.tableByScope.get(this.queryScope);
    }
  }

  public PivotTable executePivotQueryMerge(PivotTableQueryMergeDto pivotTableQueryMergeDto, SquashQLUser user) {
    return QueryMergeExecutor.executePivotQueryMerge(this, pivotTableQueryMergeDto, user);
  }

  public Table executeQueryMerge(QueryMergeDto queryMerge, SquashQLUser user) {
    return QueryMergeExecutor.executeQueryMerge(this, queryMerge, user);
  }

  public Table executeExperimentalQueryMerge(QueryJoinDto queryJoin) {
    return new ExperimentalQueryJoinExecutor(this.queryEngine).execute(queryJoin);
  }

  /**
   * Generates grouping measures based on the provided query scope.
   *
   * @param queryScope The query scope containing rollup columns and grouping sets.
   * @return A map of compiled measures, where the key is the squashql expression of the fields used in the rollup and
   * the value is the compiled measure.
   */
  public static Map<String, CompiledMeasure> generateGroupingMeasures(QueryScope queryScope) {
    Map<String, CompiledMeasure> measures = new HashMap<>();
    List<TypedField> rollups = new ArrayList<>();
    rollups.addAll(queryScope.rollup());
    rollups.addAll(queryScope.groupingSets()
            .stream()
            .flatMap(Collection::stream)
            .toList());
    if (!rollups.isEmpty()) {
      rollups.forEach(f -> {
        String expression = SqlUtils.squashqlExpression(f);
        measures.put(expression, new CompiledAggregatedMeasure(SqlUtils.groupingAlias(expression.replace(".", "_")), f, GROUPING, null, false));
      });
    }
    return measures;
  }

  public static PivotTableContext createPivotTableContext(QueryDto query) {
    return new PivotTableContext(new PivotTableQueryDto(query, query.columns, Collections.emptyList(), Collections.emptyList()));
  }
}
