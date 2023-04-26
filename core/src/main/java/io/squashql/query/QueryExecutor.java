package io.squashql.query;

import io.squashql.PrefetchVisitor;
import io.squashql.query.QueryCache.SubQueryScope;
import io.squashql.query.QueryCache.TableScope;
import io.squashql.query.context.QueryCacheContextValue;
import io.squashql.query.database.AQueryEngine;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.dto.*;
import io.squashql.query.monitoring.QueryWatch;
import io.squashql.store.Field;
import io.squashql.store.Store;
import io.squashql.table.MergeTables;
import io.squashql.util.Queries;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.squashql.query.ColumnSetKey.BUCKET;

@Slf4j
public class QueryExecutor {

  public static final int LIMIT_DEFAULT_VALUE = Integer.parseInt(System.getProperty("query.limit", Integer.toString(10_000)));
  public final QueryEngine<?> queryEngine;
  public final QueryCache queryCache;

  public QueryExecutor(QueryEngine<?> queryEngine) {
    this(queryEngine, new CaffeineQueryCache());
  }

  public QueryExecutor(QueryEngine<?> queryEngine, QueryCache cache) {
    this.queryEngine = queryEngine;
    this.queryCache = cache;
  }

  private QueryCache getQueryCache(QueryCacheContextValue queryCacheContextValue) {
    return switch (queryCacheContextValue.action) {
      case USE -> this.queryCache;
      case NOT_USE -> EmptyQueryCache.INSTANCE;
      case INVALIDATE -> {
        this.queryCache.clear();
        yield this.queryCache;
      }
    };
  }

  public Table execute(String rawSqlQuery) {
    return this.queryEngine.executeRawSql(rawSqlQuery);
  }

  public Table execute(QueryDto query) {
    return execute(
            query,
            new QueryWatch(),
            CacheStatsDto.builder(),
            null,
            true);
  }

  public Table execute(QueryDto query,
                       QueryWatch queryWatch,
                       CacheStatsDto.CacheStatsDtoBuilder cacheStatsDtoBuilder,
                       SquashQLUser user,
                       boolean replaceTotalCellsAndOrderRows) {
    int queryLimit = query.limit < 0 ? LIMIT_DEFAULT_VALUE : query.limit;
    queryWatch.start(QueryWatch.GLOBAL);
    queryWatch.start(QueryWatch.PREPARE_PLAN);

    queryWatch.start(QueryWatch.PREPARE_RESOLVE_MEASURES);
    resolveMeasures(query);
    queryWatch.stop(QueryWatch.PREPARE_RESOLVE_MEASURES);

    queryWatch.start(QueryWatch.EXECUTE_PREFETCH_PLAN);
    Function<String, Field> fieldSupplier = createQueryFieldSupplier(this.queryEngine, query.virtualTableDto);
    QueryScope queryScope = createQueryScope(query, fieldSupplier);
    Pair<DependencyGraph<QueryPlanNodeKey>, DependencyGraph<QueryScope>> dependencyGraph = computeDependencyGraph(query, queryScope, fieldSupplier);
    // Compute what needs to be prefetched
    Map<QueryScope, DatabaseQuery> prefetchQueryByQueryScope = new HashMap<>();
    Map<QueryScope, Set<Measure>> measuresByQueryScope = new HashMap<>();
    ExecutionPlan<QueryPlanNodeKey, Void> prefetchingPlan = new ExecutionPlan<>(dependencyGraph.getOne(), (node, v) -> {
      QueryScope scope = node.queryScope;
      int limit = scope.equals(queryScope) ? queryLimit : queryLimit + 1; // limit + 1 to detect when results can be wrong
      prefetchQueryByQueryScope.computeIfAbsent(scope, k -> Queries.queryScopeToDatabaseQuery(scope, fieldSupplier, limit));
      measuresByQueryScope.computeIfAbsent(scope, k -> new HashSet<>()).add(node.measure);
    });
    prefetchingPlan.execute(null);
    queryWatch.stop(QueryWatch.EXECUTE_PREFETCH_PLAN);

    queryWatch.start(QueryWatch.PREFETCH);
    Map<QueryScope, Table> tableByScope = new HashMap<>();
    for (QueryScope scope : prefetchQueryByQueryScope.keySet()) {
      DatabaseQuery prefetchQuery = prefetchQueryByQueryScope.get(scope);
      Set<Measure> measures = measuresByQueryScope.get(scope);
      QueryCache.PrefetchQueryScope prefetchQueryScope = createPrefetchQueryScope(scope, prefetchQuery, user);
      QueryCache queryCache = getQueryCache((QueryCacheContextValue) query.context.getOrDefault(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.USE)));

      // Finish to prepare the query
      Set<Measure> cached = new HashSet<>();
      Set<Measure> notCached = new HashSet<>();
      Set<Measure> primitives = measures.stream().filter(MeasureUtils::isPrimitive).collect(Collectors.toSet());
      for (Measure primitive : primitives) {
        if (queryCache.contains(primitive, prefetchQueryScope)) {
          cached.add(primitive);
        } else {
          notCached.add(primitive);
        }
      }

      Table result;
      if (!notCached.isEmpty()) {
        notCached.add(CountMeasure.INSTANCE); // Always add count
        notCached.forEach(prefetchQuery::withMeasure);
        result = this.queryEngine.execute(prefetchQuery);
      } else {
        // Create an empty result that will be populated by the query cache
        result = queryCache.createRawResult(prefetchQueryScope);
      }

      queryCache.contributeToResult(result, cached, prefetchQueryScope);
      queryCache.contributeToCache(result, notCached, prefetchQueryScope);

      tableByScope.put(scope, result);
    }
    queryWatch.stop(QueryWatch.PREFETCH);

    queryWatch.start(QueryWatch.BUCKET);
    if (query.columnSets.containsKey(BUCKET)) {
      // Apply this as it modifies the "shape" of the result
      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(BUCKET);
      // Reshape all results
      tableByScope.replaceAll((scope, table) -> BucketerExecutor.bucket(table, columnSet));
    }
    queryWatch.stop(QueryWatch.BUCKET);

    queryWatch.start(QueryWatch.EXECUTE_EVALUATION_PLAN);

    // Here we take the global plan and execute the plans for a given scope one by one, in dependency order. The order
    // is given by the graph itself.
    ExecutionPlan<QueryScope, Void> globalPlan = new ExecutionPlan<>(dependencyGraph.getTwo(), (scope, context) -> {
      ExecutionPlan<QueryPlanNodeKey, ExecutionContext> scopedPlan = new ExecutionPlan<>(dependencyGraph.getOne(), new Evaluator(fieldSupplier));
      scopedPlan.execute(new ExecutionContext(tableByScope.get(scope), scope, tableByScope, query, queryWatch));
    });
    globalPlan.execute(null);

    queryWatch.stop(QueryWatch.EXECUTE_EVALUATION_PLAN);

    queryWatch.start(QueryWatch.ORDER);

    Table result = tableByScope.get(queryScope);
    result = TableUtils.selectAndOrderColumns((ColumnarTable) result, query);
    if (replaceTotalCellsAndOrderRows) {
      result = TableUtils.replaceTotalCellValues((ColumnarTable) result, !query.rollupColumns.isEmpty());
      result = TableUtils.orderRows((ColumnarTable) result, Queries.getComparators(query), query.columnSets.values());
    }

    queryWatch.stop(QueryWatch.ORDER);
    queryWatch.stop(QueryWatch.GLOBAL);

    CacheStatsDto stats = this.queryCache.stats();
    cacheStatsDtoBuilder
            .hitCount(stats.hitCount)
            .evictionCount(stats.evictionCount)
            .missCount(stats.missCount);
    return result;
  }

  private static Pair<DependencyGraph<QueryPlanNodeKey>, DependencyGraph<QueryScope>> computeDependencyGraph(
          QueryDto query,
          QueryScope queryScope,
          Function<String, Field> fieldSupplier) {
    // This graph is used to keep track of dependency between execution plans. An Execution Plan is bound to a given scope.
    DependencyGraph<QueryScope> executionGraph = new DependencyGraph<>();

    GraphDependencyBuilder<QueryPlanNodeKey> builder = new GraphDependencyBuilder<>(nodeKey -> {
      Map<QueryScope, Set<Measure>> dependencies = nodeKey.measure.accept(new PrefetchVisitor(query, nodeKey.queryScope, fieldSupplier));
      Set<QueryPlanNodeKey> set = new HashSet<>();
      executionGraph.addNode(nodeKey.queryScope);
      for (Map.Entry<QueryScope, Set<Measure>> entry : dependencies.entrySet()) {

        executionGraph.addNode(entry.getKey());
        if (!nodeKey.queryScope.equals(entry.getKey())) {
          executionGraph.putEdge(nodeKey.queryScope, entry.getKey());
        }

        for (Measure measure : entry.getValue()) {
          set.add(new QueryPlanNodeKey(entry.getKey(), measure));
        }
      }
      return set;
    });
    Set<Measure> queriedMeasures = new HashSet<>(query.measures);
    queriedMeasures.add(CountMeasure.INSTANCE); // Always add count
    return Tuples.pair(
            builder.build(queriedMeasures.stream().map(m -> new QueryPlanNodeKey(queryScope, m)).toList()),
            executionGraph);
  }

  public static QueryScope createQueryScope(QueryDto query, Function<String, Field> fieldSupplier) {
    // If column set, it changes the scope
    List<Field> columns = Stream.concat(
            query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()),
            query.columns.stream()).map(fieldSupplier).collect(Collectors.toCollection(ArrayList::new));
    List<Field> rollupColumns = query.rollupColumns.stream().map(fieldSupplier).toList();
    return new QueryScope(query.table, query.subQuery, columns, query.whereCriteriaDto, query.havingCriteriaDto, rollupColumns, query.virtualTableDto);
  }

  private static QueryCache.PrefetchQueryScope createPrefetchQueryScope(
          QueryScope queryScope,
          DatabaseQuery prefetchQuery,
          SquashQLUser user) {
    Set<Field> fields = new HashSet<>(prefetchQuery.select);
    if (queryScope.tableDto != null) {
      return new TableScope(queryScope.tableDto,
              fields,
              queryScope.whereCriteriaDto,
              queryScope.havingCriteriaDto,
              queryScope.rollupColumns,
              queryScope.virtualTableDto,
              user,
              prefetchQuery.limit);
    } else {
      return new SubQueryScope(queryScope.subQuery,
              fields,
              queryScope.whereCriteriaDto,
              queryScope.havingCriteriaDto,
              user,
              prefetchQuery.limit);
    }
  }

  public record QueryScope(TableDto tableDto,
                           QueryDto subQuery,
                           List<Field> columns,
                           CriteriaDto whereCriteriaDto,
                           CriteriaDto havingCriteriaDto,
                           List<Field> rollupColumns,
                           VirtualTableDto virtualTableDto) {
  }

  public record QueryPlanNodeKey(QueryScope queryScope, Measure measure) {
  }

  public record ExecutionContext(Table writeToTable,
                                 QueryScope queryScope,
                                 Map<QueryScope, Table> tableByScope,
                                 QueryDto query,
                                 QueryWatch queryWatch) {
  }

  protected static void resolveMeasures(QueryDto queryDto) {
    // Deactivate for now.
  }

  public Table execute(QueryDto first, QueryDto second, SquashQLUser user) {
    Map<String, Comparator<?>> firstComparators = Queries.getComparators(first);
    Map<String, Comparator<?>> secondComparators = Queries.getComparators(second);
    secondComparators.putAll(firstComparators); // the comparators of the first query take precedence over the second's

    Set<ColumnSet> columnSets = Stream.concat(first.columnSets.values().stream(), second.columnSets.values().stream())
            .collect(Collectors.toSet());

    Function<QueryDto, Table> execute = q -> execute(
            q,
            new QueryWatch(),
            CacheStatsDto.builder(),
            user,
            false);
    CompletableFuture<Table> f1 = CompletableFuture.supplyAsync(() -> execute.apply(first));
    CompletableFuture<Table> f2 = CompletableFuture.supplyAsync(() -> execute.apply(second));
    return CompletableFuture.allOf(f1, f2).thenApply(__ -> merge(f1.join(), f2.join(), secondComparators, columnSets)).join();
  }

  public static Table merge(Table table1, Table table2, Map<String, Comparator<?>> comparators, Set<ColumnSet> columnSets) {
    ColumnarTable table = (ColumnarTable) MergeTables.mergeTables(table1, table2);
    table = (ColumnarTable) TableUtils.orderRows(table, comparators, columnSets);
    return TableUtils.replaceTotalCellValues(table, true);
  }

  public static Function<String, Field> createQueryFieldSupplier(QueryEngine<?> queryEngine, VirtualTableDto vt) {
    Map<String, Store> storesByName = new HashMap<>(queryEngine.datastore().storesByName());
    if (vt != null) {
      storesByName.put(vt.name, VirtualTableDto.toStore(vt));
    }
    return AQueryEngine.createFieldSupplier(storesByName);
  }
}
