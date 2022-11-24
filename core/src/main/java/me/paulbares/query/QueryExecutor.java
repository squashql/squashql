package me.paulbares.query;

import com.google.common.graph.Graph;
import lombok.extern.slf4j.Slf4j;
import me.paulbares.MeasurePrefetcherVisitor;
import me.paulbares.query.QueryCache.SubQueryScope;
import me.paulbares.query.QueryCache.TableScope;
import me.paulbares.query.context.QueryCacheContextValue;
import me.paulbares.query.database.DatabaseQuery;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.*;
import me.paulbares.query.monitoring.QueryWatch;
import me.paulbares.store.Field;
import me.paulbares.util.Queries;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static me.paulbares.query.ColumnSetKey.BUCKET;

@Slf4j
public class QueryExecutor {

  public final QueryEngine queryEngine;
  public final QueryCache queryCache;

  public QueryExecutor(QueryEngine queryEngine) {
    this(queryEngine, new CaffeineQueryCache());
  }

  public QueryExecutor(QueryEngine queryEngine, QueryCache cache) {
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

  public Table execute(QueryDto query) {
    return execute(
            query,
            new QueryWatch(),
            CacheStatsDto.builder());
  }

  public Table execute(QueryDto query, QueryWatch queryWatch, CacheStatsDto.CacheStatsDtoBuilder cacheStatsDtoBuilder) {
    queryWatch.start(QueryWatch.GLOBAL);
    queryWatch.start(QueryWatch.PREPARE_PLAN);

    queryWatch.start(QueryWatch.PREPARE_RESOLVE_MEASURES);
    resolveMeasures(query);
    queryWatch.stop(QueryWatch.PREPARE_RESOLVE_MEASURES);

    queryWatch.start(QueryWatch.EXECUTE_PREFETCH_PLAN);
    Function<String, Field> fieldSupplier = this.queryEngine.getFieldSupplier();
    QueryScope queryScope = createQueryScope(query, fieldSupplier);
    Graph<GraphDependencyBuilder.NodeWithId<QueryPlanNodeKey>> graph = computeDependencyGraph(query, queryScope, fieldSupplier);
    // Compute what needs to be prefetched
    Map<QueryScope, DatabaseQuery> prefetchQueryByQueryScope = new HashMap<>();
    Map<QueryScope, Set<Measure>> measuresByQueryScope = new HashMap<>();
    ExecutionPlan<QueryPlanNodeKey, Void> prefetchingPlan = new ExecutionPlan<>(graph, (node, v) -> {
      QueryScope scope = node.queryScope;
      prefetchQueryByQueryScope.computeIfAbsent(scope, k -> Queries.queryScopeToDatabaseQuery(scope));
      measuresByQueryScope.computeIfAbsent(scope, k -> new HashSet<>()).add(node.measure);
    });
    prefetchingPlan.execute(null);
    queryWatch.stop(QueryWatch.EXECUTE_PREFETCH_PLAN);

    queryWatch.start(QueryWatch.PREFETCH);
    Map<QueryScope, Table> tableByScope = new HashMap<>();
    for (QueryScope scope : prefetchQueryByQueryScope.keySet()) {
      DatabaseQuery prefetchQuery = prefetchQueryByQueryScope.get(scope);
      Set<Measure> measures = measuresByQueryScope.get(scope);
      QueryCache.PrefetchQueryScope prefetchQueryScope = createPrefetchQueryScope(scope, prefetchQuery, fieldSupplier);
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
      if (!notCached.isEmpty() || (cached.isEmpty() && notCached.isEmpty())) {
        if (!primitives.contains(CountMeasure.INSTANCE)) {
          // Always add count
          notCached.add(CountMeasure.INSTANCE);
        }
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

    Table result = tableByScope.get(queryScope);
    ExecutionPlan<QueryPlanNodeKey, ExecutionContext> plan = new ExecutionPlan<>(graph, new MeasureEvaluator(fieldSupplier));
    plan.execute(new ExecutionContext(result, queryScope, tableByScope, query, queryWatch));

    queryWatch.stop(QueryWatch.EXECUTE_EVALUATION_PLAN);

    queryWatch.start(QueryWatch.ORDER);

    result = TableUtils.selectAndOrderColumns((ColumnarTable) result, query);
    result = TableUtils.replaceTotalCellValues((ColumnarTable) result, query);
    result = TableUtils.orderRows((ColumnarTable) result, query);

    queryWatch.stop(QueryWatch.ORDER);
    queryWatch.stop(QueryWatch.GLOBAL);

    CacheStatsDto stats = this.queryCache.stats();
    cacheStatsDtoBuilder
            .hitCount(stats.hitCount)
            .evictionCount(stats.evictionCount)
            .missCount(stats.missCount);
    return result;
  }

  private static Graph<GraphDependencyBuilder.NodeWithId<QueryPlanNodeKey>> computeDependencyGraph(
          QueryDto query,
          QueryScope queryScope,
          Function<String, Field> fieldSupplier) {
    MeasurePrefetcherVisitor visitor = new MeasurePrefetcherVisitor(query, queryScope, fieldSupplier);
    GraphDependencyBuilder<QueryPlanNodeKey> builder = new GraphDependencyBuilder<>(nodeKey -> {
      Map<QueryScope, Set<Measure>> dependencies = nodeKey.measure.accept(visitor);
      Set<QueryPlanNodeKey> set = new HashSet<>();
      for (Map.Entry<QueryScope, Set<Measure>> entry : dependencies.entrySet()) {
        for (Measure measure : entry.getValue()) {
          set.add(new QueryPlanNodeKey(entry.getKey(), measure));
        }
      }
      return set;
    });
    Set<Measure> queriedMeasures = new HashSet<>(query.measures);
    queriedMeasures.add(CountMeasure.INSTANCE); // Always add count
    return builder.build(queriedMeasures.stream().map(m -> new QueryPlanNodeKey(queryScope, m)).toList());
  }

  public static QueryScope createQueryScope(QueryDto query, Function<String, Field> fieldSupplier) {
    // If column set, it changes the scope
    List<Field> columns = Stream.concat(
            query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()),
            query.columns.stream()).map(fieldSupplier).toList();
    List<Field> rollupColumns = query.rollupColumns.stream().map(fieldSupplier).toList();
    return new QueryScope(query.table, query.subQuery, columns, query.criteriaDto, rollupColumns);
  }

  private static QueryCache.PrefetchQueryScope createPrefetchQueryScope(QueryScope queryScope, DatabaseQuery prefetchQuery, Function<String, Field> fieldSupplier) {
    Set<Field> fields = prefetchQuery.select.stream().map(fieldSupplier).collect(Collectors.toSet());
    if (queryScope.tableDto != null) {
      return new TableScope(queryScope.tableDto, fields, queryScope.criteriaDto, new HashSet<>(queryScope.rollupColumns));
    } else {
      return new SubQueryScope(queryScope.subQuery, fields, queryScope.criteriaDto);
    }
  }

  public record QueryScope(TableDto tableDto,
                           QueryDto subQuery,
                           List<Field> columns,
                           CriteriaDto criteriaDto,
                           List<Field> rollupColumns) {
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

  public static Function<String, Field> withFallback(Function<String, Field> fieldProvider, Class<?> fallbackType) {
    return fieldName -> {
      Field f;
      try {
        f = fieldProvider.apply(fieldName);
      } catch (Exception e) {
        // This can happen if the using a "field" coming from the calculation of a subquery. Since the field provider
        // contains only "raw" fields, it will throw an exception.
        log.info("Cannot find field " + fieldName + " with default field provider, fallback to default type: " + fallbackType.getSimpleName());
        f = new Field(fieldName, Number.class);
      }
      return f;
    };
  }
}
