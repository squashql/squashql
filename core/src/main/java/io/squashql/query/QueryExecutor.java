package io.squashql.query;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.QueryCache.SubQueryScope;
import io.squashql.query.QueryCache.TableScope;
import io.squashql.query.compiled.*;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.dto.*;
import io.squashql.query.parameter.QueryCacheParameter;
import io.squashql.table.ColumnarTable;
import io.squashql.table.PivotTable;
import io.squashql.table.Table;
import io.squashql.table.TableUtils;
import io.squashql.type.TypedField;
import io.squashql.util.Queries;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import static io.squashql.query.ColumnSetKey.BUCKET;

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
    QueryDto preparedQuery = prepareQuery(pivotTableQueryDto.query, pivotTableContext);
    Table result = executeQuery(preparedQuery, cacheStatsDtoBuilder, user, false, limitNotifier);
    if (replaceTotalCellsAndOrderRows) {
      result = TableUtils.replaceTotalCellValues((ColumnarTable) result,
              pivotTableQueryDto.rows.stream().map(Field::name).toList(),
              pivotTableQueryDto.columns.stream().map(Field::name).toList());
      result = TableUtils.orderRows((ColumnarTable) result, Queries.getComparators(preparedQuery), preparedQuery.columnSets.values());
    }

    List<String> values = pivotTableQueryDto.query.measures.stream().map(Measure::alias).toList();
    return new PivotTable(result, pivotTableQueryDto.rows.stream().map(Field::name).toList(), pivotTableQueryDto.columns.stream().map(Field::name).toList(), values);
  }

  private static QueryDto prepareQuery(QueryDto query, PivotTableContext context) {
    Set<Field> axes = new HashSet<>(context.rows);
    axes.addAll(context.columns);
    Set<Field> select = new HashSet<>(query.columns);
    select.addAll(query.columnSets.values().stream().flatMap(cs -> cs.getNewColumns().stream()).collect(Collectors.toSet()));
    axes.removeAll(select);

    if (!axes.isEmpty()) {
      throw new IllegalArgumentException(axes.stream().map(Field::name).toList() + " on rows or columns by not in select. Please add those fields in select");
    }
    axes = new HashSet<>(context.rows);
    axes.addAll(context.columns);
    select.removeAll(axes);
    if (!select.isEmpty()) {
      throw new IllegalArgumentException(select.stream().map(Field::name).toList() + " in select but not on rows or columns. Please add those fields on one axis");
    }

    List<Field> rows = context.cleansedRows;
    List<Field> columns = context.cleansedColumns;
    List<List<Field>> groupingSets = new ArrayList<>();
    // GT use an empty list instead of list of size 1 with an empty string because could cause issue later on with FieldSupplier
    groupingSets.add(List.of());
    // Rows
    for (int i = rows.size(); i >= 1; i--) {
      groupingSets.add(rows.subList(0, i));
    }

    // Cols
    for (int i = columns.size(); i >= 1; i--) {
      groupingSets.add(columns.subList(0, i));
    }

    // all combinations
    for (int i = rows.size(); i >= 1; i--) {
      for (int j = columns.size(); j >= 1; j--) {
        List<Field> all = new ArrayList<>(rows.subList(0, i));
        all.addAll(columns.subList(0, j));
        groupingSets.add(all);
      }
    }

    QueryDto deepCopy = JacksonUtil.deserialize(JacksonUtil.serialize(query), QueryDto.class);
    deepCopy.groupingSets = groupingSets;
    return deepCopy;
  }

  public Table executeRaw(String rawSqlQuery) {
    return this.queryEngine.executeRawSql(rawSqlQuery);
  }

  public Table executeQuery(QueryDto query) {
    return executeQuery(
            query,
            CacheStatsDto.builder(),
            null,
            true,
            null);
  }

  public Table executeQuery(QueryDto query,
                            CacheStatsDto.CacheStatsDtoBuilder cacheStatsDtoBuilder,
                            SquashQLUser user,
                            boolean replaceTotalCellsAndOrderRows,
                            IntConsumer limitNotifier) {
    int queryLimit = query.limit < 0 ? LIMIT_DEFAULT_VALUE : query.limit;

    final QueryResolver queryResolver = new QueryResolver(query, new HashMap<>(this.queryEngine.datastore().storesByName()));
    DependencyGraph<QueryPlanNodeKey> dependencyGraph = computeDependencyGraph(
            queryResolver.getColumns(), queryResolver.getBucketColumns(), queryResolver.getScope());
    // Compute what needs to be prefetched
    Map<QueryScope, DatabaseQuery2> prefetchQueryByQueryScope = new HashMap<>();
    Map<QueryScope, Set<CompiledMeasure>> measuresByQueryScope = new HashMap<>();
    ExecutionPlan<QueryPlanNodeKey> prefetchingPlan = new ExecutionPlan<>(dependencyGraph, (node) -> {
      QueryScope scope = node.queryScope;
      int limit = scope.equals(queryResolver.getScope()) ? queryLimit : queryLimit + 1; // limit + 1 to detect when results can be wrong
      prefetchQueryByQueryScope.computeIfAbsent(scope, k -> queryResolver.toDatabaseQuery(scope, limit));
      measuresByQueryScope.computeIfAbsent(scope, k -> new HashSet<>()).add(node.measure);
    });
    prefetchingPlan.execute();

    Map<QueryScope, Table> tableByScope = new HashMap<>();
    for (QueryScope scope : prefetchQueryByQueryScope.keySet()) {
      DatabaseQuery2 prefetchQuery = prefetchQueryByQueryScope.get(scope);
      Set<CompiledMeasure> measures = measuresByQueryScope.get(scope);
      QueryCache.PrefetchQueryScope prefetchQueryScope = createPrefetchQueryScope(scope, prefetchQuery, user);
      QueryCache queryCache = getQueryCache((QueryCacheParameter) query.parameters.getOrDefault(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.USE)), user);

      Set<Measure> cached = new HashSet<>();
      Map<Measure, CompiledMeasure> notCached = new HashMap<>();
      for (CompiledMeasure measure : measures) {
        if (MeasureUtils.isPrimitive(measure)) {
          if (queryCache.contains(measure.measure(), prefetchQueryScope)) {
            cached.add(measure.measure());
          } else {
            notCached.put(measure.measure(), measure);
          }
        }
      }

      Table result;
      if (!notCached.isEmpty()) {
//        notCached.add(CountMeasure.INSTANCE); // Always add count todo-mde
        notCached.forEach((k, v) -> prefetchQuery.withMeasure(v));
        result = this.queryEngine.execute(prefetchQuery);
      } else {
        // Create an empty result that will be populated by the query cache
        result = queryCache.createRawResult(prefetchQueryScope);
      }

      queryCache.contributeToResult(result, cached, prefetchQueryScope);
      queryCache.contributeToCache(result, notCached.keySet(), prefetchQueryScope);

      tableByScope.put(scope, result);
    }

    if (query.columnSets.containsKey(BUCKET)) {
      // Apply this as it modifies the "shape" of the result
      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(BUCKET);
      // Reshape all results
      tableByScope.replaceAll((scope, table) -> BucketerExecutor.bucket(table, columnSet));
    }

    // Here we take the global plan and execute the plans for a given scope one by one, in dependency order. The order
    // is given by the graph itself.
    final Set<QueryPlanNodeKey> visited = new HashSet<>();
    final Evaluator evaluator = new Evaluator();
    ExecutionPlan<QueryPlanNodeKey> globalPlan = new ExecutionPlan<>(dependencyGraph, (queryNode) -> {
      if (visited.add(queryNode)) {
        final ExecutionContext executionContext = new ExecutionContext(queryNode.queryScope, tableByScope, queryResolver.getColumns(), queryResolver.getBucketColumns(), queryLimit);
        evaluator.accept(queryNode, executionContext);
      }
    });
    globalPlan.execute();

    Table result = tableByScope.get(queryResolver.getScope());

    if (limitNotifier != null && result.count() == queryLimit) {
      limitNotifier.accept(queryLimit);
    }

    result = TableUtils.selectAndOrderColumns((ColumnarTable) result, query);
    if (replaceTotalCellsAndOrderRows) {
      result = TableUtils.replaceTotalCellValues((ColumnarTable) result, !query.rollupColumns.isEmpty());
      result = TableUtils.orderRows((ColumnarTable) result, Queries.getComparators(query), query.columnSets.values());
    }

    CacheStatsDto stats = this.queryCache.stats(user);
    cacheStatsDtoBuilder
            .hitCount(stats.hitCount)
            .evictionCount(stats.evictionCount)
            .missCount(stats.missCount);
    return result;
  }

  private static DependencyGraph<QueryPlanNodeKey> computeDependencyGraph(
          List<TypedField> columns,
          List<TypedField> bucketColumns,
          QueryScope queryScope) {
    GraphDependencyBuilder<QueryPlanNodeKey> builder = new GraphDependencyBuilder<>(nodeKey -> {
      Map<QueryScope, Set<CompiledMeasure>> dependencies = nodeKey.measure.accept(new PrefetchVisitor(columns, bucketColumns, nodeKey.queryScope));
      Set<QueryPlanNodeKey> set = new HashSet<>();
      for (Map.Entry<QueryScope, Set<CompiledMeasure>> entry : dependencies.entrySet()) {
        for (CompiledMeasure measure : entry.getValue()) {
          set.add(new QueryPlanNodeKey(entry.getKey(), measure));
        }
      }
      return set;
    });
    Set<CompiledMeasure> queriedMeasures = new HashSet<>(queryScope.measures);
//    queriedMeasures.add(CountMeasure.INSTANCE); // Always add count todo-mde
    return builder.build(queriedMeasures.stream().map(m -> new QueryPlanNodeKey(queryScope, m)).toList());
  }

  private static QueryCache.PrefetchQueryScope createPrefetchQueryScope(
          QueryScope queryScope,
          DatabaseQuery2 prefetchQuery,
          SquashQLUser user) {
    Set<TypedField> fields = new HashSet<>(prefetchQuery.select);
    if (queryScope.table != null) {
      return new TableScope(queryScope.table,
              fields,
              queryScope.whereCriteria,
              queryScope.havingCriteria,
              queryScope.rollupColumns,
              queryScope.groupingSets,
              queryScope.virtualTable,
              user,
              prefetchQuery.limit);
    } else {
      return new SubQueryScope(queryScope.subQuery,
              fields,
              queryScope.whereCriteria,
              queryScope.havingCriteria,
              user,
              prefetchQuery.limit);
    }
  }

  public record QueryScope(CompiledTable table,
                           QueryScope subQuery,
                           List<TypedField> columns,
                           List<CompiledMeasure> measures,
                           CompiledCriteria whereCriteria,
                           CompiledCriteria havingCriteria,
                           List<TypedField> rollupColumns,
                           List<List<TypedField>> groupingSets,
                           VirtualTableDto virtualTable) {
  }

  public record QueryPlanNodeKey(QueryScope queryScope, CompiledMeasure measure) {
  }

  public record ExecutionContext(QueryScope queryScope,
                                 Map<QueryScope, Table> tableByScope,
                                 List<TypedField> columns,
                                 List<TypedField> bucketColumns,
                                 int queryLimit) {
    public Table getWriteToTable() {
      return tableByScope.get(queryScope);
    }
  }

  /**
   * This object is temporary until BigQuery supports the grouping sets. See <a href="https://issuetracker.google.com/issues/35905909">issue</a>
   */
  private static class PivotTableContext {
    private final List<Field> rows;
    private final List<Field> cleansedRows;
    private final List<Field> columns;
    private final List<Field> cleansedColumns;

    public PivotTableContext(PivotTableQueryDto pivotTableQueryDto) {
      this.rows = pivotTableQueryDto.rows;
      this.cleansedRows = cleanse(pivotTableQueryDto.query, pivotTableQueryDto.rows);
      this.columns = pivotTableQueryDto.columns;
      this.cleansedColumns = cleanse(pivotTableQueryDto.query, pivotTableQueryDto.columns);
    }

    public static List<Field> cleanse(QueryDto query, List<Field> fields) {
      // ColumnSet is a special type of column that does not exist in the database but only in SquashQL. Totals can't be
      // computed. This is why it is removed from the axes.
      ColumnSet columnSet = query.columnSets.get(BUCKET);
      if (columnSet != null) {
        Field name = ((BucketColumnSetDto) columnSet).newField;
        if (fields.contains(name)) {
          fields = new ArrayList<>(fields);
          fields.remove(name);
        }
      }
      return fields;
    }

  }

  public PivotTable executePivotQueryMerge(QueryDto first, QueryDto second, List<Field> rows, List<Field> columns, JoinType joinType, SquashQLUser user) {
    return QueryMergeExecutor.executePivotQueryMerge(this, first, second, rows, columns, joinType, user);
  }

  public Table executeQueryMerge(QueryDto first, QueryDto second, JoinType joinType, SquashQLUser user) {
    return QueryMergeExecutor.executeQueryMerge(this, first, second, joinType, user);
  }

}
