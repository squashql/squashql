package io.squashql.query;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.compiled.*;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.query.dto.QueryJoinDto;
import io.squashql.query.join.ExperimentalQueryMergeExecutor;
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
    query.limit = queryLimit;

    QueryResolver queryResolver = new QueryResolver(query, this.queryEngine.datastore().storesByName());
    DependencyGraph<QueryPlanNodeKey> dependencyGraph = computeDependencyGraph(
            queryResolver.getColumns(), queryResolver.getBucketColumns(), queryResolver.getMeasures().values(), queryResolver.getScope());
    // Compute what needs to be prefetched
    Map<QueryScope, DatabaseQuery> prefetchQueryByQueryScope = new HashMap<>();
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
      DatabaseQuery prefetchQuery = prefetchQueryByQueryScope.get(scope);
      Set<CompiledMeasure> measures = measuresByQueryScope.get(scope);
      QueryCache.QueryCacheKey queryCacheKey = new QueryCache.QueryCacheKey(scope, user);
      QueryCache queryCache = getQueryCache((QueryCacheParameter) query.parameters.getOrDefault(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.USE)), user);

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
        notCached.forEach(prefetchQuery::withMeasure);
        result = this.queryEngine.execute(prefetchQuery);
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
        final ExecutionContext executionContext = new ExecutionContext(queryNode.queryScope,
                tableByScope,
                queryResolver.getColumns(),
                queryResolver.getBucketColumns(),
                queryResolver.getCompiledColumnSets(),
                queryLimit);
        evaluator.accept(queryNode, executionContext);
      }
    });
    globalPlan.execute();

    Table result = tableByScope.get(queryResolver.getScope());

    if (limitNotifier != null && result.count() == queryLimit) {
      limitNotifier.accept(queryLimit);
    }

    result = TableUtils.selectAndOrderColumns(queryResolver, (ColumnarTable) result, query);
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

  private static boolean canBeCached(CompiledMeasure measure, QueryScope scope) {
    // Make sure to never cache the grouping measures. It could cause issue in some cases.
    if (generateGroupingMeasures(scope).values().contains(measure)) {
      return false;
    }
    // In case of vectors, we can rely only on the alias of the measure.
    return SqlUtils.extractFieldFromGroupingAlias(measure.alias()) == null;
  }

  private static DependencyGraph<QueryPlanNodeKey> computeDependencyGraph(
          List<TypedField> columns,
          List<TypedField> bucketColumns,
          Collection<CompiledMeasure> measures,
          QueryScope queryScope) {
    GraphDependencyBuilder<QueryPlanNodeKey> builder = new GraphDependencyBuilder<>(nodeKey -> {
      Map<QueryScope, Set<CompiledMeasure>> dependencies = nodeKey.measure.accept(new PrefetchVisitor(columns, bucketColumns, nodeKey.queryScope));
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

  public record QueryScope(CompiledTable table,
                           List<TypedField> columns,
                           CompiledCriteria whereCriteria,
                           CompiledCriteria havingCriteria,
                           List<TypedField> rollupColumns,
                           List<List<TypedField>> groupingSets,
                           List<CteRecordTable> cteRecordTables,
                           int limit) {

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("QueryScope{");
      sb.append("table=").append(this.table);
      if (this.columns != null && !this.columns.isEmpty()) {
        sb.append(", columns=").append(this.columns);
      }
      if (this.whereCriteria != null) {
        sb.append(", whereCriteria=").append(this.whereCriteria);
      }
      if (this.havingCriteria != null) {
        sb.append(", havingCriteria=").append(this.havingCriteria);
      }
      if (this.rollupColumns != null && !this.rollupColumns.isEmpty()) {
        sb.append(", rollupColumns=").append(this.rollupColumns);
      }
      if (this.groupingSets != null && !this.groupingSets.isEmpty()) {
        sb.append(", groupingSets=").append(this.groupingSets);
      }
      if (this.cteRecordTables != null && !this.cteRecordTables.isEmpty()) {
        sb.append(", cteRecordTables=").append(this.cteRecordTables);
      }
      if (this.limit > 0) {
        sb.append(", limit=").append(this.limit);
      }
      sb.append('}');
      return sb.toString();
    }
  }

  public record QueryPlanNodeKey(QueryScope queryScope, CompiledMeasure measure) {
  }

  public record ExecutionContext(QueryScope queryScope,
                                 Map<QueryScope, Table> tableByScope,
                                 List<TypedField> columns,
                                 List<TypedField> bucketColumns,
                                 Map<ColumnSetKey, CompiledColumnSet> columnSets,
                                 int queryLimit) {
    public Table getWriteToTable() {
      return this.tableByScope.get(this.queryScope);
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
        Field newField = ((BucketColumnSetDto) columnSet).newField;
        if (fields.contains(newField)) {
          fields = new ArrayList<>(fields);
          fields.remove(newField);
        }
      }
      return fields;
    }

  }

  public PivotTable executePivotQueryMerge(QueryMergeDto queryMerge, List<Field> rows, List<Field> columns, SquashQLUser user) {
    return QueryMergeExecutor.executePivotQueryMerge(this, queryMerge, rows, columns, user);
  }

  public Table executeQueryMerge(QueryMergeDto queryMerge, SquashQLUser user) {
    return QueryMergeExecutor.executeQueryMerge(this, queryMerge, user);
  }

  public Table executeExperimentalQueryMerge(QueryJoinDto queryJoin) {
    return new ExperimentalQueryMergeExecutor(this.queryEngine).execute(queryJoin);
  }

  /**
   * Generates grouping measures based on the provided query scope.
   *
   * @param queryScope The query scope containing rollup columns and grouping sets.
   * @return A map of compiled measures, where the key is the squashql expression of the fields used in the rollup and
   * the value is the compiled measure.
   */
  public static Map<String, CompiledMeasure> generateGroupingMeasures(QueryScope queryScope) {
    Map<String, CompiledMeasure> measures = new HashMap();
    List<TypedField> rollups = new ArrayList<>();
    rollups.addAll(queryScope.rollupColumns);
    rollups.addAll(queryScope.groupingSets
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
}
