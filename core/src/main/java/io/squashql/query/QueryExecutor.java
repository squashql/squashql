package io.squashql.query;

import io.squashql.PrefetchVisitor;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.QueryCache.SubQueryScope;
import io.squashql.query.QueryCache.TableScope;
import io.squashql.query.database.AQueryEngine;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.dto.*;
import io.squashql.query.parameter.QueryCacheParameter;
import io.squashql.store.Store;
import io.squashql.table.*;
import io.squashql.type.TypedField;
import io.squashql.util.Queries;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  public PivotTable execute(PivotTableQueryDto pivotTableQueryDto) {
    return execute(pivotTableQueryDto, CacheStatsDto.builder(), null, null);
  }

  public PivotTable execute(PivotTableQueryDto pivotTableQueryDto,
                            CacheStatsDto.CacheStatsDtoBuilder cacheStatsDtoBuilder,
                            SquashQLUser user,
                            IntConsumer limitNotifier) {
    if (!pivotTableQueryDto.query.rollupColumns.isEmpty()) {
      throw new IllegalArgumentException("Rollup is not supported by this API");
    }

    PivotTableContext pivotTableContext = new PivotTableContext(pivotTableQueryDto);
    QueryDto preparedQuery = prepareQuery(pivotTableQueryDto.query, pivotTableContext);
    Table result = execute(preparedQuery, pivotTableContext, cacheStatsDtoBuilder, user, false, limitNotifier);
    result = TableUtils.replaceTotalCellValues((ColumnarTable) result, pivotTableQueryDto.rows.stream().map(Field::name).collect(
            Collectors.toList()), pivotTableQueryDto.columns.stream().map(Field::name).collect(Collectors.toList()));
    result = TableUtils.orderRows((ColumnarTable) result, Queries.getComparators(preparedQuery), preparedQuery.columnSets.values());

    List<String> values = pivotTableQueryDto.query.measures.stream().map(Measure::alias).toList();
    return new PivotTable(result, pivotTableQueryDto.rows.stream().map(Field::name).toList(), pivotTableQueryDto.columns.stream().map(Field::name).toList(), values);
  }

  public static QueryDto prepareQuery(QueryDto query, PivotTableContext context) {
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

  public Table execute(String rawSqlQuery) {
    return this.queryEngine.executeRawSql(rawSqlQuery);
  }

  public Table execute(QueryDto query) {
    return execute(
            query,
            null,
            CacheStatsDto.builder(),
            null,
            true,
            null);
  }

  public Table execute(QueryDto query,
                       PivotTableContext pivotTableContext,
                       CacheStatsDto.CacheStatsDtoBuilder cacheStatsDtoBuilder,
                       SquashQLUser user,
                       boolean replaceTotalCellsAndOrderRows,
                       IntConsumer limitNotifier) {
    int queryLimit = query.limit < 0 ? LIMIT_DEFAULT_VALUE : query.limit;

    Function<Field, TypedField> fieldSupplier = createQueryFieldSupplier(this.queryEngine, query.virtualTableDto);
    if (pivotTableContext != null) {
      pivotTableContext.init(fieldSupplier);
    }
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

    Map<QueryScope, Table> tableByScope = new HashMap<>();
    for (QueryScope scope : prefetchQueryByQueryScope.keySet()) {
      DatabaseQuery prefetchQuery = prefetchQueryByQueryScope.get(scope);
      Set<Measure> measures = measuresByQueryScope.get(scope);
      QueryCache.PrefetchQueryScope prefetchQueryScope = createPrefetchQueryScope(scope, prefetchQuery, user);
      QueryCache queryCache = getQueryCache((QueryCacheParameter) query.parameters.getOrDefault(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.USE)), user);

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
        result = this.queryEngine.execute(prefetchQuery, pivotTableContext);
      } else {
        // Create an empty result that will be populated by the query cache
        result = queryCache.createRawResult(prefetchQueryScope);
      }

      queryCache.contributeToResult(result, cached, prefetchQueryScope);
      queryCache.contributeToCache(result, notCached, prefetchQueryScope);

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
    ExecutionPlan<QueryScope, Void> globalPlan = new ExecutionPlan<>(dependencyGraph.getTwo(), (scope, context) -> {
      ExecutionPlan<QueryPlanNodeKey, ExecutionContext> scopedPlan = new ExecutionPlan<>(dependencyGraph.getOne(), new Evaluator(fieldSupplier));
      scopedPlan.execute(new ExecutionContext(tableByScope.get(scope), scope, tableByScope, query, queryLimit));
    });
    globalPlan.execute(null);

    Table result = tableByScope.get(queryScope);

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

  private static Pair<DependencyGraph<QueryPlanNodeKey>, DependencyGraph<QueryScope>> computeDependencyGraph(
          QueryDto query,
          QueryScope queryScope,
          Function<Field, TypedField> fieldSupplier) {
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

  public static QueryScope createQueryScope(QueryDto query, Function<Field, TypedField> fieldSupplier) {
    // If column set, it changes the scope
    List<TypedField> columns = Stream.concat(
            query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()),
            query.columns.stream()).map(fieldSupplier).collect(Collectors.toCollection(ArrayList::new));
    List<TypedField> rollupColumns = query.rollupColumns.stream().map(fieldSupplier).toList();
    List<List<TypedField>> groupingSets = query.groupingSets.stream().map(g -> g.stream().map(fieldSupplier).toList()).toList();
    return new QueryScope(query.table,
            query.subQuery,
            columns,
            query.whereCriteriaDto,
            query.havingCriteriaDto,
            rollupColumns,
            groupingSets,
            query.virtualTableDto);
  }

  private static QueryCache.PrefetchQueryScope createPrefetchQueryScope(
          QueryScope queryScope,
          DatabaseQuery prefetchQuery,
          SquashQLUser user) {
    Set<TypedField> fields = new HashSet<>(prefetchQuery.select);
    if (queryScope.tableDto != null) {
      return new TableScope(queryScope.tableDto,
              fields,
              queryScope.whereCriteriaDto,
              queryScope.havingCriteriaDto,
              queryScope.rollupColumns,
              queryScope.groupingSets,
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
                           List<TypedField> columns,
                           CriteriaDto whereCriteriaDto,
                           CriteriaDto havingCriteriaDto,
                           List<TypedField> rollupColumns,
                           List<List<TypedField>> groupingSets,
                           VirtualTableDto virtualTableDto) {
  }

  public record QueryPlanNodeKey(QueryScope queryScope, Measure measure) {
  }

  public record ExecutionContext(Table writeToTable,
                                 QueryScope queryScope,
                                 Map<QueryScope, Table> tableByScope,
                                 QueryDto query,
                                 int queryLimit) {
  }


  /**
   * This object is temporary until BigQuery supports the grouping sets. See <a href="https://issuetracker.google.com/issues/35905909">issue</a>
   */
  public static class PivotTableContext {
    private final List<Field> rows;
    private final List<Field> cleansedRows;
    private final List<Field> columns;
    private final List<Field> cleansedColumns;
    @Getter
    private List<TypedField> rowFields;
    @Getter
    private List<TypedField> columnFields;

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
        Field name = ((BucketColumnSetDto) columnSet).name;
        if (fields.contains(name)) {
          fields = new ArrayList<>(fields);
          fields.remove(name);
        }
      }
      return fields;
    }

    public void init(Function<Field, TypedField> fieldSupplier) {
      this.rowFields = this.cleansedRows.stream().map(fieldSupplier).toList();
      this.columnFields = this.cleansedColumns.stream().map(fieldSupplier).toList();
    }
  }

  public Table execute(QueryDto first, QueryDto second, JoinType joinType, SquashQLUser user) {
    Map<String, Comparator<?>> firstComparators = Queries.getComparators(first);
    Map<String, Comparator<?>> secondComparators = Queries.getComparators(second);
    secondComparators.putAll(firstComparators); // the comparators of the first query take precedence over the second's

    Set<ColumnSet> columnSets = Stream.concat(first.columnSets.values().stream(), second.columnSets.values().stream())
            .collect(Collectors.toSet());

    Function<QueryDto, Table> execute = q -> execute(
            q,
            null,
            CacheStatsDto.builder(),
            user,
            false,
            limit -> {
              throw new RuntimeException("Result of " + q + " is too big (limit=" + limit + ")");
            });
    CompletableFuture<Table> f1 = CompletableFuture.supplyAsync(() -> execute.apply(first));
    CompletableFuture<Table> f2 = CompletableFuture.supplyAsync(() -> execute.apply(second));
    return CompletableFuture.allOf(f1, f2).thenApply(__ -> merge(f1.join(), f2.join(), joinType, secondComparators, columnSets)).join();
  }

  public static Table merge(Table table1, Table table2, JoinType joinType, Map<String, Comparator<?>> comparators, Set<ColumnSet> columnSets) {
    ColumnarTable table = (ColumnarTable) MergeTables.mergeTables(table1, table2, joinType);
    table = (ColumnarTable) TableUtils.orderRows(table, comparators, columnSets);
    return TableUtils.replaceTotalCellValues(table, true);
  }

  public static Function<Field, TypedField> createQueryFieldSupplier(QueryEngine<?> queryEngine, VirtualTableDto vt) {
    Map<String, Store> storesByName = new HashMap<>(queryEngine.datastore().storesByName());
    if (vt != null) {
      storesByName.put(vt.name, VirtualTableDto.toStore(vt));
    }
    return AQueryEngine.createFieldSupplier(storesByName);
  }
}
