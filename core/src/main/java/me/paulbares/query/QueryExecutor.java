package me.paulbares.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.graph.Graph;
import lombok.extern.slf4j.Slf4j;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.QueryCacheContextValue;
import me.paulbares.query.context.Repository;
import me.paulbares.query.database.DatabaseQuery;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.monitoring.QueryWatch;
import me.paulbares.store.Field;
import me.paulbares.util.Queries;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static me.paulbares.query.ColumnSetKey.BUCKET;
import static me.paulbares.query.ColumnSetKey.PERIOD;

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

    DatabaseQuery prefetchQuery = Queries.toDatabaseQuery(query);

    // Create plan
    queryWatch.start(QueryWatch.PREPARE_CREATE_EXEC_PLAN);
    ExecutionPlan<Measure, ExecutionContext> plan = createExecutionPlan(query);
    queryWatch.stop(QueryWatch.PREPARE_CREATE_EXEC_PLAN);

    Function<String, Field> fieldSupplier = this.queryEngine.getFieldSupplier();
    queryWatch.start(QueryWatch.PREPARE_CREATE_QUERY_SCOPE);
    QueryCache.QueryScope queryScope = createCacheKey(query, prefetchQuery, fieldSupplier);
    queryWatch.stop(QueryWatch.PREPARE_CREATE_QUERY_SCOPE);
    QueryCache queryCache = getQueryCache((QueryCacheContextValue) query.context.getOrDefault(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.USE)));

    // Finish to prepare the query
    Set<Measure> cached = new HashSet<>();
    Set<Measure> notCached = new HashSet<>();
    Set<Measure> leaves = plan.getLeaves();
    for (Measure leaf : leaves.stream().filter(m -> !(m instanceof ConstantMeasure)).toList()) {
      if (queryCache.contains(leaf, queryScope)) {
        cached.add(leaf);
      } else {
        notCached.add(leaf);
      }
    }

    queryWatch.stop(QueryWatch.PREPARE_PLAN);
    queryWatch.start(QueryWatch.PREFETCH);

    Table prefetchResult;
    if (!notCached.isEmpty() || (cached.isEmpty() && notCached.isEmpty())) {
      if (!leaves.contains(CountMeasure.INSTANCE)) {
        // Always add count
        notCached.add(CountMeasure.INSTANCE);
      }
      notCached.forEach(prefetchQuery::withMeasure);
      prefetchResult = this.queryEngine.execute(prefetchQuery);
    } else {
      // Create an empty result that will be populated by the query cache
      prefetchResult = queryCache.createRawResult(queryScope);
    }
    queryWatch.stop(QueryWatch.PREFETCH);

    queryCache.contributeToResult(prefetchResult, cached, queryScope);
    queryCache.contributeToCache(prefetchResult, notCached, queryScope);

    queryWatch.start(QueryWatch.BUCKET);
    if (query.columnSets.containsKey(BUCKET)) {
      // Apply this as it modifies the "shape" of the result
      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(BUCKET);
      prefetchResult = BucketerExecutor.bucket(prefetchResult, columnSet);
    }
    queryWatch.stop(QueryWatch.BUCKET);

    queryWatch.start(QueryWatch.EXECUTE_PLAN);
    plan.execute(new ExecutionContext(prefetchResult, query, queryWatch));
    queryWatch.stop(QueryWatch.EXECUTE_PLAN);

    queryWatch.start(QueryWatch.ORDER);

    ColumnarTable columnarTable = buildFinalResult(query, prefetchResult);
    Table sortedTable = TableUtils.order(columnarTable, query);

    queryWatch.stop(QueryWatch.ORDER);
    queryWatch.stop(QueryWatch.GLOBAL);

    CacheStatsDto stats = queryCache.stats();
    cacheStatsDtoBuilder
            .hitCount(stats.hitCount)
            .evictionCount(stats.evictionCount)
            .missCount(stats.missCount);
    return sortedTable;
  }

  private static QueryCache.QueryScope createCacheKey(QueryDto query, DatabaseQuery prefetchQuery, Function<String, Field> fieldSupplier) {
    Set<Field> fields = prefetchQuery.coordinates.keySet().stream().map(fieldSupplier).collect(Collectors.toSet());
    if (query.table != null) {
      return new QueryCache.TableScope(query.table, fields, query.conditions);
    } else {
      return new QueryCache.SubQueryScope(query.subQuery, fields, query.conditions);
    }
  }

  private ColumnarTable buildFinalResult(QueryDto query, Table prefetchResult) {
    List<String> finalColumns = new ArrayList<>();
    query.columnSets.values().forEach(cs -> finalColumns.addAll(cs.getNewColumns().stream().map(Field::name).toList()));
    query.columns.forEach(finalColumns::add);

    // Once complete, construct the final result with columns in correct order.
    List<Field> fields = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (String finalColumn : finalColumns) {
      fields.add(prefetchResult.getField(finalColumn));
      values.add(Objects.requireNonNull(prefetchResult.getColumnValues(finalColumn)));
    }

    for (Measure measure : query.measures) {
      fields.add(prefetchResult.getField(measure));
      values.add(Objects.requireNonNull(prefetchResult.getAggregateValues(measure)));
    }

    return new ColumnarTable(fields,
            query.measures,
            IntStream.range(finalColumns.size(), fields.size()).toArray(),
            IntStream.range(0, finalColumns.size()).toArray(),
            values);
  }

  private static ExecutionPlan<Measure, ExecutionContext> createExecutionPlan(QueryDto query) {
    GraphDependencyBuilder<Measure> builder = new GraphDependencyBuilder<>(m -> getMeasureDependencies(m));
    Graph<GraphDependencyBuilder.NodeWithId<Measure>> graph = builder.build(query.measures);
    ExecutionPlan<Measure, ExecutionContext> plan = new ExecutionPlan<>(graph, new MeasureEvaluator());
    return plan;
  }

  private static Set<Measure> getMeasureDependencies(Measure measure) {
    if (measure instanceof ComparisonMeasureReferencePosition cm) {
      return Set.of(cm.measure);
    } else if (measure instanceof BinaryOperationMeasure bom) {
      Set<Measure> s = new HashSet<>();
      s.add(bom.leftOperand);
      s.add(bom.rightOperand);
      return s;
    } else {
      return Collections.emptySet();
    }
  }

  record ExecutionContext(Table table, QueryDto query, QueryWatch queryWatch) {
  }

  static class MeasureEvaluator implements BiConsumer<Measure, ExecutionContext> {
    @Override
    public void accept(Measure measure, ExecutionContext executionContext) {
      if (executionContext.table.measures().contains(measure)) {
        return; // nothing to do
      }

      executionContext.queryWatch.start(measure);
      if (measure instanceof ComparisonMeasureReferencePosition cm) {
        Map<ColumnSetKey, Function<ColumnSet, AComparisonExecutor>> m = Map.of(
                BUCKET, cs -> new BucketComparisonExecutor((BucketColumnSetDto) cs),
                PERIOD, cs -> new PeriodComparisonExecutor((PeriodColumnSetDto) cs));
        ColumnSet t = executionContext.query.columnSets.get(cm.columnSetKey);
        if (t == null) {
          throw new IllegalArgumentException(String.format("columnSet %s is not specified in the query but is used in a comparison measure: %s", cm.columnSetKey, cm));
        }
        AComparisonExecutor executor = m.get(cm.columnSetKey).apply(t);
        if (executor != null) {
          executeComparator(cm, executionContext.table, executor);
        }
      } else if (measure instanceof BinaryOperationMeasure bom) {
        executeBinaryOperation(bom, executionContext.table);
      } else if (measure instanceof ConstantMeasure cm) {
        executeConstantOperation(cm, executionContext.table);
      } else {
        throw new IllegalStateException(measure + " not expected");
      }
      executionContext.queryWatch.stop(measure);
    }

    private static void executeComparator(ComparisonMeasureReferencePosition cm, Table intermediateResult, AComparisonExecutor executor) {
      List<Object> agg = executor.compare(cm, intermediateResult);
      Field field = new Field(cm.alias(), BinaryOperations.getComparisonOutputType(cm.method, intermediateResult.getField(cm.measure).type()));
      intermediateResult.addAggregates(field, cm, agg);
    }

    private static void executeBinaryOperation(BinaryOperationMeasure bom, Table intermediateResult) {
      List<Object> lo = intermediateResult.getAggregateValues(bom.leftOperand);
      List<Object> ro = intermediateResult.getAggregateValues(bom.rightOperand);
      List<Object> r = new ArrayList<>(lo.size());

      Class<?> lType = intermediateResult.getField(bom.leftOperand).type();
      Class<?> rType = intermediateResult.getField(bom.rightOperand).type();
      BiFunction<Number, Number, Number> operation = BinaryOperations.createBiFunction(bom.operator, lType, rType);
      for (int i = 0; i < lo.size(); i++) {
        r.add(operation.apply((Number) lo.get(i), (Number) ro.get(i)));
      }
      Field field = new Field(bom.alias(), BinaryOperations.getOutputType(bom.operator, lType, rType));
      intermediateResult.addAggregates(field, bom, r);
    }

    private static void executeConstantOperation(ConstantMeasure<?> cm, Table intermediateResult) {
      Object v;
      Class<?> type;
      if (cm instanceof DoubleConstantMeasure dcm) {
        v = ((Number) dcm.value).doubleValue();
        type = double.class;
      } else if (cm instanceof LongConstantMeasure lcm) {
        v = ((Number) lcm.value).longValue();
        type = long.class;
      } else {
        throw new IllegalArgumentException("Unexpected type " + cm.getValue().getClass() + ". Only double and long are supported");
      }
      Field field = new Field(cm.alias(), type);
      List<Object> r = Collections.nCopies((int) intermediateResult.count(), v);
      intermediateResult.addAggregates(field, cm, r);
    }
  }

  protected static void resolveMeasures(QueryDto queryDto) {
    ContextValue repo = queryDto.context.get(Repository.KEY);
    Supplier<Map<String, ExpressionMeasure>> supplier = Suppliers.memoize(() -> ExpressionResolver.get(((Repository) repo).url));
    List<Measure> newMeasures = new ArrayList<>();
    for (Measure measure : queryDto.measures) {
      newMeasures.add(resolveExpressionMeasure(repo, supplier, measure));
    }
    queryDto.measures = newMeasures;
  }

  private static Measure resolveExpressionMeasure(ContextValue repo, Supplier<Map<String, ExpressionMeasure>> supplier, Measure measure) {
    if (measure instanceof UnresolvedExpressionMeasure) {
      if (repo == null) {
        throw new IllegalStateException(Repository.class.getSimpleName() + " context is missing in the query");
      }
      String alias = ((UnresolvedExpressionMeasure) measure).alias;
      ExpressionMeasure expressionMeasure = supplier.get().get(alias);
      if (expressionMeasure == null) {
        throw new IllegalArgumentException("Cannot find expression with alias " + alias);
      }
      return expressionMeasure;
    } else {
      resolveMeasureDependencies(repo, supplier, measure);
      return measure;
    }
  }

  private static void resolveMeasureDependencies(ContextValue repo, Supplier<Map<String, ExpressionMeasure>> supplier, Measure measure) {
    if (measure instanceof ComparisonMeasureReferencePosition cm) {
      cm.measure = resolveExpressionMeasure(repo, supplier, cm.measure);
      resolveMeasureDependencies(repo, supplier, cm.measure);
    } else if (measure instanceof BinaryOperationMeasure bom) {
      bom.leftOperand = resolveExpressionMeasure(repo, supplier, bom.leftOperand);
      bom.rightOperand = resolveExpressionMeasure(repo, supplier, bom.rightOperand);
      resolveMeasureDependencies(repo, supplier, bom.leftOperand);
      resolveMeasureDependencies(repo, supplier, bom.rightOperand);
    }
  }
}
