package me.paulbares.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.graph.Graph;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.QueryCacheContextValue;
import me.paulbares.query.context.Repository;
import me.paulbares.query.database.DatabaseQuery;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static me.paulbares.query.dto.QueryDto.BUCKET;
import static me.paulbares.query.dto.QueryDto.PERIOD;

public class QueryExecutor {

  public final QueryEngine queryEngine;
  public final CaffeineQueryCache caffeineCache;

  public QueryExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
    this.caffeineCache = new CaffeineQueryCache();
  }

  private QueryCache getQueryCache(QueryCacheContextValue queryCacheContextValue) {
    return switch (queryCacheContextValue.action) {
      case USE -> this.caffeineCache;
      case NOT_USE -> EmptyQueryCache.INSTANCE;
      case INVALIDATE -> {
        this.caffeineCache.clear();
        yield this.caffeineCache;
      }
    };
  }

  public Table execute(QueryDto query) {
    resolveMeasures(query);

    Set<String> cols = new HashSet<>();
    query.columns.forEach(cols::add);
    query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).forEach(cols::add);
    DatabaseQuery prefetchQuery = new DatabaseQuery().table(query.table);
    prefetchQuery.conditions = query.conditions;
    cols.forEach(prefetchQuery::wildcardCoordinate);

    // Create plan
    ExecutionPlan<Measure, ExecutionContext> plan = createExecutionPlan(query);

    Function<String, Field> fieldSupplier = this.queryEngine.getFieldSupplier();
    CaffeineQueryCache.QueryScope scope = new CaffeineQueryCache.QueryScope(query.table, cols.stream().map(fieldSupplier).collect(Collectors.toSet()), query.conditions);
    QueryCache queryCache = getQueryCache((QueryCacheContextValue) query.context.getOrDefault(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.USE)));

    // Finish to prepare the query
    Set<Measure> cached = new HashSet<>();
    Set<Measure> notCached = new HashSet<>();
    for (Measure leaf : plan.getLeaves()) {
      if (queryCache.contains(leaf, scope)) {
        cached.add(leaf);
      } else {
        notCached.add(leaf);
      }
    }

    Table prefetchResult;
    if (!notCached.isEmpty() || (cached.isEmpty() && notCached.isEmpty())) {
      if (!plan.getLeaves().contains(CountMeasure.INSTANCE)) {
        // Always add count
        notCached.add(CountMeasure.INSTANCE);
      }
      notCached.forEach(prefetchQuery::withMeasure);
      prefetchResult = this.queryEngine.execute(prefetchQuery);
    } else {
      // Create an empty result that will be populated by the query cache
      prefetchResult = queryCache.createRawResult(scope);
    }

    queryCache.contributeToResult(prefetchResult, cached, scope);
    queryCache.contributeToCache(prefetchResult, notCached, scope);

    if (query.columnSets.containsKey(BUCKET)) {
      // Apply this as it modifies the "shape" of the result
      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(BUCKET);
      prefetchResult = BucketerExecutor.bucket(prefetchResult, columnSet);
    }

    plan.execute(new ExecutionContext(prefetchResult, query));

    ColumnarTable columnarTable = buildFinalResult(query, prefetchResult);
    return TableUtils.order(columnarTable, query.comparators);
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

  private ExecutionPlan<Measure, ExecutionContext> createExecutionPlan(QueryDto query) {
    GraphDependencyBuilder<Measure> builder = new GraphDependencyBuilder<>(m -> getMeasureDependencies(m));
    Graph<GraphDependencyBuilder.NodeWithId<Measure>> graph = builder.build(query.measures);
    ExecutionPlan<Measure, ExecutionContext> plan = new ExecutionPlan<>(graph, new MeasureEvaluator());
    return plan;
  }

  private static Set<Measure> getMeasureDependencies(Measure measure) {
    if (measure instanceof ComparisonMeasure cm) {
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

  record ExecutionContext(Table table, QueryDto query) {
  }

  static class MeasureEvaluator implements BiConsumer<Measure, ExecutionContext> {
    @Override
    public void accept(Measure measure, ExecutionContext executionContext) {
      if (measure instanceof ComparisonMeasure cm) {
        Map<String, Function<ColumnSet, AComparisonExecutor>> m = Map.of(
                BUCKET, cs -> new BucketComparisonExecutor((BucketColumnSetDto) cs),
                PERIOD, cs -> new PeriodComparisonExecutor((PeriodColumnSetDto) cs));
        ColumnSet t = executionContext.query.columnSets.get(cm.columnSet);
        if (t == null) {
          throw new IllegalArgumentException(String.format("columnSet %s is not specified in the query but is used in a comparison measure: %s", cm.columnSet, cm));
        }
        AComparisonExecutor executor = m.get(cm.columnSet).apply(t);
        if (executor != null) {
          executeComparator(cm, executionContext.table, executor);
        }
      } else if (measure instanceof BinaryOperationMeasure bom) {
        executeBinaryOperation(bom, executionContext.table);
      } else {
        throw new RuntimeException("nothing to do");
      }
    }

    private static void executeComparator(ComparisonMeasure cm, Table intermediateResult, AComparisonExecutor executor) {
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
    if (measure instanceof ComparisonMeasure cm) {
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
