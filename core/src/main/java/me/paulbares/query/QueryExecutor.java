package me.paulbares.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.graph.Graph;
import lombok.extern.slf4j.Slf4j;
import me.paulbares.query.QueryCache.SubQueryScope;
import me.paulbares.query.QueryCache.TableScope;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.QueryCacheContextValue;
import me.paulbares.query.context.Repository;
import me.paulbares.query.database.DatabaseQuery;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.*;
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
    resolveMeasures(query);

    Set<QueryScope> scopes = new HashSet<>();
    QueryScope queryScope = new QueryScope(query.table, query.subQuery, query.columns.stream().map(this.queryEngine.getFieldSupplier()).toList(), query.conditions);
    scopes.add(queryScope);

    for (Measure measure : query.measures) {
      Set<QueryScope> requiredScopes = computeRequiredScopes(queryScope, measure);
      scopes.addAll(requiredScopes);
    }

    Map<QueryScope, DatabaseQuery> prefetchQueryByQueryScope = new HashMap<>();
    Map<QueryScope, ExecutionPlan<Measure, ExecutionContext>> plans = new HashMap<>();
    for (QueryScope scope : scopes) {
      prefetchQueryByQueryScope.put(scope, Queries.toDatabaseQuery(scope));
      // Create plan
      plans.put(scope, createExecutionPlan(query.measures, this.queryEngine.getFieldSupplier()));
    }

    Map<QueryScope, Table> tableByScope = new HashMap<>();
    // FIXME there might be a dependency between the plans...
    for (QueryScope scope : scopes) {
      DatabaseQuery prefetchQuery = prefetchQueryByQueryScope.get(scope);
      ExecutionPlan<Measure, ExecutionContext> plan = plans.get(scope);
      Function<String, Field> fieldSupplier = this.queryEngine.getFieldSupplier();
      QueryCache.PrefetchQueryScope prefetchQueryScope = createPrefetchQueryScope(queryScope, prefetchQuery, fieldSupplier);
      QueryCache queryCache = getQueryCache((QueryCacheContextValue) query.context.getOrDefault(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.USE)));

      // Finish to prepare the query
      Set<Measure> cached = new HashSet<>();
      Set<Measure> notCached = new HashSet<>();
      Set<Measure> leaves = plan.getLeaves();
      for (Measure leaf : leaves.stream().filter(m -> !(m instanceof ConstantMeasure)).toList()) {
        if (queryCache.contains(leaf, prefetchQueryScope)) {
          cached.add(leaf);
        } else {
          notCached.add(leaf);
        }
      }

      Table result;
      if (!notCached.isEmpty() || (cached.isEmpty() && notCached.isEmpty())) {
        if (!leaves.contains(CountMeasure.INSTANCE)) {
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

      if (query.columnSets.containsKey(BUCKET)) {
        // Apply this as it modifies the "shape" of the result
        BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(BUCKET);
        result = BucketerExecutor.bucket(result, columnSet);
      }

      tableByScope.put(scope, result);
      plan.execute(new ExecutionContext(result, scope, tableByScope, query, queryWatch));
    }

    ColumnarTable columnarTable = buildFinalResult(query, tableByScope.get(queryScope));
    Table sortedTable = TableUtils.order(columnarTable, query);

    CacheStatsDto stats = this.queryCache.stats();
    cacheStatsDtoBuilder
            .hitCount(stats.hitCount)
            .evictionCount(stats.evictionCount)
            .missCount(stats.missCount);
    return sortedTable;
  }

  private static QueryCache.PrefetchQueryScope createPrefetchQueryScope(QueryScope queryScope, DatabaseQuery prefetchQuery, Function<String, Field> fieldSupplier) {
    Set<Field> fields = prefetchQuery.coordinates.keySet().stream().map(fieldSupplier).collect(Collectors.toSet());
    if (queryScope.tableDto != null) {
      return new TableScope(queryScope.tableDto, fields, queryScope.conditions);
    } else {
      return new SubQueryScope(queryScope.subQuery, fields, queryScope.conditions);
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

  private static ExecutionPlan<Measure, ExecutionContext> createExecutionPlan(List<Measure> measures, Function<String, Field> fieldSupplier) {
    GraphDependencyBuilder<Measure> builder = new GraphDependencyBuilder<>(m -> getMeasureDependencies(m));
    Graph<GraphDependencyBuilder.NodeWithId<Measure>> graph = builder.build(measures);
    ExecutionPlan<Measure, ExecutionContext> plan = new ExecutionPlan<>(graph, new MeasureEvaluator(fieldSupplier));
    return plan;
  }

  private static Set<Measure> getMeasureDependencies(Measure measure) {
    if (measure instanceof ComparisonMeasure cm) {
      return Set.of(cm.getMeasure());
    } else if (measure instanceof BinaryOperationMeasure bom) {
      Set<Measure> s = new HashSet<>();
      s.add(bom.leftOperand);
      s.add(bom.rightOperand);
      return s;
    } else {
      return Collections.emptySet();
    }
  }

  private Set<QueryScope> computeRequiredScopes(QueryScope queryScope, Measure measure) {
    Set<QueryScope> requiredScopes = new HashSet<>();

    if (measure instanceof ParentComparisonMeasure pcm) {
      requiredScopes.addAll(MeasureUtils.getParentScopes(queryScope, pcm, this.queryEngine.getFieldSupplier()));
    }
    return requiredScopes;
  }

  public record QueryScope(TableDto tableDto, QueryDto subQuery, List<Field> columns,
                           Map<String, ConditionDto> conditions) {
  }

  record ExecutionContext(Table writeToTable,
                          QueryScope queryScope,
                          Map<QueryScope, Table> readFromTables,
                          QueryDto query,
                          QueryWatch queryWatch) {
  }

  static class MeasureEvaluator implements BiConsumer<Measure, ExecutionContext> {

    private final Function<String, Field> fieldSupplier;

    public MeasureEvaluator(Function<String, Field> fieldSupplier) {
      this.fieldSupplier = fieldSupplier;
    }

    @Override
    public void accept(Measure measure, ExecutionContext executionContext) {
      if (executionContext.writeToTable.measures().contains(measure)) {
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
          executeComparator(cm, executionContext.writeToTable, executor);
        }
      } else if (measure instanceof ParentComparisonMeasure pcm) {
        // FIXME
        Table whereToWrite = executionContext.writeToTable;
        List<String> ancestors = pcm.ancestors;

        List<Field> ancestorCandidates = new ArrayList<>(executionContext.queryScope.columns);
        ancestorCandidates.retainAll(ancestors.stream().map(this.fieldSupplier).toList());
        int[] ancestorIndices = new int[ancestorCandidates.size()];
        for (int i = 0; i < ancestorCandidates.size(); i++) {
          ancestorIndices[i] = whereToWrite.index(ancestorCandidates.get(i));
        }
        // FIXME take the first above executionContext.queryScope
        QueryScope parentScope = MeasureUtils.getParentScopes(executionContext.queryScope, pcm, this.fieldSupplier).get(0);  // Take the first one
        Table parentTable = executionContext.readFromTables.get(parentScope);
        List<Object> aggregateValues = whereToWrite.getAggregateValues(pcm.measure);
        List<Object> parentAggregateValues = parentTable.getAggregateValues(pcm.measure);
        List<Object> result = new ArrayList<>((int) whereToWrite.count());
        BiFunction<Number, Number, Number> divide = BinaryOperations.createComparisonBiFunction(ComparisonMethod.DIVIDE, double.class);

        int[] rowIndex = new int[1];
        whereToWrite.forEach(row -> {
          // Start - Shift operation
          int rowSize = row.size() - 1;
          Object[] parentRow = new Object[rowSize];
          int j = 0;
          for (int i = 0; i < rowSize; i++) {
            if (ancestorIndices[0] != i) {
              parentRow[j++] = row.get(i);
            }
          }
          // End - Shift operation
          int position = parentTable.pointDictionary().getPosition(parentRow);
          if (position != -1) {
            Object referenceValue = parentAggregateValues.get(position);
            Object currentValue = aggregateValues.get(rowIndex[0]);
            Object div = divide.apply((Number) currentValue, (Number) referenceValue);
            result.add(div);
          } else {
            result.add(null); // nothing to compare with
          }

//          int parentPosition = whereToWrite.pointDictionary().map(parentRow);

          rowIndex[0]++;
          // New rows must appear.
        });

        // Add total and subtotal here?

        throw new IllegalStateException("not finished");
      } else if (measure instanceof BinaryOperationMeasure bom) {
        executeBinaryOperation(bom, executionContext.writeToTable);
      } else if (measure instanceof ConstantMeasure cm) {
        executeConstantOperation(cm, executionContext.writeToTable);
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
