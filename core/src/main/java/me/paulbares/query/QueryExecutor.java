package me.paulbares.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.graph.Graph;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.Repository;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.database.DatabaseQuery;
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

  public QueryExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table execute(QueryDto query) {
    resolveMeasures(query);
    List<String> finalColumns = new ArrayList<>();
    query.columnSets.values().forEach(cs -> finalColumns.addAll(cs.getNewColumns().stream().map(Field::name).toList()));
    query.columns.forEach(finalColumns::add);

    List<String> cols = new ArrayList<>();
    query.columns.forEach(cols::add);
    query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).forEach(cols::add);
    DatabaseQuery prefetchQuery = new DatabaseQuery().table(query.table);
    prefetchQuery.conditions = query.conditions;
    cols.forEach(prefetchQuery::wildcardCoordinate);

    // Create plan
    ExecutionPlan<Measure, ExecutionContext> plan = createExecutionPlan(query);

    // Finish to prepare the query
    plan.getLeaves().forEach(prefetchQuery::withMeasure);

    Table prefetchResult = this.queryEngine.execute(prefetchQuery);

    if (query.columnSets.containsKey(BUCKET)) {
      // Apply this as it modifies the "shape" of the result
      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(BUCKET);
      prefetchResult = BucketerExecutor.bucket(prefetchResult, columnSet);
    }

    plan.execute(new ExecutionContext(prefetchResult, query));

    // Once complete, construct the final result with columns in correct order.
    List<Field> fields = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (String finalColumn : finalColumns) {
      fields.add(prefetchResult.getField(finalColumn));
      values.add(Objects.requireNonNull(prefetchResult.getColumnValues(finalColumn)));
    }

    List<Measure> measures = new ArrayList<>(query.measures);
    for (Measure measure : query.measures) {
      fields.add(prefetchResult.getField(measure));
      values.add(Objects.requireNonNull(prefetchResult.getAggregateValues(measure)));
    }

    return new ColumnarTable(fields,
            measures,
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
        AComparisonExecutor executor = createComparisonExecutor(executionContext.query.columnSets, cm);
        if (executor != null) {
          executeComparator(cm, executionContext.table, executor);
        }
      } else if (measure instanceof BinaryOperationMeasure bom) {
        executeBinaryOperation(bom, executionContext.table);
      } else {
        throw new RuntimeException("nothing to do");
      }
    }

    private AComparisonExecutor createComparisonExecutor(Map<String, ColumnSet> columnSetMap, ComparisonMeasure cm) {
      Map<String, Function<ColumnSet, AComparisonExecutor>> m = Map.of(
              BUCKET, cs -> new BucketComparisonExecutor((BucketColumnSetDto) cs),
              PERIOD, cs -> new PeriodComparisonExecutor((PeriodColumnSetDto) cs));
      for (Map.Entry<String, Function<ColumnSet, AComparisonExecutor>> e : m.entrySet()) {
        ColumnSet cs = columnSetMap.get(e.getKey());
        if (cs != null && isComparisonFor(cm, cs)) {
          return m.get(e.getKey()).apply(cs);
        }
      }
      return null;
    }

    private static boolean isComparisonFor(Measure measure, ColumnSet cs) {
      if (measure instanceof ComparisonMeasure cm) {
        Set<String> intersection = new HashSet<>(cm.referencePosition.keySet());
        intersection.retainAll(cs.getNewColumns().stream().map(Field::name).collect(Collectors.toSet()));
        if (intersection.isEmpty()) {
          return false;
        }
      }
      return true;
    }

    private static void executeComparator(ComparisonMeasure cm, Table intermediateResult, AComparisonExecutor executor) {
      List<Object> agg = executor.compare(cm, intermediateResult);
      String newName = cm.alias == null
              ? String.format("%s(%s, %s)", cm.method, cm.measure.alias(), cm.referencePosition)
              : cm.alias;
      Field field = new Field(newName, BinaryOperations.getOutputType(cm.method, intermediateResult.getField(cm.measure).type()));
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

      String newName = bom.alias == null
              ? String.format("%s %s %s", bom.leftOperand, bom.operator, bom.rightOperand)
              : bom.alias;
      Field field = new Field(newName, BinaryOperations.getOutputType(bom.operator, lType, rType));
      intermediateResult.addAggregates(field, bom, r);
    }
  }

  protected void resolveMeasures(QueryDto queryDto) {
    ContextValue repo = queryDto.context.get(Repository.KEY);
    Supplier<Map<String, ExpressionMeasure>> supplier = Suppliers.memoize(() -> ExpressionResolver.get(((Repository) repo).url));
    List<Measure> newMeasures = new ArrayList<>();
    for (Measure measure : queryDto.measures) {
      if (measure instanceof UnresolvedExpressionMeasure) {
        if (repo == null) {
          throw new IllegalStateException(Repository.class.getSimpleName() + " context is missing in the query");
        }
        String alias = ((UnresolvedExpressionMeasure) measure).alias;
        ExpressionMeasure expressionMeasure = supplier.get().get(alias);
        if (expressionMeasure == null) {
          throw new IllegalArgumentException("Cannot find expression with alias " + alias);
        }
        newMeasures.add(expressionMeasure);
      } else {
        newMeasures.add(measure);
      }
    }
    queryDto.measures = newMeasures;
  }
}