package me.paulbares.query;

import com.google.common.graph.Graph;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.NewQueryDto;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static me.paulbares.query.dto.NewQueryDto.BUCKET;
import static me.paulbares.query.dto.NewQueryDto.PERIOD;

public class NewQueryExecutor {

  public final QueryEngine queryEngine;

  public NewQueryExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table execute(NewQueryDto query) {
    List<String> finalColumns = new ArrayList<>();
    query.columnSets.values().forEach(cs -> finalColumns.addAll(cs.getNewColumns().stream().map(Field::name).toList()));
    query.columns.forEach(finalColumns::add);

    List<String> cols = new ArrayList<>();
    query.columns.forEach(cols::add);
    query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).forEach(cols::add);
    QueryDto prefetchQuery = new QueryDto().table(query.table);
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

  private ExecutionPlan<Measure, ExecutionContext> createExecutionPlan(NewQueryDto query) {
    GraphDependencyBuilder<Measure> builder = new GraphDependencyBuilder<>(m -> getMeasureDependencies(m));
    Graph<GraphDependencyBuilder.NodeWithId<Measure>> graph = builder.build(query.measures);
    ExecutionPlan<Measure, ExecutionContext> plan = new ExecutionPlan<>(graph, new MeasureEvaluator());
    return plan;
  }

  private static Set<Measure> getMeasureDependencies(Measure measure) {
    if (measure instanceof BinaryOperationMeasure bom) {
      return Set.of(bom.measure);
    } else {
      return Collections.emptySet();
    }
  }

  record ExecutionContext(Table table, NewQueryDto query) {
  }

  static class MeasureEvaluator implements BiConsumer<Measure, ExecutionContext> {
    @Override
    public void accept(Measure measure, ExecutionContext executionContext) {
      if (measure instanceof BinaryOperationMeasure bom) {
        AComparisonExecutor executor = createComparisonExecutor(executionContext.query.columnSets, bom);
        if (executor != null) {
          executeComparator(bom, executionContext.table, executor);
        }
      } else {
        throw new RuntimeException("nothing to do");
      }
    }

    private AComparisonExecutor createComparisonExecutor(Map<String, ColumnSet> columnSetMap, BinaryOperationMeasure bom) {
      Map<String, Function<ColumnSet, AComparisonExecutor>> m = Map.of(
              BUCKET, cs -> new BucketComparisonExecutor((BucketColumnSetDto) cs),
              PERIOD, cs -> new PeriodComparisonExecutor((PeriodColumnSetDto) cs));
      for (Map.Entry<String, Function<ColumnSet, AComparisonExecutor>> e : m.entrySet()) {
        ColumnSet cs = columnSetMap.get(e.getKey());
        if (cs != null && isComparisonFor(bom, cs)) {
          return m.get(e.getKey()).apply(cs);
        }
      }
      return null;
    }

    private static boolean isComparisonFor(Measure measure, ColumnSet cs) {
      if (measure instanceof BinaryOperationMeasure bom) {
        Set<String> intersection = new HashSet<>(bom.referencePosition.keySet());
        intersection.retainAll(cs.getNewColumns().stream().map(Field::name).collect(Collectors.toSet()));
        if (intersection.isEmpty()) {
          return false;
        }
      }
      return true;
    }

    private static void executeComparator(BinaryOperationMeasure bom, Table intermediateResult, AComparisonExecutor executor) {
      List<Object> agg = executor.compare(bom, intermediateResult);
      String newName = bom.alias == null
              ? String.format("%s(%s, %s)", bom.method, bom.measure.alias(), bom.referencePosition)
              : bom.alias;
      Field field = new Field(newName, BinaryOperations.getOutputType(bom.method, intermediateResult.getField(bom.measure).type()));
      intermediateResult.addAggregates(field, bom, agg);
    }
  }
}
