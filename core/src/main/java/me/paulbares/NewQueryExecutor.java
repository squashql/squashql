package me.paulbares;

import me.paulbares.query.*;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.NewQueryDto;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NewQueryExecutor {

  public final QueryEngine queryEngine;

  public NewQueryExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table execute(NewQueryDto query) {
    List<String> finalColumns = new ArrayList<>();
    query.columnSets.values().forEach(cs -> finalColumns.addAll(cs.getNewColumns().stream().map(Field::name).toList()));
    query.columns.forEach(finalColumns::add);

    Supplier<Map<Measure, LinkedList<Measure>>> graphByMeasureSupplier = () -> {
      Map<Measure, LinkedList<Measure>> graphByMeasure = new HashMap<>();
      query.measures.forEach(m -> {
        LinkedList<Measure> g = new LinkedList<>();
        buildDependencyGraph(g, m);
        graphByMeasure.put(m, g);
      });
      return graphByMeasure;
    };

    List<String> cols = new ArrayList<>();
    query.columns.forEach(cols::add);
    query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).forEach(cols::add);
    QueryDto prefetchQuery = new QueryDto().table(query.table);
    cols.forEach(prefetchQuery::wildcardCoordinate);
    Set<Measure> measuresToPrefetch = new HashSet<>();
    query.measures.forEach(m -> measuresToPrefetch.add(getMeasureToPrefetch(m)));
    measuresToPrefetch.forEach(prefetchQuery::withMeasure);
    Table[] intermediateResult = new Table[]{this.queryEngine.execute(prefetchQuery)};

    if (query.columnSets.containsKey(NewQueryDto.PERIOD)) {
      PeriodColumnSetDto columnSet = (PeriodColumnSetDto) query.columnSets.get(NewQueryDto.PERIOD);
      PeriodComparisonExecutor periodComparisonExecutor = new PeriodComparisonExecutor(columnSet);
      executeComparator(graphByMeasureSupplier.get(), intermediateResult[0], periodComparisonExecutor);
    }

    if (query.columnSets.containsKey(NewQueryDto.BUCKET)) {
      // Now bucket...
      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(NewQueryDto.BUCKET);
      intermediateResult[0] = BucketerExecutor.bucket(intermediateResult[0], columnSet);
      BucketComparisonExecutor bucketComparisonExecutor = new BucketComparisonExecutor(columnSet);
      executeComparator(graphByMeasureSupplier.get(), intermediateResult[0], bucketComparisonExecutor);
    }

    // Once complete, construct the final result with columns in correct order.
    List<Field> fields = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (String finalColumn : finalColumns) {
      fields.add(intermediateResult[0].getField(finalColumn));
      values.add(Objects.requireNonNull(intermediateResult[0].getColumnValues(finalColumn)));
    }

    List<Measure> measures = new ArrayList<>(query.measures);
    for (Measure measure : query.measures) {
      fields.add(intermediateResult[0].getField(measure));
      values.add(Objects.requireNonNull(intermediateResult[0].getAggregateValues(measure)));
    }

    ColumnarTable columnarTable = new ColumnarTable(fields,
            measures,
            IntStream.range(finalColumns.size(), fields.size()).toArray(),
            IntStream.range(0, finalColumns.size()).toArray(),
            values);
    return columnarTable;
  }

  private void visitGraph(ColumnSet columnSet, Map<Measure, LinkedList<Measure>> graph) {
    Iterator<Map.Entry<Measure, LinkedList<Measure>>> iterator = graph.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Measure, LinkedList<Measure>> next = iterator.next();
      Measure measure = next.getKey();
      if (!isComparisonFor(measure, columnSet)) {
        iterator.remove();
      } else {
        Iterator<Measure> measureIterator = next.getValue().iterator();
        while (measureIterator.hasNext()) {
          if (!isComparisonFor(measureIterator.next(), columnSet)) {
            measureIterator.remove();
          }
        }
      }
    }
  }

  private boolean isComparisonFor(Measure measure, ColumnSet cs) {
    if (measure instanceof BinaryOperationMeasure bom) {
      Set<String> intersection = new HashSet<>(bom.referencePosition.keySet());
      intersection.retainAll(cs.getNewColumns().stream().map(Field::name).collect(Collectors.toSet()));
      if (intersection.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private Measure getMeasureToPrefetch(Measure measure) {
    if (measure instanceof BinaryOperationMeasure bom) {
      return getMeasureToPrefetch(bom.measure);
    } else {
      return measure;
    }
  }

  private void buildDependencyGraph(LinkedList<Measure> graph, Measure measure) {
    graph.add(measure);
    if (measure instanceof BinaryOperationMeasure bom) {
      buildDependencyGraph(graph, bom.measure);
    }
  }

  private void executeComparator(Map<Measure, LinkedList<Measure>> measureGraph, Table intermediateResult, AComparisonExecutor executor) {
    visitGraph(executor.getColumnSet(), measureGraph);
    Map<Measure, List<Object>> aggregateValuesByMeasure = new HashMap<>();
    measureGraph.forEach((m, graph) -> {
      Measure last = graph.pollLast();
      aggregateValuesByMeasure.computeIfAbsent(last, intermediateResult::getAggregateValues);
      while ((last = graph.pollLast()) != null) {
        aggregateValuesByMeasure.computeIfAbsent(last, measure -> {
          if (measure instanceof BinaryOperationMeasure bom) {
            // FIXME check where the comparison happen.
            List<Object> agg = executor.compare(bom, intermediateResult);
            String newName = bom.alias == null
                    ? String.format("%s(%s, %s)", bom.method, bom.measure.alias(), bom.referencePosition)
                    : bom.alias;
            Field field = new Field(newName, BinaryOperations.getOutputType(bom.method, intermediateResult.getField(bom.measure).type()));
            intermediateResult.addAggregates(field, bom, agg);
            return agg;
          } else {
            throw new IllegalStateException("unexpected measure type " + measure.getClass());
          }
        });
      }
    });
  }
}
