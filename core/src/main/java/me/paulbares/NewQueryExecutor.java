package me.paulbares;

import me.paulbares.query.*;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.NewQueryDto;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;

import java.util.*;
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

    Map<Measure, LinkedList<Measure>> graphByMeasure = new HashMap<>();
    query.measures.forEach(m -> {
      LinkedList<Measure> g = new LinkedList<>();
      buildDependencyGraph(g, m);
      graphByMeasure.put(m, g);
    });

    List<String> cols = new ArrayList<>();
    query.columns.forEach(cols::add);
    query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).forEach(cols::add);
    QueryDto prefetchQuery = new QueryDto().table(query.table);
    cols.forEach(prefetchQuery::wildcardCoordinate);
    Set<Measure> measuresToPrefetch = new HashSet<>();
    query.measures.forEach(m -> measuresToPrefetch.add(getMeasureToPrefetch(m)));
    measuresToPrefetch.forEach(prefetchQuery::withMeasure);
    Table[] intermediateResult = new Table[]{this.queryEngine.execute(prefetchQuery)};

    Map<Measure, List<Object>> aggregateValuesByMeasure = new HashMap<>();
    if (query.columnSets.containsKey(NewQueryDto.PERIOD)) {
      PeriodColumnSetDto columnSet = (PeriodColumnSetDto) query.columnSets.get(NewQueryDto.PERIOD);

      graphByMeasure.forEach((m, graph) -> {
        Measure last = graph.pollLast();
        aggregateValuesByMeasure.computeIfAbsent(last, intermediateResult[0]::getAggregateValues);
        while ((last = graph.pollLast()) != null) {
          aggregateValuesByMeasure.computeIfAbsent(last, measure -> {
            if (measure instanceof BinaryOperationMeasure bom) {
              List<Object> agg = PeriodComparisonExecutor.compare(bom, columnSet, intermediateResult[0]);
              String newName = bom.alias == null
                      ? String.format("%s(%s, %s)", bom.method, bom.measure.alias(), bom.referencePosition)
                      : bom.alias;
              Field field = new Field(newName, BinaryOperations.getOutputType(bom.method, intermediateResult[0].getField(bom.measure).type()));
              intermediateResult[0].addAggregates(field, bom, agg);
              return agg;
            } else {
              throw new IllegalStateException("unexpected measure type " + measure.getClass());
            }
          });
        }
      });
    }

    if (query.columnSets.containsKey(NewQueryDto.BUCKET)) {
      // Now bucket...
      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(NewQueryDto.BUCKET);
      intermediateResult[0] = BucketerExecutor.bucket(intermediateResult[0], columnSet);

      // TODO execute bucket comparison.
      graphByMeasure.forEach((m, graph) -> {
        Measure last = graph.pollLast();
        aggregateValuesByMeasure.computeIfAbsent(last, intermediateResult[0]::getAggregateValues);
        while ((last = graph.pollLast()) != null) {
          aggregateValuesByMeasure.computeIfAbsent(last, measure -> {
            if (measure instanceof BinaryOperationMeasure bom) {
              List<Object> agg = BucketComparisonExecutor.compare(bom, columnSet, intermediateResult[0]);
              String newName = bom.alias == null
                      ? String.format("%s(%s, %s)", bom.method, bom.measure.alias(), bom.referencePosition)
                      : bom.alias;
              Field field = new Field(newName, BinaryOperations.getOutputType(bom.method, intermediateResult[0].getField(bom.measure).type()));
              intermediateResult[0].addAggregates(field, bom, agg);
              return agg;
            } else {
              throw new IllegalStateException("unexpected measure type " + measure.getClass());
            }
          });
        }
      });
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
}
