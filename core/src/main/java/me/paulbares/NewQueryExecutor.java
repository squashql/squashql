package me.paulbares;

import me.paulbares.query.*;
import me.paulbares.query.comp.BinaryOperations;
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

    Map<Measure, List<Object>> aggregateValuesByMeasure = new HashMap<>();

    Table t = null;
    if (query.columnSets.containsKey(NewQueryDto.PERIOD)) {
      PeriodColumnSetDto columnSet = (PeriodColumnSetDto) query.columnSets.get(NewQueryDto.PERIOD);

      {
        List<String> cols = new ArrayList<>();
        query.columns.forEach(cols::add);
        columnSet.getColumnsForPrefetching().forEach(cols::add);
        QueryDto prefetchQuery = new QueryDto().table(query.table);
        cols.forEach(prefetchQuery::wildcardCoordinate);
        Set<Measure> measures = new HashSet<>();
        query.measures.forEach(m -> measures.add(getMeasureToPrefetch(m)));
        measures.forEach(prefetchQuery::withMeasure);
        t = this.queryEngine.execute(prefetchQuery);
      }

      Table finalT = t;
      graphByMeasure.forEach((m, graph) -> {
        Measure last = graph.pollLast();
        aggregateValuesByMeasure.computeIfAbsent(last, finalT::getAggregateValues);
        while ((last = graph.pollLast()) != null) {
          aggregateValuesByMeasure.computeIfAbsent(last, measure -> {
            if (measure instanceof BinaryOperationMeasure bom) {
              List<Object> agg = PeriodComparisonExecutor.executeComparison(bom, columnSet, finalT);
              String newName = bom.alias == null
                      ? String.format("%s(%s, %s)", bom.method, bom.measure.alias(), bom.referencePosition)
                      : bom.alias;
              Field field = new Field(newName, BinaryOperations.getOutputType(bom.method, finalT.getField(bom.measure).type()));
              finalT.addAggregates(field, bom, agg);
              return agg;
            } else {
              throw new IllegalStateException("unexpected measure type " + measure.getClass());
            }
          });
        }
      });

      // Once complete, construct the final result

      List<Field> fields = new ArrayList<>();
      List<List<Object>> values = new ArrayList<>();
      int n = 0;
      for (String finalColumn : finalColumns) {
        fields.add(finalT.getField(finalColumn));
        values.add(Objects.requireNonNull(finalT.getColumnValues(finalColumn)));
        n++;
      }

      List<Measure> measures = new ArrayList<>(query.measures);
      for (Measure measure : query.measures) {
        fields.add(finalT.getField(measure));
        values.add(Objects.requireNonNull(finalT.getAggregateValues(measure)));
      }

      ColumnarTable columnarTable = new ColumnarTable(fields,
              measures,
              IntStream.range(n, fields.size()).toArray(),
              IntStream.range(0, n).toArray(),
              values);
      return columnarTable;
    }

    if (query.columnSets.containsKey(NewQueryDto.BUCKET)) {
      // Now bucket...
//      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(NewQueryDto.BUCKET);
//      Map<String, List<String>> bucketsByValue = new HashMap<>();
//      for (Pair<String, List<String>> value : columnSet.values) {
//        for (String v : value.getTwo()) {
//          bucketsByValue
//                  .computeIfAbsent(v, k -> new ArrayList<>())
//                  .add(value.getOne());
//        }
//      }

//      List<String> r = t.headers().stream().map(Field::name).toList();
//      int[] indexColsToBucket = new int[1];
//      int[] indexColAggregates = t.measureIndices();
//      MutableIntList indexColsToLeaveList = new IntArrayList();
//      for (int i = 0; i < r.size(); i++) {
//        if (columnSet.getColumnsForPrefetching().contains(r.get(i))) {
//          indexColsToBucket[0] = i;
//        } else if (Arrays.binarySearch(indexColAggregates, i) < 0) {
//          indexColsToLeaveList.add(i);
//        }
//      }
//      int[] indexColsToLeave = indexColsToLeaveList.toArray();
//
//      Bucketer.Holder holder = new Bucketer(this.queryEngine)
//              .executeBucketing(t,
//                      indexColsToLeave,
//                      indexColAggregates,
//                      indexColsToBucket,
//                      t.measures().stream().map(AggregatedMeasure.class::cast).toList(),
//                      columnSet.getNewColumns(),
//                      toBucketColumnValues -> {
//                        String value = (String) toBucketColumnValues.get(0);
//                        List<String> buckets = bucketsByValue.get(value);
//                        return buckets.stream().map(b -> new Object[]{b, value}).toList();
//                      });
//      t = holder.table();
      System.out.println();
    }

    return t;
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
