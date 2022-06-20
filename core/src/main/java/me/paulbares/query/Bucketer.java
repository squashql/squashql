package me.paulbares.query;

import me.paulbares.query.agg.SumAggregator;
import me.paulbares.query.dictionary.ObjectArrayDictionary;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

public class Bucketer {

  public final QueryEngine queryEngine;

  public Bucketer(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Holder executeBucketing(Table result,
                                 int[] indexColsToLeave,
                                 int[] indexColAggregates,
                                 int[] indexColsToBucket,
                                 List<AggregatedMeasure> aggregatedMeasures, // align with indexColAggregates
                                 List<Field> newColumns, // align indexColsToBucket
                                 Function<List<Object>, List<Object[]>> bucketer) {
    ObjectArrayDictionary dictionary = new ObjectArrayDictionary(indexColsToLeave.length + newColumns.size());
    List<Field> aggregatedFields = get(indexColAggregates, result.headers());
    SumAggregator aggregator = new SumAggregator(aggregatedMeasures, aggregatedFields);
    for (List<Object> row : result) {
      List<Object> originalColumnValues = get(indexColsToLeave, row);
      List<Object> aggregateValues = get(indexColAggregates, row); // align with aggregatedMeasures
      List<Object> toBucketColumnValues = get(indexColsToBucket, row);
      List<Object[]> bucketValuesList = bucketer.apply(toBucketColumnValues);
      for (Object[] bucketValues : bucketValuesList) {
        Object[] key = new Object[originalColumnValues.size() + newColumns.size()];
        for (int i = 0; i < key.length; i++) {
          if (i < originalColumnValues.size()) {
            key[i] = originalColumnValues.get(i);
          } else {
            key[i] = bucketValues[i - indexColsToLeave.length];
          }
        }
        // FIXME use copyaggregator when needed. And should aggregation should be defined by measure
        aggregator.aggregate(dictionary.map(key), aggregateValues);
      }
    }

    // Once the aggregation is done, build the table
    List<List<Object>> rows = new ArrayList<>();
    dictionary.forEach((points, row) -> {
      List<Object> r = new ArrayList<>();
      r.addAll(Arrays.asList(points));
      r.addAll(aggregator.getAggregates(row));
      rows.add(r);
    });

    List<Field> originalColumns = get(indexColsToLeave, result.headers());
    int columnSize = originalColumns.size() + newColumns.size();
    Table arrayTable = new ArrayTable(ImmutableListFactoryImpl.INSTANCE
            .withAll(originalColumns)
            .newWithAll(newColumns)
            .newWithAll(aggregatedFields)
            .castToList(),
            aggregatedMeasures,
            IntStream.range(columnSize, columnSize + aggregatedFields.size()).toArray(),
            rows);

    return new Holder(arrayTable, originalColumns, newColumns, aggregatedFields, aggregatedMeasures, dictionary, aggregator);
  }

  public static <T> List<T> get(int[] indices, List<T> list) {
    List<T> subList = new ArrayList<>();
    for (int index : indices) {
      subList.add(list.get(index));
    }
    return subList;
  }

  public record Holder(Table table,
                       List<Field> originalColumns,
                       List<Field> newColumns,
                       List<Field> aggregatedFields,
                       List<AggregatedMeasure> aggregatedMeasures,
                       ObjectArrayDictionary dictionary,
                       SumAggregator aggregator) {
  }
}
