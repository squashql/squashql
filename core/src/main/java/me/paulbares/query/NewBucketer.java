package me.paulbares.query;

import me.paulbares.query.agg.SumAggregator;
import me.paulbares.query.dictionary.ObjectArrayDictionary;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.store.Field;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class NewBucketer {

  public final QueryEngine queryEngine;

  public NewBucketer(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table executeBucketing(Table intermediateResult,
                                BucketColumnSetDto bucketColumnSetDto) {
    Map<String, List<String>> bucketsByValue = new HashMap<>();
    for (Pair<String, List<String>> value : bucketColumnSetDto.values) {
      for (String v : value.getTwo()) {
        bucketsByValue
                .computeIfAbsent(v, k -> new ArrayList<>())
                .add(value.getOne());
      }
    }
    Function<List<Object>, List<Object[]>> bucketer = toBucketColumnValues -> {
      String value = (String) toBucketColumnValues.get(0);
      List<String> buckets = bucketsByValue.get(value);
      return buckets.stream().map(b -> new Object[]{b, value}).toList();
    };

    int[] indexColsToBucket = new int[bucketColumnSetDto.getColumnsForPrefetching().size()];
    int i = 0;
    for (String col : bucketColumnSetDto.getColumnsForPrefetching()) {
      indexColsToBucket[i++] = intermediateResult.columnIndex(col);
    }
    MutableIntSet indexColsInPrefetch = new IntHashSet();
    List<Field> newColumns = bucketColumnSetDto.getNewColumns();
    for (int j = 0; j < newColumns.size(); j++) {
      if (!bucketColumnSetDto.getColumnsForPrefetching().contains(newColumns.get(j).name())) {
        indexColsInPrefetch.add(j);
      }
    }

    List<List<Object>> newRows = new ArrayList<>();
    for (List<Object> row : intermediateResult) {
      List<Object> toBucketColumnValues = get(indexColsToBucket, row);
      List<Object[]> bucketValuesList = bucketer.apply(toBucketColumnValues);
      for (Object[] bucketValues : bucketValuesList) {
        List<Object> newRow = new ArrayList<>(row);
        for (int j = 0; j < bucketValues.length; j++) {
          if (indexColsInPrefetch.contains(j)) {
            newRow.add(bucketValues[j]);
          }
        }
        newRows.add(newRow);
      }
    }

    System.out.println();

    // Once the aggregation is done, build the table
//    List<List<Object>> rows = new ArrayList<>();
//    dictionary.forEach((points, row) -> {
//      List<Object> r = new ArrayList<>();
//      r.addAll(Arrays.asList(points));
//      r.addAll(aggregator.getAggregates(row));
//      rows.add(r);
//    });

//    List<Field> originalColumns = get(indexColsToLeave, intermediateResults.headers());
//    int columnSize = originalColumns.size() + newColumns.size();
//    Table arrayTable = new ArrayTable(ImmutableListFactoryImpl.INSTANCE
//            .withAll(originalColumns)
//            .newWithAll(newColumns)
//            .newWithAll(aggregatedFields)
//            .castToList(),
//            null,
//            IntStream.range(columnSize, columnSize + aggregatedFields.size()).toArray(),
//            IntStream.range(0, columnSize).toArray(),
//            rows);

    return null;
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
