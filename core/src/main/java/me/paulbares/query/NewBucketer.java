package me.paulbares.query;

import me.paulbares.query.agg.SumAggregator;
import me.paulbares.query.dictionary.ObjectArrayDictionary;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.store.Field;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
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
    List<Field> finalHeaders = new ArrayList<>(intermediateResult.headers());
    MutableIntList columnIndices = new IntArrayList(intermediateResult.columnIndices());
    for (int j = 0; j < newColumns.size(); j++) {
      Field field = newColumns.get(j);
      if (!bucketColumnSetDto.getColumnsForPrefetching().contains(field.name())) {
        indexColsInPrefetch.add(j);
      }

      if (!intermediateResult.headers().contains(field)) {
        finalHeaders.add(field); // append to the end
        columnIndices.add(finalHeaders.size() - 1);
      }
    }

    List<List<Object>> newColumnValues = new ArrayList<>();
    for (int j = 0; j < finalHeaders.size(); j++) {
      newColumnValues.add(new ArrayList<>());
    }

    int originalHeadersSize = intermediateResult.headers().size();
    for (List<Object> row : intermediateResult) {
      List<Object> toBucketColumnValues = get(indexColsToBucket, row);
      List<Object[]> bucketValuesList = bucketer.apply(toBucketColumnValues);

      for (Object[] bucketValues : bucketValuesList) {
        // Pure copy for everything before
        for (int j = 0; j < originalHeadersSize; j++) {
          newColumnValues.get(j).add(row.get(j));
        }
        for (int j = 0; j < bucketValues.length; j++) {
          if (indexColsInPrefetch.contains(j)) {
            newColumnValues.get(j + originalHeadersSize).add(bucketValues[j]);
          }
        }
      }
    }

    // FIXME should we reorder the columns here?
    return new ColumnarTable(
            finalHeaders,
            intermediateResult.measures(),
            intermediateResult.measureIndices(),
            columnIndices.toArray(),
            newColumnValues);
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
