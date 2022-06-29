package me.paulbares.query;

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

public class BucketerExecutor {

  public static Table bucket(Table intermediateResult,
                             BucketColumnSetDto bucketColumnSetDto) {
    Function<Object[], List<Object[]>> bucketer = createBucketer(bucketColumnSetDto);

    int[] indexColumnsToRead = new int[bucketColumnSetDto.getColumnsForPrefetching().size()];
    for (int i = 0; i < bucketColumnSetDto.getColumnsForPrefetching().size(); i++) {
      indexColumnsToRead[i] = intermediateResult.columnIndex(bucketColumnSetDto.getColumnsForPrefetching().get(i));
    }

    MutableIntSet indexColsInPrefetch = new IntHashSet();
    List<Field> newColumns = bucketColumnSetDto.getNewColumns();
    List<Field> finalHeaders = new ArrayList<>(intermediateResult.headers());
    MutableIntList columnIndices = new IntArrayList(intermediateResult.columnIndices());
    for (int i = 0; i < newColumns.size(); i++) {
      Field field = newColumns.get(i);
      if (!bucketColumnSetDto.getColumnsForPrefetching().contains(field.name())) {
        indexColsInPrefetch.add(i);
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
    Object[] buffer = new Object[indexColumnsToRead.length];
    for (List<Object> row : intermediateResult) {
      transferValues(indexColumnsToRead, buffer, row);
      List<Object[]> bucketValuesList = bucketer.apply(buffer);
      for (Object[] bucketValues : bucketValuesList) {
        // Pure copy for everything before
        for (int i = 0; i < originalHeadersSize; i++) {
          newColumnValues.get(i).add(row.get(i));
        }
        for (int i = 0; i < bucketValues.length; i++) {
          if (indexColsInPrefetch.contains(i)) {
            newColumnValues.get(i + originalHeadersSize).add(bucketValues[i]);
          }
        }
      }
    }

    return new ColumnarTable(
            finalHeaders,
            intermediateResult.measures(),
            intermediateResult.measureIndices(),
            columnIndices.toArray(),
            newColumnValues);
  }

  private static Function<Object[], List<Object[]>> createBucketer(BucketColumnSetDto bucketColumnSetDto) {
    Map<String, List<String>> bucketsByValue = new HashMap<>();
    for (Pair<String, List<String>> value : bucketColumnSetDto.values) {
      for (String v : value.getTwo()) {
        bucketsByValue
                .computeIfAbsent(v, k -> new ArrayList<>())
                .add(value.getOne());
      }
    }
    Function<Object[], List<Object[]>> bucketer = toBucketColumnValues -> {
      List<String> buckets = bucketsByValue.get(toBucketColumnValues[0]);
      return buckets.stream().map(b -> new Object[]{b, toBucketColumnValues[0]}).toList();
    };
    return bucketer;
  }

  public static <T> void transferValues(int[] indices, Object[] buffer, List<T> list) {
    for (int i = 0; i < indices.length; i++) {
      buffer[i] = list.get(indices[i]);
    }
  }
}
