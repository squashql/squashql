package io.squashql.query;

import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.store.Field;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.*;
import java.util.function.Function;

public class BucketerExecutor {

  public static Table bucket(Table table, BucketColumnSetDto bucketColumnSetDto) {
    Function<Object[], List<Object[]>> bucketer = createBucketer(bucketColumnSetDto);

    int[] indexColumnsToRead = new int[bucketColumnSetDto.getColumnsForPrefetching().size()];
    for (int i = 0; i < bucketColumnSetDto.getColumnsForPrefetching().size(); i++) {
      indexColumnsToRead[i] = table.columnIndex(bucketColumnSetDto.getColumnsForPrefetching().get(i));
    }

    MutableIntSet indexColsInPrefetch = new IntHashSet();
    List<Field> newColumns = bucketColumnSetDto.getNewColumns();
    List<Header> finalHeaders = new ArrayList<>(table.headers());
    for (int i = 0; i < newColumns.size(); i++) {
      Field field = newColumns.get(i);
      if (!bucketColumnSetDto.getColumnsForPrefetching().contains(field.name())) {
        indexColsInPrefetch.add(i);
      }
      Header header = new Header(field.name(), field.type(), false);
      if (!table.headers().contains(header)) {
        finalHeaders.add(header); // append to the end
      }
    }

    List<List<Object>> newColumnValues = new ArrayList<>();
    for (int j = 0; j < finalHeaders.size(); j++) {
      newColumnValues.add(new ArrayList<>());
    }

    int originalHeadersSize = table.headers().size();
    Object[] buffer = new Object[indexColumnsToRead.length];
    for (List<Object> row : table) {
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
            table.measures(),
            newColumnValues);
  }

  private static Function<Object[], List<Object[]>> createBucketer(BucketColumnSetDto bucketColumnSetDto) {
    Map<String, List<String>> bucketsByValue = new HashMap<>();
    for (Map.Entry<String, List<String>> value : bucketColumnSetDto.values.entrySet()) {
      for (String v : value.getValue()) {
        bucketsByValue
                .computeIfAbsent(v, k -> new ArrayList<>())
                .add(value.getKey());
      }
    }
    Function<Object[], List<Object[]>> bucketer = toBucketColumnValues -> {
      List<String> buckets = bucketsByValue.get(toBucketColumnValues[0]);
      return buckets == null ? Collections.emptyList()
              : buckets.stream().map(b -> new Object[] {b, toBucketColumnValues[0]}).toList();
    };
    return bucketer;
  }

  public static <T> void transferValues(int[] indices, Object[] buffer, List<T> list) {
    for (int i = 0; i < indices.length; i++) {
      buffer[i] = list.get(indices[i]);
    }
  }
}
