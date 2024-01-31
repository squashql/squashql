package io.squashql.query;

import io.squashql.query.compiled.CompiledBucketColumnSet;
import io.squashql.query.database.SqlUtils;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.TypedField;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.*;
import java.util.function.Function;

public class BucketerExecutor {

  public static Table bucket(Table table, CompiledBucketColumnSet bucketColumnSet) {
    Function<Object[], List<Object[]>> bucketer = createBucketer(bucketColumnSet);

    int[] indexColumnsToRead = new int[bucketColumnSet.columnsForPrefetching().size()];
    for (int i = 0; i < bucketColumnSet.columnsForPrefetching().size(); i++) {
      indexColumnsToRead[i] = table.columnIndex(SqlUtils.squashqlExpression(bucketColumnSet.columnsForPrefetching().get(i)));
    }

    MutableIntSet indexColsInPrefetch = new IntHashSet();
    List<TypedField> newColumns = bucketColumnSet.newColumns();
    List<Header> finalHeaders = new ArrayList<>(table.headers());
    for (int i = 0; i < newColumns.size(); i++) {
      TypedField field = newColumns.get(i);
      if (!bucketColumnSet.columnsForPrefetching().contains(field)) {
        indexColsInPrefetch.add(i);
      }
      Header header = new Header(SqlUtils.squashqlExpression(field), String.class, false);
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

  private static Function<Object[], List<Object[]>> createBucketer(CompiledBucketColumnSet bucketColumnSet) {
    Map<String, List<String>> bucketsByValue = new HashMap<>();
    for (Map.Entry<String, List<String>> value : bucketColumnSet.values().entrySet()) {
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
