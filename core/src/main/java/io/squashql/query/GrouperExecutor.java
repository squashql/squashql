package io.squashql.query;

import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.GroupColumnSetDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.*;
import java.util.function.Function;

public class GrouperExecutor {

  public static Table group(Table table, GroupColumnSetDto groupColumnSetDto) {
    Function<Object[], List<Object[]>> grouper = createGrouper(groupColumnSetDto);

    int[] indexColumnsToRead = new int[groupColumnSetDto.getColumnsForPrefetching().size()];
    for (int i = 0; i < groupColumnSetDto.getColumnsForPrefetching().size(); i++) {
      indexColumnsToRead[i] = table.columnIndex(SqlUtils.squashqlExpression(groupColumnSetDto.getColumnsForPrefetching().get(i)));
    }

    MutableIntSet indexColsInPrefetch = new IntHashSet();
    List<Field> newColumns = groupColumnSetDto.getNewColumns();
    List<Header> finalHeaders = new ArrayList<>(table.headers());
    for (int i = 0; i < newColumns.size(); i++) {
      Field field = newColumns.get(i);
      if (!groupColumnSetDto.getColumnsForPrefetching().contains(field)) {
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
      List<Object[]> groupValuesList = grouper.apply(buffer);
      for (Object[] groupValues : groupValuesList) {
        // Pure copy for everything before
        for (int i = 0; i < originalHeadersSize; i++) {
          newColumnValues.get(i).add(row.get(i));
        }
        for (int i = 0; i < groupValues.length; i++) {
          if (indexColsInPrefetch.contains(i)) {
            newColumnValues.get(i + originalHeadersSize).add(groupValues[i]);
          }
        }
      }
    }

    return new ColumnarTable(
            finalHeaders,
            table.measures(),
            newColumnValues);
  }

  private static Function<Object[], List<Object[]>> createGrouper(GroupColumnSetDto groupColumnSetDto) {
    Map<Object, List<Object>> groupsByValue = new HashMap<>();
    for (Map.Entry<Object, List<Object>> value : groupColumnSetDto.values.entrySet()) {
      for (Object v : value.getValue()) {
        groupsByValue
                .computeIfAbsent(v, k -> new ArrayList<>())
                .add(value.getKey());
      }
    }
    Function<Object[], List<Object[]>> grouper = toGroupColumnValues -> {
      List<Object> groups = groupsByValue.get(toGroupColumnValues[0]);
      return groups == null ? Collections.emptyList()
              : groups.stream().map(b -> new Object[] {b, toGroupColumnValues[0]}).toList();
    };
    return grouper;
  }

  public static <T> void transferValues(int[] indices, Object[] buffer, List<T> list) {
    for (int i = 0; i < indices.length; i++) {
      buffer[i] = list.get(indices[i]);
    }
  }
}
