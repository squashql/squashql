package io.squashql.table;

import com.google.common.collect.ImmutableList;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.Header;
import io.squashql.query.dto.SimpleTableDto;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;

public final class PivotTableUtils {

  private PivotTableUtils() {
  }

  // FIXME where to put this?
  public static void toJson(PivotTable pivotTable) {
    List<String> list = pivotTable.table.headers().stream().map(Header::name).toList();

    SimpleTableDto simpleTable = SimpleTableDto.builder()
            .rows(ImmutableList.copyOf(pivotTable.table.iterator()))
            .columns(list)
            .build();

    Map<String, Object> data = Map.of("rows", pivotTable.rows, "columns", pivotTable.columns, "values", pivotTable.values, "table", simpleTable);
    String encodedString = Base64.getEncoder().encodeToString(JacksonUtil.serialize(data).getBytes(StandardCharsets.UTF_8));
    System.out.println("http://localhost:8080?data=" + encodedString);
  }

  static List<List<Object>> pivot(Table table, List<String> rows, List<String> columns, List<String> values) {
    Set<ObjectArrayKey> columnHeaderValues = getHeaderValues(table, columns);

    List<List<Object>> headerColumns = new ArrayList<>(); // The header values for the columns
    int size = columnHeaderValues.size() * values.size();
    // Prepare the lists
    columns.forEach(__ -> {
      List<Object> r = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        r.add(null);
      }
      headerColumns.add(r);
    });

    // Fill the lists
    int columnIndex = 0;
    for (ObjectArrayKey columnHeaderValue : columnHeaderValues) {
      for (int __ = 0; __ < values.size(); __++) {
        for (int rowIndex = 0; rowIndex < columnHeaderValue.a.length; rowIndex++) {
          headerColumns.get(rowIndex).set(columnIndex, columnHeaderValue.a[rowIndex]);
        }
        columnIndex++;
      }
    }

    Set<ObjectArrayKey> rowHeaderValues = getHeaderValues(table, rows);

    int[] rowIndices = getHeaderIndices(table, rows);
    int[] colIndices = getHeaderIndices(table, columns);
    List<List<Object>> cells = new ArrayList<>(rowHeaderValues.size() * size); // The values of the cells.
    rowHeaderValues.forEach(rowPoint -> {
      Object[] buffer = new Object[rows.size() + columns.size()];
      List<Object> r = new ArrayList<>();
      cells.add(r);
      for (int i = 0; i < rowIndices.length; i++) {
        buffer[rowIndices[i]] = rowPoint.a[i];
      }

      for (int i = 0; i < columnHeaderValues.size(); i++) {
        for (int j = 0; j < colIndices.length; j++) {
          buffer[colIndices[j]] = headerColumns.get(j).get(i * values.size());
        }
        int position = ((ColumnarTable) table).pointDictionary.get().getPosition(buffer);

        for (String value : values) {
          r.add(position >= 0 ? table.getColumnValues(value).get(position) : null);
        }
      }
    });

    List<List<Object>> finalRows = new ArrayList<>();
    Supplier<List<Object>> listSpwaner = () -> {
      finalRows.add(new ArrayList<>(rows.size() + size));
      return finalRows.get(finalRows.size() - 1);
    };
    for (int i = 0; i < columns.size(); i++) {
      List<Object> r = listSpwaner.get();
      for (int j = 0; j < rows.size(); j++) {
        r.add(columns.get(i)); // recopy name of the column
      }
      r.addAll(headerColumns.get(i));
    }

    List<Object> r = listSpwaner.get();
    r.addAll(rows);
    for (int i = 0; i < columnHeaderValues.size(); i++) {
      r.addAll(values); // Recopy measure names
    }

    int[] index = new int[1];
    rowHeaderValues.forEach(a -> {
      List<Object> rr = listSpwaner.get();
      rr.addAll(Arrays.asList(a.a));
      rr.addAll(cells.get(index[0]++));
    });

    return finalRows;
  }

  /**
   * Gets all the possible values in the table for the given headers. The values stored in {@link ObjectArrayKey} are
   * aligned with the headers and the order in the resulting set is preserved.
   */
  private static Set<ObjectArrayKey> getHeaderValues(Table table, List<String> headers) {
    int[] mapping = getHeaderIndices(table, headers);

    LinkedHashSet<ObjectArrayKey> result = new LinkedHashSet<>();
    table.forEach(row -> {
      Object[] columnValues = new Object[headers.size()];
      for (int i = 0; i < headers.size(); i++) {
        columnValues[i] = row.get(mapping[i]);
      }
      result.add(new ObjectArrayKey(columnValues));
    });
    return result;
  }

  /**
   * Gets the indices of the given headers in the table.
   */
  private static int[] getHeaderIndices(Table table, List<String> headers) {
    int[] mapping = new int[headers.size()];
    Arrays.fill(mapping, -1);
    for (int i = 0; i < headers.size(); i++) {
      for (int j = 0; j < table.headers().size(); j++) {
        if (table.headers().get(j).name().equals(headers.get(i))) {
          mapping[i] = j;
          break;
        }
      }
    }
    return mapping;
  }

  private record ObjectArrayKey(Object[] a) {

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ObjectArrayKey that = (ObjectArrayKey) o;
      return Arrays.equals(this.a, that.a);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(this.a);
    }

    @Override
    public String toString() {
      return Arrays.toString(this.a);
    }
  }
}
