package io.squashql.table;

import io.squashql.query.Header;
import io.squashql.util.ListUtils;
import io.squashql.util.NullAndTotalComparator;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.util.*;
import java.util.function.Supplier;

public final class PivotTableUtils {

  private PivotTableUtils() {
  }

  static List<List<Object>> pivot(PivotTable pivotTable) {
    Table table = pivotTable.table;
    List<String> rows = pivotTable.rows;
    List<String> columns = pivotTable.columns;
    List<String> values = pivotTable.values;

    Set<ObjectArrayKey> columnHeaderValues = getHeaderValues(table, columns);

    List<List<Object>> headerColumns = new ArrayList<>(); // The header values for the columns
    int size = columnHeaderValues.size() * values.size();
    // Prepare the lists
    columns.forEach(__ -> {
      headerColumns.add(ListUtils.createListWithNulls(size));
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
        int position = table.pointDictionary().getPosition(buffer);

        for (String value : values) {
          r.add(position >= 0 ? table.getColumnValues(value).get(position) : null);
        }
      }
    });

    List<List<Object>> finalRows = new ArrayList<>();
    Supplier<List<Object>> listSpawner = () -> {
      finalRows.add(new ArrayList<>(rows.size() + size));
      return finalRows.get(finalRows.size() - 1);
    };
    for (int i = 0; i < columns.size(); i++) {
      List<Object> r = listSpawner.get();
      for (int j = 0; j < rows.size(); j++) {
        r.add(columns.get(i)); // recopy name of the column
      }
      r.addAll(headerColumns.get(i));
    }

    List<Object> r = listSpawner.get();
    r.addAll(rows);
    for (int i = 0; i < columnHeaderValues.size(); i++) {
      r.addAll(values); // Recopy measure names
    }

    int[] index = new int[1];
    rowHeaderValues.forEach(a -> {
      List<Object> rr = listSpawner.get();
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

  private static Map<String, BitSet> findNullValuesOnEntireColumn(PivotTable pivotTable) {
    int[] rowIndices = getHeaderIndices(pivotTable.table, pivotTable.columns);
    int[] measureIndices = getHeaderIndices(pivotTable.table, pivotTable.values);
    int[] line = new int[1];
    Map<ObjectArrayKey, Set<Object>[]> distinctValuesByKey = new HashMap<>();
    Map<ObjectArrayKey, IntArrayList> lineByKey = new HashMap<>();
    pivotTable.table.forEach(row -> {
      Object[] coord = new Object[rowIndices.length];
      for (int i = 0; i < rowIndices.length; i++) {
        coord[i] = row.get(rowIndices[i]);
      }

      ObjectArrayKey key = new ObjectArrayKey(coord);
      Set<Object>[] distinctValues = distinctValuesByKey.computeIfAbsent(key, k -> {
        Set[] sets = new Set[measureIndices.length];
        for (int i = 0; i < measureIndices.length; i++) {
          sets[i] = new HashSet();
        }
        return sets;
      });

      for (int i = 0; i < measureIndices.length; i++) {
        distinctValues[i].add(row.get(measureIndices[i]));
      }
      lineByKey.computeIfAbsent(key, k -> new IntArrayList()).add(line[0]);
      line[0]++;
    });

    BitSet[] bitSets = new BitSet[measureIndices.length];
    for (int i = 0; i < measureIndices.length; i++) {
      bitSets[i] = new BitSet(line[0]);
    }
    for (Map.Entry<ObjectArrayKey, Set<Object>[]> entry : distinctValuesByKey.entrySet()) {
      Set<Object>[] distinctValues = entry.getValue();
      for (int i = 0; i < measureIndices.length; i++) {
        if (distinctValues[i].size() == 1 && distinctValues[i].iterator().next() == null) {
          // only nulls values.
          int finalI = i;
          lineByKey.get(entry.getKey()).forEach(index -> bitSets[finalI].set(index));
        }
      }
    }

    Map<String, BitSet> result = new HashMap<>();
    for (int i = 0; i < pivotTable.values.size(); i++) {
      result.put(pivotTable.values.get(i), bitSets[i]);
    }
    return result;
  }

  /**
   * Generates cells of the pivot table. Entire column of null values are removed if minify set to true or null (default).
   * For instance, if the pivot table looks like this:
   * <pre>
   * +-------------+-------------+-------------+-------------+--------+------------+---------------------+---------------------+
   * |    category |    category | Grand Total | Grand Total |  extra |      extra | minimum expenditure | minimum expenditure |
   * |   continent |     country |      amount |  population | amount | population |              amount |          population |
   * +-------------+-------------+-------------+-------------+--------+------------+---------------------+---------------------+
   * | Grand Total | Grand Total |        56.0 |       465.0 |   17.0 |       null |                39.0 |                null |
   * |          am |       Total |        39.0 |       330.0 |   10.0 |       null |                29.0 |                null |
   * |          am |         usa |        39.0 |       330.0 |   10.0 |       null |                29.0 |                null |
   * |          eu |       Total |        17.0 |       135.0 |    7.0 |       null |                10.0 |                null |
   * |          eu |      france |         8.0 |        70.0 |    2.0 |       null |                 6.0 |                null |
   * |          eu |          uk |         9.0 |        65.0 |    5.0 |       null |                 4.0 |                null |
   * +-------------+-------------+-------------+-------------+--------+------------+---------------------+---------------------+
   * </pre>
   * The two columns for extra/population and minimum expenditure/population are removed.
   */
  public static List<Map<String, Object>> generateCells(PivotTable pivotTable, Boolean minify) {
    Map<String, BitSet> empty = new HashMap<>();
    for (String value : pivotTable.values) {
      empty.put(value, null);
    }

    Map<String, BitSet> bitSetByValue = minify == null || minify
            ? PivotTableUtils.findNullValuesOnEntireColumn(pivotTable)
            : empty;

    List<Map<String, Object>> cells = new ArrayList<>((int) pivotTable.table.count());
    List<String> headerNames = pivotTable.table.headers().stream().map(Header::name).toList();
    int[] line = new int[1];
    pivotTable.table.forEach(row -> {
      Map<String, Object> cell = new HashMap<>();
      for (int i = 0; i < row.size(); i++) {
        Object value = row.get(i);

        BitSet bitSet = bitSetByValue.get(headerNames.get(i));
        if ((bitSet == null && !NullAndTotalComparator.isTotal(value)) || (bitSet != null && !bitSet.get(line[0]))) {
          cell.put(headerNames.get(i), value);
        }
      }
      line[0]++;
      cells.add(cell);
    });
    return cells;
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
