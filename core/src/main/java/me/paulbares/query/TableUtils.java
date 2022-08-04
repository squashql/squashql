package me.paulbares.query;

import me.paulbares.util.MultipleColumnsSorter;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class TableUtils {

  public static String toString(List<? extends Object> columns,
                                Iterable<List<Object>> rows,
                                Function<Object, String> columnElementPrinters,
                                Function<Object, String> rowElementPrinters) {
    /*
     * leftJustifiedRows - If true, it will add "-" as a flag to format string to
     * make it left justified. Otherwise right justified.
     */
    boolean leftJustifiedRows = false;

    /*
     * Calculate appropriate Length of each column by looking at width of data in
     * each column.
     *
     * Map columnLengths is <column_number, column_length>
     */
    Map<Integer, Integer> columnLengths = new HashMap<>();
    String[] headers = new String[columns.size()];
    for (int h = 0; h < columns.size(); h++) {
      String header = columnElementPrinters.apply(columns.get(h));
      headers[h] = header;
      columnLengths.put(h, header.length());
    }

    Iterator<List<Object>> it = rows.iterator();
    while (it.hasNext()) {
      List<Object> row = it.next();
      for (int i = 0; i < row.size(); i++) {
        int length = rowElementPrinters.apply(row.get(i)).length();
        if (columnLengths.get(i) < length) {
          columnLengths.put(i, length);
        }
      }
    }

    /*
     * Prepare format String
     */
    final StringBuilder formatString = new StringBuilder();
    String flag = leftJustifiedRows ? "-" : "";
    columnLengths.entrySet().stream().forEach(e -> formatString.append("| %" + flag + e.getValue() + "s "));
    formatString.append("|\n");

    /*
     * Prepare line for top, bottom & below header row.
     */
    String line = columnLengths.entrySet().stream().reduce("", (ln, b) -> {
      String templn = "+-";
      templn = templn + Stream.iterate(0, (i -> i < b.getValue()), (i -> ++i)).reduce("", (ln1, b1) -> ln1 + "-", (a1
              , b1) -> a1 + b1);
      templn = templn + "-";
      return ln + templn;
    }, (a, b) -> a + b);
    line = line + "+\n";

    StringBuilder sb = new StringBuilder()
            .append(line)
            .append(String.format(formatString.toString(), headers))
            .append(line);

    it = rows.iterator();
    while (it.hasNext()) {
      List<Object> row = it.next();
      sb.append(String.format(
              formatString.toString(),
              row.stream().map(rowElementPrinters).toList().toArray(new String[0])));
    }
    sb.append(line);
    return sb.toString();
  }

  public static Table order(ColumnarTable table, Map<String, Comparator<?>> comparatorByColumnName) {
    List<List<?>> args = new ArrayList<>();
    List<Comparator<?>> comparators = new ArrayList<>();

    boolean hasComparatorOnMeasure = false;
    for (int i = 0; i < table.headers.size(); i++) {
      boolean isMeasure = Arrays.binarySearch(table.measureIndices, i) >= 0;
      if (isMeasure) {
        String headerName = table.headers.get(i).name();
        hasComparatorOnMeasure |= comparatorByColumnName.containsKey(headerName);
      }
    }

    for (int i = 0; i < table.headers.size(); i++) {
      boolean isColumn = Arrays.binarySearch(table.columnsIndices, i) >= 0;
      String headerName = table.headers.get(i).name();
      Comparator<?> queryComp = comparatorByColumnName.get(headerName);
      // Order a column even if no explicitly asked in the query only if no comparator on any measure
      if (isColumn && !hasComparatorOnMeasure || queryComp != null) {
        args.add(table.getColumnValues(headerName));
        // Always order table. If not defined, use natural order comp.
        comparators.add(queryComp == null ? Comparator.naturalOrder() : queryComp);
      }
    }

    if (args.isEmpty()) {
      return table;
    }

    int[] finalIndices = MultipleColumnsSorter.sort(args, comparators);

    List<List<Object>> values = new ArrayList<>();
    for (List<Object> value : table.values) {
      values.add(reorder(value, finalIndices));
    }

    return new ColumnarTable(table.headers, table.measures, table.measureIndices, table.columnsIndices, values);
  }

  public static List<Object> reorder(List<?> list, int[] order) {
    List<Object> ordered = new ArrayList<>(list);
    for (int i = 0; i < list.size(); i++) {
      ordered.set(i, list.get(order[i]));
    }
    return ordered;
  }
}
