package me.paulbares.query;

import me.paulbares.store.Field;
import me.paulbares.util.MultipleColumnsSorter;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class TableUtils {

  public static final String NAME_KEY = "name";
  public static final String TYPE_KEY = "type";
  public static final String EXPRESSION_KEY = "expression";

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

  public static List<Map<String, Object>> buildTableMetadata(Table t) {
    List<Map<String, Object>> metadata = new ArrayList<>();
    for (Field field : t.headers()) {
      Map<String, Object> fieldMetadata = new HashMap<>();
      fieldMetadata.put(NAME_KEY, field.name());
      fieldMetadata.put(TYPE_KEY, field.type().getSimpleName().toLowerCase());
      metadata.add(fieldMetadata);
    }

    int index = 0;
    for (int i : t.measureIndices()) {
      Measure m = t.measures().get(index);
      metadata.get(i).put(EXPRESSION_KEY, m.expression());
      index++;
    }
    return metadata;
  }

  public static Table order(ColumnarTable table, Map<String, Comparator<?>> comparatorByColumnName) {
    List<List<?>> args = new ArrayList<>();
    List<Comparator<?>> comparators = new ArrayList<>();
    for (Field header : table.headers) {
      args.add(table.getColumnValues(header.name()));
      Comparator<?> queryComp = comparatorByColumnName.get(header.name());
      // Always order table. If not defined, use natural order comp.
      comparators.add(queryComp == null ? Comparator.naturalOrder() : queryComp);
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
