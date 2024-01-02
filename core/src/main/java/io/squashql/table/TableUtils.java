package io.squashql.table;

import com.google.common.base.Suppliers;
import io.squashql.query.*;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.MetadataItem;
import io.squashql.query.dto.QueryDto;
import io.squashql.type.TypedField;
import io.squashql.util.MultipleColumnsSorter;
import io.squashql.util.NullAndTotalComparator;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TableUtils {

  public static String toString(Iterable<List<Object>> rows,
                                Function<Object, String> rowElementPrinters,
                                Predicate<Integer> predicate) {
    return toString(null, rows, null, rowElementPrinters, predicate);
  }

  public static String toString(List<? extends Object> columns,
                                Iterable<List<Object>> rows,
                                Function<Object, String> columnElementPrinters,
                                Function<Object, String> rowElementPrinters) {
    return toString(columns, rows, columnElementPrinters, rowElementPrinters, __ -> false);
  }

  public static String toString(List<? extends Object> columns,
                                Iterable<List<Object>> rows,
                                Function<Object, String> columnElementPrinters,
                                Function<Object, String> rowElementPrinters,
                                Predicate<Integer> predicate) {
    /*
     * leftJustifiedRows - If true, it will add "-" as a flag to format string to
     * make it left justified. Otherwise, right justified.
     */
    boolean leftJustifiedRows = false;

    /*
     * Calculate appropriate Length of each column by looking at width of data in
     * each column.
     *
     * Map columnLengths is <column_number, column_length>
     */
    Map<Integer, Integer> columnLengths = new HashMap<>();
    String[] headers = new String[columns != null ? columns.size() : 0];
    if (columns != null) {
      for (int h = 0; h < columns.size(); h++) {
        String header = columnElementPrinters.apply(columns.get(h));
        headers[h] = header;
        columnLengths.put(h, header.length());
      }
    }

    Iterator<List<Object>> it = rows.iterator();
    while (it.hasNext()) {
      List<Object> row = it.next();
      for (int i = 0; i < row.size(); i++) {
        int length = rowElementPrinters.apply(row.get(i)).length();
        if (columnLengths.computeIfAbsent(i, __ -> 0) < length) {
          columnLengths.put(i, length);
        }
      }
    }

    /*
     * Prepare format String
     */
    final StringBuilder formatString = new StringBuilder();
    String flag = leftJustifiedRows ? "-" : "";
    columnLengths.forEach((key, value) -> formatString.append("| %" + flag + value + "s "));
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

    StringBuilder sb = new StringBuilder().append(line);
    if (columns != null) {
      sb.append(String.format(formatString.toString(), headers))
              .append(line);
    }

    it = rows.iterator();
    int count = 0;
    while (it.hasNext()) {
      List<Object> row = it.next();
      sb.append(String.format(
              formatString.toString(),
              row.stream().map(rowElementPrinters).toList().toArray(new String[0])));
      if (predicate.test(count)) {
        sb.append(line);
      }
      count++;
    }
    sb.append(line);
    return sb.toString();
  }

  public static List<MetadataItem> buildTableMetadata(Table t) {
    List<MetadataItem> metadata = new ArrayList<>();
    for (Header header : t.headers()) {
      Optional<Measure> optionalMeasure = t.measures().stream()
              .filter(m -> m.alias().equals(header.name()))
              .findAny();
      if (header.isMeasure() && optionalMeasure.isPresent()) {
        Measure measure = optionalMeasure.get();
        String expression = measure.expression();
        if (expression == null) {
          measure = measure.withExpression(MeasureUtils.createExpression(measure));
        }
        metadata.add(new MetadataItem(header.name(), measure.expression(), header.type()));
      } else {
        metadata.add(new MetadataItem(header.name(), header.name(), header.type()));
      }
    }
    return metadata;
  }


  /**
   * Selects and reorder the columns to match the selection and order in the query.
   */
  public static ColumnarTable selectAndOrderColumns(QueryResolver queryResolver,
                                                    ColumnarTable table,
                                                    QueryDto queryDto) {
    // Resolve fields...
    List<TypedField> finalFields = new ArrayList<>();
    queryDto.columnSets.values()
            .forEach(cs -> finalFields.addAll(cs.getNewColumns()
                    .stream()
                    .map(queryResolver::getTypedField)
                    .toList()));
    finalFields.addAll(queryDto.columns.stream().map(queryResolver::getTypedField).toList());

    // ... and then get their string representation.
    List<String> finalColumns = finalFields.stream().map(SqlUtils::squashqlExpression).toList();
    return selectAndOrderColumns(table, finalColumns, queryDto.measures);
  }

  public static ColumnarTable selectAndOrderColumns(ColumnarTable table, List<String> columns, List<Measure> measures) {
    List<Header> headers = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (String finalColumn : columns) {
      headers.add(table.getHeader(finalColumn));
      values.add(Objects.requireNonNull(table.getColumnValues(finalColumn)));
    }
    for (Measure measure : measures) {
      headers.add(table.getHeader(measure));
      values.add(Objects.requireNonNull(table.getAggregateValues(measure)));
    }
    return new ColumnarTable(headers, new HashSet<>(measures), values);
  }

  /**
   * Naturally order the rows from left to right.
   */
  public static Table orderRows(ColumnarTable table) {
    return orderRows(table, Collections.emptyMap(), Collections.emptySet());
  }

  public static Table orderRows(ColumnarTable table,
                                Map<String, Comparator<?>> comparatorByColumnName,
                                Collection<ColumnSet> columnSets) {
    List<List<?>> args = new ArrayList<>();
    List<Comparator<?>> comparators = new ArrayList<>();
    Map<String, Comparator<?>> copy = new HashMap<>(comparatorByColumnName);

    columnSets.forEach(columnSet -> {
      if (columnSet.getColumnSetKey() != ColumnSetKey.BUCKET) {
        throw new IllegalArgumentException("Unexpected column set type " + columnSet);
      }
      BucketColumnSetDto cs = (BucketColumnSetDto) columnSet;
      // Remove from the map of comparators to use default one when only none is defined for regular column
      copy.remove(cs.newField.name());
      copy.remove(cs.field.name());
    });

    List<Header> headers = table.headers;
    for (Header header : headers) {
      String headerName = header.name();
      Comparator<?> queryComp = comparatorByColumnName.get(headerName);
      // Order by default if not explicitly asked in the query. Otherwise, respect the order.
      if (queryComp != null || copy.isEmpty()) {
        args.add(table.getColumnValues(headerName));
        // Always order table. If not defined, use natural order comp.
        comparators.add(queryComp == null ? NullAndTotalComparator.nullsLastAndTotalsFirst(Comparator.naturalOrder())
                : queryComp);
      }
    }

    if (args.isEmpty()) {
      return table;
    }

    // Special case for the CS comparators.
    int[] contextIndices = new int[args.size()];
    Arrays.fill(contextIndices, -1);
    for (ColumnSet columnSet : new HashSet<>(columnSets)) {
      BucketColumnSetDto cs = (BucketColumnSetDto) columnSet;
      // cs.field can appear multiple times in the table.
      table.columnIndices(cs.field).forEach(i -> contextIndices[i] = table.columnIndex(cs.newField.name()));
    }

    int[] finalIndices = MultipleColumnsSorter.sort(args, comparators, contextIndices);

    List<List<Object>> values = new ArrayList<>();
    for (List<Object> value : table.values) {
      values.add(reorder(value, finalIndices));
    }

    return new ColumnarTable(headers, table.measures, values);
  }

  public static List<Object> reorder(List<?> list, int[] order) {
    List<Object> ordered = new ArrayList<>(list);
    for (int i = 0; i < list.size(); i++) {
      ordered.set(i, list.get(order[i]));
    }
    return ordered;
  }

  /**
   * Replaces cell values containing {@link SQLTranslator#TOTAL_CELL} with {@link QueryEngine#GRAND_TOTAL} or
   * {@link QueryEngine#TOTAL}.
   */
  public static Table replaceTotalCellValues(ColumnarTable table, boolean hasTotal) {
    return !hasTotal ? table : replaceTotalCellValues(table, table.headers().stream().map(Header::name).toList(), List.of());
  }

  /**
   * Same as {@link #replaceTotalCellValues(ColumnarTable, boolean)} but adapted to pivot table.
   */
  public static Table replaceTotalCellValues(ColumnarTable table, List<String> rows, List<String> columns) {
    // To lazily copy the table when needed.
    boolean[] lazilyCreated = new boolean[1];
    Supplier<Table> finalTable = Suppliers.memoize(() -> {
      List<List<Object>> newValues = new ArrayList<>();
      for (int i = 0; i < table.headers.size(); i++) {
        newValues.add(new ArrayList<>(table.getColumn(i)));
      }
      lazilyCreated[0] = true;
      return new ColumnarTable(table.headers, table.measures, newValues);
    });

    for (int rowIndex = 0; rowIndex < table.count(); rowIndex++) {
      boolean grandTotalRow = true;
      boolean grandTotalCol = true;
      String total = QueryEngine.TOTAL;
      for (int i = 0; i < table.headers().size(); i++) {
        Header header = table.headers().get(i);
        if (!header.isMeasure()) {
          boolean isTotalCell = SQLTranslator.TOTAL_CELL.equals(table.getColumn(i).get(rowIndex));
          if (isTotalCell) {
            finalTable.get().getColumn(i).set(rowIndex, total);
          }

          if (rows.contains(header.name())) {
            grandTotalRow &= isTotalCell;
          }

          if (columns.contains(header.name())) {
            grandTotalCol &= isTotalCell;
          }
        }
      }

      int finalRowIndex = rowIndex;
      BiConsumer<Boolean, List<String>> consumer = (grandTotal, axis) -> {
        if (grandTotal) {
          for (int i = 0; i < table.headers().size(); i++) {
            Header header = table.headers().get(i);
            if (!header.isMeasure() && axis.contains(header.name())) {
              finalTable.get().getColumn(i).set(finalRowIndex, QueryEngine.GRAND_TOTAL);
            }
          }
        }
      };
      consumer.accept(grandTotalRow, rows);
      consumer.accept(grandTotalCol, columns);
    }

    return lazilyCreated[0] ? finalTable.get() : table;
  }
}
