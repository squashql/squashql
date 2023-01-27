package io.squashql.query;

import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.MetadataItem;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import io.squashql.util.NullAndTotalComparator;
import io.squashql.util.MultipleColumnsSorter;
import io.squashql.util.Queries;

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

  public static List<MetadataItem> buildTableMetadata(Table t) {
    List<MetadataItem> metadata = new ArrayList<>();
    for (Header header : t.headers()) {
      Field field = header.field();
      Optional<Measure> optionalMeasure = t.measures().stream().filter(m -> m.alias().equals(header.field().name()))
              .findAny();
      if (header.isMeasure() && optionalMeasure.isPresent()) {
        Measure measure = optionalMeasure.get();
        String expression = measure.expression();
        if (expression == null) {
          measure = measure.withExpression(MeasureUtils.createExpression(measure));
        }
        metadata.add(new MetadataItem(field.name(), measure.expression(), field.type()));
      } else {
        metadata.add(new MetadataItem(field.name(), field.name(), field.type()));
      }
    }
    return metadata;
  }


  /**
   * Selects and reorder the columns to match the selection and order in the query.
   */
  public static ColumnarTable selectAndOrderColumns(ColumnarTable table, QueryDto queryDto) {
    List<String> finalColumns = new ArrayList<>();
    queryDto.columnSets.values()
            .forEach(cs -> finalColumns.addAll(cs.getNewColumns().stream().map(Field::name).toList()));
    finalColumns.addAll(queryDto.columns);

    // Once complete, construct the final result with columns in correct order.
    List<Header> headers = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (String finalColumn : finalColumns) {
      headers.add(new Header(table.getField(finalColumn), false));
      values.add(Objects.requireNonNull(table.getColumnValues(finalColumn)));
    }

    for (Measure measure : queryDto.measures) {
      headers.add(new Header(table.getField(measure), true));
      values.add(Objects.requireNonNull(table.getAggregateValues(measure)));
    }

    return new ColumnarTable(headers, queryDto.measures, values);
  }

  public static Table orderRows(ColumnarTable table, QueryDto queryDto) {
    Map<String, Comparator<?>> comparatorByColumnName = Queries.getComparators(queryDto);
    List<List<?>> args = new ArrayList<>();
    List<Comparator<?>> comparators = new ArrayList<>();

    boolean hasComparatorOnMeasure = false;
    List<Header> headers = table.headers;
    for (Header header : headers) {
      if (header.isMeasure()) {
        hasComparatorOnMeasure |= comparatorByColumnName.containsKey(header.field().name());
      }
    }

    for (int i = 0; i < headers.size(); i++) {
      String headerName = headers.get(i).field().name();
      Comparator<?> queryComp = comparatorByColumnName.get(headerName);
      // Order a column even if not explicitly asked in the query only if no comparator on any measure
      if (queryComp != null || (!headers.get(i).isMeasure() && !hasComparatorOnMeasure)) {
        args.add(table.getColumnValues(headerName));
        // Always order table. If not defined, use natural order comp.
        comparators.add(queryComp == null ? NullAndTotalComparator.nullsLastAndTotalsFirst(Comparator.naturalOrder())
                : queryComp);
      }
    }

    if (args.isEmpty()) {
      return table;
    }

    int[] contextIndices = new int[args.size()];
    java.util.Arrays.fill(contextIndices, -1);
    ColumnSet bucket = queryDto.columnSets.get(ColumnSetKey.BUCKET);
    if (bucket != null) {
      BucketColumnSetDto cs = (BucketColumnSetDto) bucket;
      contextIndices[table.columnIndex(cs.field)] = table.columnIndex(cs.name);
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
  public static Table replaceTotalCellValues(ColumnarTable table, QueryDto queryDto) {
    if (queryDto.rollupColumns.isEmpty()) {
      return table;
    }

    for (int rowIndex = 0; rowIndex < table.count(); rowIndex++) {
      boolean grandTotal = true;
      String total = QueryEngine.TOTAL;
      for (int i = 0; i < table.headers().size(); i++) {
        if (!table.headers().get(i).isMeasure()) {
          boolean isTotalCell = SQLTranslator.TOTAL_CELL.equals(table.getColumn(i).get(rowIndex));
          if (isTotalCell) {
            table.getColumn(i).set(rowIndex, total);
          }
          grandTotal &= isTotalCell;
        }
      }

      if (grandTotal) {
        for (int i = 1; i < table.headers().size(); i++) {
          if (!table.headers().get(i).isMeasure()) {
            table.getColumn(i).set(rowIndex, null);
          }
        }
      }
    }

    return table;
  }
}
