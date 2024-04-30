package io.squashql.table;

import com.google.common.base.Suppliers;
import io.squashql.query.ColumnSet;
import io.squashql.query.Header;
import io.squashql.query.Measure;
import io.squashql.query.QueryExecutor;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.QueryScope;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.GroupColumnSetDto;
import io.squashql.query.dto.MetadataItem;
import io.squashql.query.dto.QueryDto;
import io.squashql.util.MultipleColumnsSorter;
import io.squashql.util.NullAndTotalComparator;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.squashql.util.ListUtils.reorder;

public class TableUtils {

  public static String toString(Iterable<List<Object>> rows,
                                Function<Object, String> rowElementPrinters,
                                Predicate<Integer> predicate) {
    return toString(null, rows, null, rowElementPrinters, predicate);
  }

  public static String toString(List<?> columns,
                                Iterable<List<Object>> rows,
                                Function<Object, String> columnElementPrinters,
                                Function<Object, String> rowElementPrinters) {
    return toString(columns, rows, columnElementPrinters, rowElementPrinters, __ -> false);
  }

  public static String toString(List<?> columns,
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
      Optional<CompiledMeasure> optionalMeasure = t.measures().stream()
              .filter(m -> m.alias().equals(header.name()))
              .findAny();
      if (header.isMeasure() && optionalMeasure.isPresent()) {
        // FIXME deactivated for now
        metadata.add(new MetadataItem(header.name(), header.name(), header.type()));
//        metadata.add(new MetadataItem(header.name(), MeasureUtils.createExpression(optionalMeasure.get()), header.type()));
      } else {
        metadata.add(new MetadataItem(header.name(), header.name(), header.type()));
      }
    }
    return metadata;
  }


  /**
   * Selects and reorder the columns to match the selection and order in the query.
   */
  public static ColumnarTable selectAndOrderColumns(QueryDto queryDto,
                                                    ColumnarTable table) {
    // Resolve fields...
    List<String> finalColumns = new ArrayList<>();
    queryDto.columnSets.values()
            .forEach(cs -> finalColumns.addAll(cs.getNewColumns()
                    .stream()
                    .map(SqlUtils::squashqlExpression)
                    .toList()));
    finalColumns.addAll(queryDto.columns.stream().map(SqlUtils::squashqlExpression).toList());
    return selectAndOrderColumns(table, finalColumns, queryDto.measures.stream().map(Measure::alias).toList());
  }

  public static ColumnarTable selectAndOrderColumns(ColumnarTable table, List<String> columns, List<String> measureAliases) {
    List<Header> headers = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (String finalColumn : columns) {
      headers.add(table.getHeader(finalColumn));
      values.add(Objects.requireNonNull(table.getColumnValues(finalColumn)));
    }
    for (String alias : measureAliases) {
      headers.add(table.getHeader(alias));
      values.add(Objects.requireNonNull(table.getColumnValues(alias)));
    }
    return new ColumnarTable(headers,
            table.measures().stream().filter(cm -> measureAliases.contains(cm.alias())).collect(Collectors.toSet()),
            values);
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

    // Start with the explicit comparators.
    List<String> namesForOrdering = new ArrayList<>(table.headers().size());
    comparatorByColumnName.forEach((columnName, comp) -> {
      args.add(table.getColumnValues(columnName));
      comparators.add(comp);
      namesForOrdering.add(columnName);
    });

    // Order by default if not explicitly asked in the query.
    List<Header> headers = table.headers();
    for (Header header : headers) {
      String headerName = header.name();
      if (!comparatorByColumnName.containsKey(headerName)) {
        namesForOrdering.add(headerName);
        args.add(table.getColumnValues(headerName));
        // Always order table. If not defined, use natural order comp.
        comparators.add(NullAndTotalComparator.nullsLastAndTotalsFirst(Comparator.naturalOrder()));
      }
    }

    if (args.isEmpty()) {
      return table;
    }

    // Special case for the CS comparators.
    int[] contextIndices = new int[args.size()];
    Arrays.fill(contextIndices, -1);
    for (ColumnSet columnSet : new HashSet<>(columnSets)) {
      GroupColumnSetDto cs = (GroupColumnSetDto) columnSet;
      // cs.field can appear multiple times in the table.
      int index = namesForOrdering.indexOf(SqlUtils.squashqlExpression(cs.field));
      contextIndices[index] = namesForOrdering.indexOf(SqlUtils.squashqlExpression(cs.newField));
    }

    int[] finalIndices = MultipleColumnsSorter.sort(args, comparators, contextIndices);

    List<List<Object>> values = new ArrayList<>();
    for (List<Object> value : table.getColumns()) {
      values.add(reorder(value, finalIndices));
    }

    return new ColumnarTable(headers, table.measures(), values);
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
      lazilyCreated[0] = true;
      return table.copy();
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

  /**
   * Changes the content of the input table to remove columns corresponding to grouping() (columns that help to identify
   * rows containing totals) and write {@link SQLTranslator#TOTAL_CELL} in the corresponding cells. The input table is
   * <b>NOT</b> modified, a copy is created instead.
   * <pre>
   *   Input:
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   | scenario | category | ___grouping___scenario___ | ___grouping___category___ |    p | _contributors_count_ |  q |
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   |     base |    drink |                         0 |                         0 |  2.0 |                    1 | 10 |
   *   |     base |     food |                         0 |                         0 |  3.0 |                    1 | 20 |
   *   |     base |    cloth |                         0 |                         0 | 10.0 |                    1 |  3 |
   *   |     null |     null |                         1 |                         1 | 15.0 |                    3 | 33 |
   *   |     base |     null |                         0 |                         1 | 15.0 |                    3 | 33 |
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   Output:
   *   +-------------+-------------+------+----------------------+----+
   *   |    scenario |    category |    p | _contributors_count_ |  q |
   *   +-------------+-------------+------+----------------------+----+
   *   |        base |       drink |  2.0 |                    1 | 10 |
   *   |        base |        food |  3.0 |                    1 | 20 |
   *   |        base |       cloth | 10.0 |                    1 |  3 |
   *   | ___total___ | ___total___ | 15.0 |                    3 | 33 |
   *   |        base | ___total___ | 15.0 |                    3 | 33 |
   *   +-------------+-------------+------+----------------------+----+
   * </pre>
   */
  public static Table replaceNullCellsByTotal(Table input, QueryScope scope) {
    Map<String, String> groupingHeaders = findGroupingHeaderNamesByBaseName(input.headers(), scope);
    if (!groupingHeaders.isEmpty()) {
      List<List<Object>> newValues = new ArrayList<>();
      for (int i = 0; i < input.headers().size(); i++) {
        newValues.add(new ArrayList<>(input.getColumn(i)));
      }
      ColumnarTable copy = new ColumnarTable(input.headers(), input.measures(), newValues);

      Set<String> groupingNames = groupingHeaders.keySet();
      for (int i = 0; i < copy.headers().size(); i++) {
        Header header = copy.headers().get(i);
        List<Object> columnValues = copy.getColumn(i);
        if (groupingNames.contains(header.name())) {
          String baseName = groupingHeaders.get(header.name());
          List<Object> baseColumnValues = copy.getColumnValues(baseName);
          for (int rowIndex = 0; rowIndex < columnValues.size(); rowIndex++) {
            Object o = columnValues.get(rowIndex);
            if (o != null && ((Number) o).longValue() == 1) {
              // It is a total if == 1. It is cast as Number because the type is Byte with Spark, Long with
              // ClickHouse...
              baseColumnValues.set(rowIndex, SQLTranslator.TOTAL_CELL);
            }
          }
        }
      }
      return copy;
    }
    return input;
  }

  /**
   * [___grouping___field_name_a___, field_name_a]
   * [___grouping___field_name_b___, field_name_b]
   * ...
   */
  private static Map<String, String> findGroupingHeaderNamesByBaseName(List<Header> headers, QueryScope scope) {
    Set<String> rollupExpressions = QueryExecutor.generateGroupingMeasures(scope).keySet();
    Map<String, String> groupingHeaders = new HashMap<>();
    // rollupExpressions can be empty when working with vector. In that case, we rely on the header name only.
    // Do it in that order. First this...
    for (Header header : headers) {
      String baseName = SqlUtils.extractFieldFromGroupingAlias(header.name());
      if (baseName != null) {
        groupingHeaders.put(header.name(), baseName);
      }
    }

    // Then this.
    for (Header header : headers) {
      if (!header.isMeasure() && rollupExpressions.contains(header.name())) {
        String groupingAlias = SqlUtils.groupingAlias(header.name().replace(".", "_")); // The alias we are looking for
        for (Header m : headers) {
          if (m.isMeasure() && m.name().equals(groupingAlias)) {
            String baseName = SqlUtils.extractFieldFromGroupingAlias(groupingAlias);
            if (baseName != null) {
              groupingHeaders.put(m.name(), header.name());
            }
          }
        }
      }
    }
    return groupingHeaders;
  }

  public static List<Map<String, Object>> generateCells(Table table, Boolean minify) {
    Set<String> measuresWithNullValuesOnEntireColumn;
    if (minify == null || minify) {
      measuresWithNullValuesOnEntireColumn = table.measures().stream().map(CompiledMeasure::alias).collect(Collectors.toCollection(HashSet::new));
      Set<String> toRemoveFromCandidates = new HashSet<>();
      for (String m : measuresWithNullValuesOnEntireColumn) {
        List<Object> columnValues = table.getColumnValues(m);
        for (Object columnValue : columnValues) {
          if (columnValue != null) {
            toRemoveFromCandidates.add(m);
            break;
          }
        }
      }
      measuresWithNullValuesOnEntireColumn.removeAll(toRemoveFromCandidates);
    } else {
      measuresWithNullValuesOnEntireColumn = Collections.emptySet();
    }

    List<Map<String, Object>> cells = new ArrayList<>((int) table.count());
    List<String> headerNames = table.headers().stream().map(Header::name).toList();
    int[] sizeOfCell = new int[]{-1};
    table.forEach(row -> {
      if (sizeOfCell[0] == -1) {
        sizeOfCell[0] = row.size() - measuresWithNullValuesOnEntireColumn.size();
      }
      Map<String, Object> cell = new HashMap<>(sizeOfCell[0]);
      for (int i = 0; i < row.size(); i++) {
        if (measuresWithNullValuesOnEntireColumn.contains(headerNames.get(i))) {
          continue;
        }
        Object value = row.get(i);
        if (!NullAndTotalComparator.isTotal(value)) {
          cell.put(headerNames.get(i), value);
        }
      }
      cells.add(cell);
    });
    return cells;
  }
}
