package io.squashql.table;

import io.squashql.query.ColumnarTable;
import io.squashql.query.Measure;
import io.squashql.query.Table;
import io.squashql.query.database.SQLTranslator;
import io.squashql.store.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

class MergeTables {

  private MergeTables() {
  }

  static Table mergeTables(List<Table> tables) {
    Table result = null;
    for (Table table : tables) {
      result = result == null ? table : mergeTables(result, table);
    }
    return result;
  }

  /**
   * Merge two tables into only one resulting table. We choose to first get all columns and then all measures.
   * Hypothesis:
   * <ul>
   *   <li>the input tables must have their common columns at the beginning, in the same order, and sorted</li>
   *   <li>every table must have all its columns first, then its measures</li>
   *   <li>the input tables must not share any measure</li>
   * </ul>
   */
  static Table mergeTables(Table leftTable, Table rightTable) {
    if (leftTable == null) {
      return rightTable;
    }
    if (rightTable == null) {
      return leftTable;
    }

    List<Field> leftHeaders = leftTable.headers();
    List<Field> rightHeaders = rightTable.headers();
    final List<Field> mergedTableHeaders = mergeHeaders(leftHeaders, leftTable.columnIndices(),
            leftTable.measureIndices(), rightHeaders, rightTable.columnIndices(), rightTable.measureIndices());
    final List<Measure> mergedTableMeasures = mergeMeasures(leftTable.measures(), rightTable.measures());
    int mergedTableHeaderSize = mergedTableHeaders.size();
    int mergedTableColumnSize = mergedTableHeaderSize - mergedTableMeasures.size();
    final int[] mergedTableColumnIndices = IntStream.range(0, mergedTableColumnSize).toArray();
    final int[] mergedTableMeasureIndices = IntStream.range(mergedTableColumnSize, mergedTableHeaderSize).toArray();
    final List<List<Object>> mergedValues = mergeValues(mergedTableHeaders, mergedTableColumnIndices, leftTable,
            leftHeaders, rightTable, rightHeaders);

    return new ColumnarTable(
            mergedTableHeaders,
            mergedTableMeasures,
            mergedTableMeasureIndices,
            mergedTableColumnIndices,
            mergedValues);

  }

  private static int getCommonColumnsCount(List<Field> leftHeaders, List<Field> rightHeaders) {
    return leftHeaders.stream().filter(rightHeaders::contains).mapToInt(e -> 1).sum();
  }

  private static List<Field> mergeHeaders(List<Field> leftHeaders, int[] leftColumnIndices, int[] leftMeasureIndices,
          List<Field> rightHeaders, int[] rightColumnIndices, int[] rightMeasureIndices) {
    List<Field> mergedTableHeaders = new ArrayList<>();
    for (int index : leftColumnIndices) {
      mergedTableHeaders.add(leftHeaders.get(index));
    }
    for (int index : rightColumnIndices) {
      Field rightTableHeader = rightHeaders.get(index);
      if (!mergedTableHeaders.contains(rightTableHeader)) {
        mergedTableHeaders.add(rightTableHeader);
      }
    }
    for (int index : leftMeasureIndices) {
      mergedTableHeaders.add(leftHeaders.get(index));
    }
    for (int index : rightMeasureIndices) {
      Field rightTableHeader = rightHeaders.get(index);
      if (mergedTableHeaders.contains(rightTableHeader)) {
        throw new UnsupportedOperationException(String.format(
                "The two tables both contain the measure %s while they must not share any measure to be merged.",
                rightTableHeader.name()));
      } else {
        mergedTableHeaders.add(rightTableHeader);
      }
    }
    return mergedTableHeaders;
  }

  private static List<Measure> mergeMeasures(List<Measure> leftMeasures, List<Measure> rightMeasures) {
    List<Measure> mergedTableMeasures = new ArrayList<>(leftMeasures);
    for (Measure rightTableMeasure : rightMeasures) {
      if (mergedTableMeasures.contains(rightTableMeasure)) {
        throw new UnsupportedOperationException(String.format(
                "The two tables both contain the measure %s while they must not share any measure to be merged.",
                rightTableMeasure));
      } else {
        mergedTableMeasures.add(rightTableMeasure);
      }
    }
    return mergedTableMeasures;
  }

  private static List<List<Object>> mergeValues(
          List<Field> mergedTableHeaders,
          int[] mergedTableColumnIndices,
          Table leftTable,
          List<Field> leftHeaders,
          Table rightTable,
          List<Field> rightHeaders) {
    // values initialization
    final List<List<Object>> mergedValues = new ArrayList<>();
    for (int i = 0; i < mergedTableHeaders.size(); i++) {
      mergedValues.add(new ArrayList<>());
    }

    int leftRowIndex = 0;
    int rightRowIndex = 0;
    List<Object> leftRow = leftTable.getFactRow(leftRowIndex);
    List<Object> rightRow = rightTable.getFactRow(rightRowIndex);
    int commonColumns = getCommonColumnsCount(leftHeaders, rightHeaders);
    while (leftRow != null || rightRow != null) {
      // Handle null cases
      if (leftRow == null) {
        addRowFromTableToValues(mergedValues, mergedTableHeaders, mergedTableColumnIndices, rightTable, rightRowIndex);
        rightRow = rightTable.getFactRow(++rightRowIndex);
        continue;
      }
      if (rightRow == null) {
        addRowFromTableToValues(mergedValues, mergedTableHeaders, mergedTableColumnIndices, leftTable, leftRowIndex);
        leftRow = leftTable.getFactRow(++leftRowIndex);
        continue;
      }

      if (!leftRow.subList(0, commonColumns).equals(rightRow.subList(0, commonColumns))) {
        for (int commonIndex = 0; commonIndex < commonColumns; commonIndex++) {
          // Is it enough to compare string values ?
          int columnValueComparison = leftRow.get(commonIndex).toString()
                  .compareTo(rightRow.get(commonIndex).toString());
          if (columnValueComparison != 0) {
            if (columnValueComparison < 0) {
              addRowFromTableToValues(mergedValues, mergedTableHeaders, mergedTableColumnIndices, leftTable,
                      leftRowIndex);
              leftRow = leftTable.getFactRow(++leftRowIndex);
            } else {
              addRowFromTableToValues(mergedValues, mergedTableHeaders, mergedTableColumnIndices, rightTable,
                      rightRowIndex);
              rightRow = rightTable.getFactRow(++rightRowIndex);
            }
            break;
          }
        }
        continue;
      }
      if (((commonColumns != leftRow.size()) && leftRow.subList(commonColumns, leftRow.size()).stream()
              .anyMatch(x -> x != SQLTranslator.TOTAL_CELL)) ||
              ((commonColumns != rightRow.size()) && rightRow.subList(commonColumns, rightRow.size()).stream()
                      .anyMatch(x -> x != SQLTranslator.TOTAL_CELL))) {
        if (commonColumns == leftRow.size()) {
          addRowFromTableToValues(mergedValues, mergedTableHeaders, mergedTableColumnIndices, leftTable, leftRowIndex);
          leftRow = leftTable.getFactRow(++leftRowIndex);
        } else {
          addRowFromTableToValues(mergedValues, mergedTableHeaders, mergedTableColumnIndices, rightTable,
                  rightRowIndex);
          rightRow = rightTable.getFactRow(++rightRowIndex);
        }
        continue;
      }

      addMergedRowToValues(mergedValues, mergedTableHeaders, leftTable, leftRowIndex, rightTable, rightRowIndex);
      leftRow = leftTable.getFactRow(++leftRowIndex);
      rightRow = rightTable.getFactRow(++rightRowIndex);
    }
    return mergedValues;
  }

  private static void addRowFromTableToValues(List<List<Object>> values, List<Field> headers, int[] columnIndices,
          Table table,
          int rowToAddIndex) {
    for (int index = 0; index < headers.size(); index++) {
      Object element = null;
      for (int columnIndex : columnIndices) {
        if (index == columnIndex) {
          element = SQLTranslator.TOTAL_CELL;
          break;
        }
      }
      Field header = headers.get(index);
      if (table.headers().contains(header)) {
        element = table.getColumnValues(header.name()).get(rowToAddIndex);
      }
      values.get(index).add(element);
    }
  }

  private static void addMergedRowToValues(List<List<Object>> values, List<Field> headers, Table leftTable,
          int leftRowIndex, Table rightTable, int rightRowIndex) {
    for (int index = 0; index < headers.size(); index++) {
      Object element;
      Field header = headers.get(index);
      if (leftTable.headers().contains(header)) {
        element = leftTable.getColumnValues(header.name()).get(leftRowIndex);
      } else {
        element = rightTable.getColumnValues(header.name()).get(rightRowIndex);
      }
      values.get(index).add(element);
    }
  }

}
