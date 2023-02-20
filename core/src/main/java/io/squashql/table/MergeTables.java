package io.squashql.table;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.squashql.query.*;
import io.squashql.query.database.SQLTranslator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class MergeTables {

  private MergeTables() {
  }

  public static Table mergeTables(List<Table> tables) {
    Table result = null;
    for (Table table : tables) {
      result = result == null ? table : mergeTables(result, table);
    }
    return result;
  }

  /**
   * Merge two tables into only one resulting table. Hypothesis: the input tables must not share any measure.
   */
  public static Table mergeTables(Table leftTable, Table rightTable) {
    if (leftTable == null) {
      return rightTable;
    }
    if (rightTable == null) {
      return leftTable;
    }

    final List<Header> mergedTableHeaders = mergeHeaders(leftTable, rightTable);
    final Set<Measure> mergedTableMeasures = mergeMeasures(leftTable.measures(), rightTable.measures());
    final List<List<Object>> mergedValues = mergeValues(mergedTableHeaders, leftTable, rightTable);

    return new ColumnarTable(
            mergedTableHeaders,
            mergedTableMeasures,
            mergedValues);
  }

  private static int getCommonColumnsCount(List<Header> leftHeaders, List<Header> rightHeaders) {
    return (int) leftHeaders.stream().filter(rightHeaders::contains).count();
  }

  private static List<Header> mergeHeaders(Table leftTable, Table rightTable) {
    List<Header> mergedColumns = new ArrayList<>();
    List<Header> mergedMeasures = new ArrayList<>();
    leftTable.headers().forEach(leftHeader -> {
      if (leftHeader.isMeasure()) {
        mergedMeasures.add(leftHeader);
      } else {
        mergedColumns.add(leftHeader);
      }
    });
    rightTable.headers().forEach(rightHeader -> {
      if (rightHeader.isMeasure()) {
        if (mergedMeasures.contains(rightHeader)) {
          throw new UnsupportedOperationException(String.format(
                  "The two tables both contain the measure %s while they must not share any measure to be merged.",
                  rightHeader.field().name()));
        } else {
          mergedMeasures.add(rightHeader);
        }
      } else {
        if (!mergedColumns.contains(rightHeader)) {
          mergedColumns.add(rightHeader);
        }
      }
    });
    List<Header> mergedTableHeaders = new ArrayList<>(mergedColumns);
    mergedTableHeaders.addAll(mergedMeasures);
    return mergedTableHeaders;
  }

  private static Set<Measure> mergeMeasures(Set<Measure> leftMeasures, Set<Measure> rightMeasures) {
    return Sets.newHashSet(Iterables.concat(leftMeasures, rightMeasures));
  }

  /**
   * In order to merge the tables, they must have their common columns at the beginning, in the same order, and sorted.
   * They also must have all their columns first, then their measures.
   *
   * This method takes as input the tables to merge and return them reshaped to respect this.
   */
  private static Table[] prepareTablesForMerge(Table leftTable, Table rightTable) {
    // Order columns to have all commons first
    List<Header> leftHeaders = leftTable.headers();
    List<Header> rightHeaders = rightTable.headers();
    List<String> commonColumns = leftHeaders.stream()
            .filter(header -> !header.isMeasure() && rightHeaders.contains(header)).map(header -> header.field().name())
            .toList();

    List<String> leftColumns = leftHeaders.stream().filter(header -> !header.isMeasure())
            .map(header -> header.field().name()).toList();
    List<String> leftColumnsOrdered = new ArrayList<>(commonColumns.stream().filter(leftColumns::contains).toList());
    leftColumns.forEach(columnName -> {
      if (!leftColumnsOrdered.contains(columnName)) {
        leftColumnsOrdered.add(columnName);
      }
    });

    List<String> rightColumns = rightHeaders.stream().filter(header -> !header.isMeasure())
            .map(header -> header.field().name()).toList();
    List<String> rightColumnsOrdered = new ArrayList<>(commonColumns.stream().filter(rightColumns::contains).toList());
    rightColumns.forEach(columnName -> {
      if (!rightColumnsOrdered.contains(columnName)) {
        rightColumnsOrdered.add(columnName);
      }
    });

    ColumnarTable orderedLeftTable = TableUtils.selectAndOrderColumns((ColumnarTable) leftTable, leftColumnsOrdered,
            leftTable.measures().stream().toList());
    ColumnarTable orderedRightTable = TableUtils.selectAndOrderColumns((ColumnarTable) rightTable, rightColumnsOrdered,
            rightTable.measures().stream().toList());

    // Sort rows on all columns with default comparator (natural order)
    orderedLeftTable = (ColumnarTable) TableUtils.orderRows(orderedLeftTable, Collections.emptyMap(),
            Collections.emptySet());
    orderedRightTable = (ColumnarTable) TableUtils.orderRows(orderedRightTable, Collections.emptyMap(),
            Collections.emptySet());

    return new Table[] {orderedLeftTable, orderedRightTable};
  }

  private static List<List<Object>> mergeValues(
          List<Header> mergedTableHeaders,
          Table leftTable,
          Table rightTable) {
    // Order headers and sort rows
    Table[] preparedTablesForMerge = prepareTablesForMerge(leftTable, rightTable);
    leftTable = preparedTablesForMerge[0];
    rightTable = preparedTablesForMerge[1];
    // values initialization
    final List<List<Object>> mergedValues = new ArrayList<>();
    for (int i = 0; i < mergedTableHeaders.size(); i++) {
      mergedValues.add(new ArrayList<>());
    }

    List<Header> leftHeaders = leftTable.headers();
    List<Header> rightHeaders = rightTable.headers();
    int commonColumnsCount = getCommonColumnsCount(leftHeaders, rightHeaders);

    int leftRowIndex = 0;
    int rightRowIndex = 0;
    List<Object> leftRow = leftTable.getFactRow(leftRowIndex);
    List<Object> rightRow = rightTable.getFactRow(rightRowIndex);
    while (leftRow != null || rightRow != null) {
      MergeRowsStrategy mergeRowsStrategy = getMergeRowsStrategy(leftRow, rightRow, commonColumnsCount);
      switch (mergeRowsStrategy) {
        case KEEP_LEFT -> {
          addRowFromTableToValues(mergedValues, mergedTableHeaders, leftTable, leftRowIndex);
          leftRow = leftTable.getFactRow(++leftRowIndex);
        }
        case KEEP_RIGHT -> {
          addRowFromTableToValues(mergedValues, mergedTableHeaders, rightTable, rightRowIndex);
          rightRow = rightTable.getFactRow(++rightRowIndex);
        }
        case MERGE -> {
          addMergedRowToValues(mergedValues, mergedTableHeaders, leftTable, leftRowIndex, rightTable, rightRowIndex);
          leftRow = leftTable.getFactRow(++leftRowIndex);
          rightRow = rightTable.getFactRow(++rightRowIndex);
        }
      }
    }

    return mergedValues;
  }

  enum MergeRowsStrategy {
    KEEP_LEFT,
    KEEP_RIGHT,
    MERGE
  }

  private static MergeRowsStrategy getMergeRowsStrategy(List<Object> leftRow, List<Object> rightRow,
          int commonColumnsCount) {
    // Handle null row cases
    if (leftRow == null) {
      return MergeRowsStrategy.KEEP_RIGHT;
    }
    if (rightRow == null) {
      return MergeRowsStrategy.KEEP_LEFT;
    }

    // Check if the two rows have the same values on the common columns
    for (int commonIndex = 0; commonIndex < commonColumnsCount; commonIndex++) {
      Object leftRowValue = leftRow.get(commonIndex);
      Object rightRowValue = rightRow.get(commonIndex);
      if (!leftRowValue.equals(rightRowValue)) { // The two rows don't have the same values on all the common columns
        // Keep one row ensuring that the merged table will be well sorted
        // Is it enough to compare string values ?
        return leftRowValue.toString().compareTo(rightRowValue.toString()) < 0 ? MergeRowsStrategy.KEEP_LEFT
                : MergeRowsStrategy.KEEP_RIGHT;
      }
    }
    // The two rows have the same values on all the common columns
    // Check if there are only total values on the non-common columns
    if (((leftRow.size() == commonColumnsCount) || leftRow.subList(commonColumnsCount, leftRow.size()).stream()
            .noneMatch(x -> x != SQLTranslator.TOTAL_CELL))
            && ((rightRow.size() == commonColumnsCount) || rightRow.subList(commonColumnsCount, rightRow.size())
            .stream().noneMatch(x -> x != SQLTranslator.TOTAL_CELL))) {
      // We can merge the rows
      return MergeRowsStrategy.MERGE;
    }
    // Keep one row ensuring that the merged table will be well sorted
    return leftRow.size() == commonColumnsCount ? MergeRowsStrategy.KEEP_LEFT : MergeRowsStrategy.KEEP_RIGHT;
  }

  private static void addRowFromTableToValues(List<List<Object>> values, List<Header> headers, Table table,
          int rowToAddIndex) {
    for (int index = 0; index < headers.size(); index++) {
      Header header = headers.get(index);
      Object element = header.isMeasure() ? null : SQLTranslator.TOTAL_CELL;
      if (table.headers().contains(header)) {
        element = table.getColumnValues(header.field().name()).get(rowToAddIndex);
      }
      values.get(index).add(element);
    }
  }

  private static void addMergedRowToValues(List<List<Object>> values, List<Header> headers, Table leftTable,
          int leftRowIndex, Table rightTable, int rightRowIndex) {
    for (int index = 0; index < headers.size(); index++) {
      Object element;
      Header header = headers.get(index);
      if (leftTable.headers().contains(header)) {
        element = leftTable.getColumnValues(header.field().name()).get(leftRowIndex);
      } else {
        element = rightTable.getColumnValues(header.field().name()).get(rightRowIndex);
      }
      values.get(index).add(element);
    }
  }
}
