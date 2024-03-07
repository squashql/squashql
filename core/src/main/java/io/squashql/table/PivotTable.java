package io.squashql.table;

import java.util.List;

public class PivotTable {

  public final Table table;
  public final List<List<Object>> pivotTableCells;
  public final List<String> rows;
  public final List<String> columns;
  public final List<String> values;
  public final List<String> hideTotals;

  public PivotTable(Table table, List<String> rows, List<String> columns, List<String> values, List<String> hideTotals) {
    this.table = table;
    this.rows = rows;
    this.columns = columns;
    this.values = values;
    this.hideTotals = hideTotals;
    this.pivotTableCells = PivotTableUtils.pivot(this);
  }

  public void show() {
    System.out.println(this);
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.pivotTableCells, String::valueOf, line -> line.equals(this.columns.size()));
  }
}
