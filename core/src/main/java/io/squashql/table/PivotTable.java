package io.squashql.table;

import java.util.List;

public class PivotTable implements Renderable {

  public final Table table;
  public final List<List<Object>> pivotTableCells;
  public final List<String> rows;
  public final List<String> columns;
  public final List<String> values;
  public final List<String> hiddenTotals;

  public PivotTable(Table table, List<String> rows, List<String> columns, List<String> values, List<String> hiddenTotals) {
    this.table = table;
    this.rows = rows;
    this.columns = columns;
    this.values = values;
    this.hiddenTotals = hiddenTotals;
    this.pivotTableCells = PivotTableUtils.pivot(this);
  }

  public void show() {
    System.out.println(this);
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.pivotTableCells, String::valueOf, line -> line.equals(this.columns.size()));
  }

  public String toCSV() {
    return TableUtils.toCSV(null, this.pivotTableCells);
  }
}
