package io.squashql.table;

import java.util.List;

public class PivotTable {

  public final List<List<Object>> pivotTableRows;
  public final Table table;
  public final List<String> rows;
  public final List<String> columns;
  public final List<String> values;

  public PivotTable(Table table, List<String> rows, List<String> columns, List<String> values) {
    this.table = table;
    this.rows = rows;
    this.columns = columns;
    this.values = values;
    this.pivotTableRows = PivotTableUtils.pivot(this);
  }

  public void show() {
    System.out.println(this);
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.pivotTableRows, String::valueOf, line -> line.equals(this.columns.size()));
  }
}
