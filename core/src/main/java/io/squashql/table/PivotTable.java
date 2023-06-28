package io.squashql.table;

import java.util.List;

public class PivotTable {

  final List<List<Object>> pivotTableRows;
  final Table table;
  final List<String> rows;
  final List<String> columns;
  final List<String> values;

  public PivotTable(Table table, List<String> rows, List<String> columns, List<String> values) {
    this.table = table;
    this.rows = rows;
    this.columns = columns;
    this.values = values;
    this.pivotTableRows = PivotTableUtils.pivot(table, rows, columns, values);
  }

  public List<List<Object>> getPivotTableRows() {
    return this.pivotTableRows;
  }

  public void show() {
    System.out.println(this);
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.pivotTableRows, String::valueOf, line -> line.equals(this.columns.size()));
  }
}
