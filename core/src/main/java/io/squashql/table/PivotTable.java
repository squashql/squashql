package io.squashql.table;

import io.squashql.query.Field;
import java.util.List;

public class PivotTable {

  //todo-mde how should we serialize this
  public final Table table;
  public final List<List<Object>> pivotTableCells;
  public final List<Field> rows;
  public final List<Field> columns;
  public final List<String> values;

  public PivotTable(Table table, List<Field> rows, List<Field> columns, List<String> values) {
    this.table = table;
    this.rows = rows;
    this.columns = columns;
    this.values = values;
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
