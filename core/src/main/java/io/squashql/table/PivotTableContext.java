package io.squashql.table;

import io.squashql.query.ColumnSet;
import io.squashql.query.field.Field;
import io.squashql.query.dto.GroupColumnSetDto;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.squashql.query.ColumnSetKey.GROUP;

public class PivotTableContext {
  public final List<Field> rows;
  public final List<Field> cleansedRows;
  public final List<Field> columns;
  public final List<Field> cleansedColumns;
  public final List<Field> hiddenTotals;

  public PivotTableContext(PivotTableQueryDto pivotTableQueryDto) {
    this.hiddenTotals = pivotTableQueryDto.hiddenTotals == null ? Collections.emptyList() : pivotTableQueryDto.hiddenTotals;
    this.rows = pivotTableQueryDto.rows;
    this.cleansedRows = cleanse(pivotTableQueryDto.query, pivotTableQueryDto.rows);
    this.columns = pivotTableQueryDto.columns;
    this.cleansedColumns = cleanse(pivotTableQueryDto.query, pivotTableQueryDto.columns);
  }

  public static List<Field> cleanse(QueryDto query, List<Field> fields) {
    // ColumnSet is a special type of column that does not exist in the database but only in SquashQL. Totals can't be
    // computed. This is why it is removed from the axes.
    ColumnSet columnSet = query.columnSets.get(GROUP);
    if (columnSet != null) {
      Field newField = ((GroupColumnSetDto) columnSet).newField;
      if (fields.contains(newField)) {
        fields = new ArrayList<>(fields);
        fields.remove(newField);
      }
    }
    return fields;
  }
}
