package io.squashql.query.database;

import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.compiled.CompiledOrderBy;
import io.squashql.query.compiled.CompiledTable;
import io.squashql.query.compiled.CteRecordTable;
import io.squashql.type.TypedField;

import java.util.List;
import java.util.Set;

public record QueryScope(CompiledTable table,
                         List<TypedField> columns,
                         CompiledCriteria whereCriteria,
                         CompiledCriteria havingCriteria,
                         List<TypedField> rollup,
                         Set<Set<TypedField>> groupingSets,
                         List<CteRecordTable> cteRecordTables,
                         List<CompiledOrderBy> orderBy,
                         int limit) {

  public QueryScope copyWithNewLimit(int newLimit) {
    return new QueryScope(this.table, this.columns, this.whereCriteria, this.havingCriteria, this.rollup, this.groupingSets, this.cteRecordTables, this.orderBy, newLimit);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("QueryScope{");
    sb.append("table=").append(this.table);
    if (this.columns != null && !this.columns.isEmpty()) {
      sb.append(", columns=").append(this.columns);
    }
    if (this.whereCriteria != null) {
      sb.append(", whereCriteria=").append(this.whereCriteria);
    }
    if (this.havingCriteria != null) {
      sb.append(", havingCriteria=").append(this.havingCriteria);
    }
    if (this.rollup != null && !this.rollup.isEmpty()) {
      sb.append(", rollup=").append(this.rollup);
    }
    if (this.groupingSets != null && !this.groupingSets.isEmpty()) {
      sb.append(", groupingSets=").append(this.groupingSets);
    }
    if (this.cteRecordTables != null && !this.cteRecordTables.isEmpty()) {
      sb.append(", cteRecordTables=").append(this.cteRecordTables);
    }
    if (this.orderBy != null && !this.orderBy.isEmpty()) {
      sb.append(", orderBy=").append(this.orderBy);
    }
    if (this.limit > 0) {
      sb.append(", limit=").append(this.limit);
    }
    sb.append('}');
    return sb.toString();
  }
}
