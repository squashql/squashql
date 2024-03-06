package io.squashql.query.database;

import io.squashql.query.compiled.*;
import io.squashql.type.TypedField;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@EqualsAndHashCode
@RequiredArgsConstructor
public class DatabaseQuery {

  public final List<CteRecordTable> cteRecordTables; // CTEs
  public final CompiledTable table;
  public final Set<TypedField> select;
  public final CompiledCriteria whereCriteria;
  public final CompiledCriteria havingCriteria;
  public final List<TypedField> rollup;
  public final Set<Set<TypedField>> groupingSets;
  public final int limit;
  public List<CompiledMeasure> measures = new ArrayList<>();

  public DatabaseQuery withMeasure(CompiledMeasure m) {
    this.measures.add(m);
    return this;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DatabaseQuery{");
    sb.append("table=").append(this.table);
    if (this.select != null && !this.select.isEmpty()) {
      sb.append(", columns=").append(this.select);
    }
    if (this.whereCriteria != null) {
      sb.append(", whereCriteria=").append(this.whereCriteria);
    }
    if (this.havingCriteria != null) {
      sb.append(", havingCriteria=").append(this.havingCriteria);
    }
    if (this.rollup != null && !this.rollup.isEmpty()) {
      sb.append(", rollupColumns=").append(this.rollup);
    }
    if (this.groupingSets != null && !this.groupingSets.isEmpty()) {
      sb.append(", groupingSets=").append(this.groupingSets);
    }
    if (this.cteRecordTables != null && !this.cteRecordTables.isEmpty()) {
      sb.append(", cteRecordTables=").append(this.cteRecordTables);
    }
    if (this.limit > 0) {
      sb.append(", limit=").append(this.limit);
    }
    sb.append(", measures=").append(this.measures);
    sb.append('}');
    return sb.toString();
  }
}
