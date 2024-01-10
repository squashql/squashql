package io.squashql.query.database;

import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.compiled.CompiledTable;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.type.TypedField;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@EqualsAndHashCode
@RequiredArgsConstructor
public class DatabaseQuery {

  public final VirtualTableDto virtualTableDto; // CTE
  public final CompiledTable table;
  public final DatabaseQuery subQuery;
  public final Set<TypedField> select;
  public final CompiledCriteria whereCriteria;
  public final CompiledCriteria havingCriteria;
  public final List<TypedField> rollup;
  public final List<List<TypedField>> groupingSets;
  public final int limit;
  public List<CompiledMeasure> measures = new ArrayList<>();

  public DatabaseQuery withMeasure(CompiledMeasure m) {
    this.measures.add(m);
    return this;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DatabaseQuery{");
    sb.append("table=").append(table);
    if (subQuery != null) {
      sb.append(", subQuery=").append(subQuery);
    }
    if (select != null && !select.isEmpty()) {
      sb.append(", columns=").append(select);
    }
    if (whereCriteria != null) {
      sb.append(", whereCriteria=").append(whereCriteria);
    }
    if (havingCriteria != null) {
      sb.append(", havingCriteria=").append(havingCriteria);
    }
    if (rollup != null && !rollup.isEmpty()) {
      sb.append(", rollupColumns=").append(rollup);
    }
    if (groupingSets != null && !groupingSets.isEmpty()) {
      sb.append(", groupingSets=").append(groupingSets);
    }
    if (virtualTableDto != null) {
      sb.append(", virtualTable=").append(virtualTableDto);
    }
    if (limit > 0) {
      sb.append(", limit=").append(limit);
    }
    sb.append(", measures=").append(measures);
    sb.append('}');
    return sb.toString();
  }
}
