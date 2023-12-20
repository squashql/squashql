package io.squashql.query.database;

import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.compiled.CompiledTable;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.type.TypedField;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class DatabaseQuery {

  public final VirtualTableDto virtualTableDto; // CTE
  public final CompiledTable table;
  public final DatabaseQuery subQuery;
  public final Set<TypedField> select;
  public final CompiledCriteria whereCriteriaDto;
  public final CompiledCriteria havingCriteriaDto;
  public final List<TypedField> rollup;
  public final List<List<TypedField>> groupingSets;
  public final int limit;
  public List<CompiledMeasure> measures = new ArrayList<>();

  public DatabaseQuery withMeasure(CompiledMeasure m) {
    this.measures.add(m);
    return this;
  }

}
