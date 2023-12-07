package io.squashql.query.database;

import io.squashql.query.Measure;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.TableDto;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.type.TypedField;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class DatabaseQuery {

  public VirtualTableDto virtualTableDto; // CTE
  public TableDto table;
  public DatabaseQuery subQuery;
  public List<TypedField> select = new ArrayList<>();
  public CriteriaDto whereCriteriaDto = null;
  public CriteriaDto havingCriteriaDto = null;
  public List<Measure> measures = new ArrayList<>();
  public List<TypedField> rollup = new ArrayList<>();
  public List<List<TypedField>> groupingSets = new ArrayList<>();
  public int limit = -1;

  public DatabaseQuery withSelect(TypedField field) {
    this.select.add(field);
    return this;
  }

  public DatabaseQuery rollup(List<TypedField> rollup) {
    this.rollup = rollup;
    return this;
  }

  public DatabaseQuery groupingSets(List<List<TypedField>> groupingSets) {
    this.groupingSets = groupingSets;
    return this;
  }

  public DatabaseQuery withMeasure(Measure m) {
    this.measures.add(m);
    return this;
  }

  public DatabaseQuery table(TableDto table) {
    this.table = table;
    return this;
  }

  public DatabaseQuery table(String tableName) {
    this.table = new TableDto(tableName);
    return this;
  }

  public DatabaseQuery subQuery(DatabaseQuery subQuery) {
    this.subQuery = subQuery;
    return this;
  }

  public DatabaseQuery whereCriteria(CriteriaDto criteriaDto) {
    this.whereCriteriaDto = criteriaDto;
    return this;
  }

  public DatabaseQuery havingCriteria(CriteriaDto criteriaDto) {
    this.havingCriteriaDto = criteriaDto;
    return this;
  }

  public DatabaseQuery limit(int limit) {
    this.limit = limit;
    return this;
  }

  public DatabaseQuery virtualTable(VirtualTableDto virtualTableDto) {
    this.virtualTableDto = virtualTableDto;
    return this;
  }
}
