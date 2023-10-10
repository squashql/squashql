package io.squashql.query.compiled;

import io.squashql.query.dto.VirtualTableDto;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class DatabaseQuery2 {

  public VirtualTableDto virtualTableDto; // CTE
  public CompiledTable table;
  public DatabaseQuery2 subQuery;
  public List<TypedField> select = new ArrayList<>();
  public CompiledCriteria whereCriteriaDto = null;
  public CompiledCriteria havingCriteriaDto = null;
  public List<CompiledMeasure> measures = new ArrayList<>();
  public List<TypedField> rollup = new ArrayList<>();
  public List<List<TypedField>> groupingSets = new ArrayList<>();
  public int limit = -1;

  public DatabaseQuery2 withSelect(TypedField field) {
    this.select.add(field);
    return this;
  }

  public DatabaseQuery2 rollup(List<TypedField> rollup) {
    this.rollup = rollup;
    return this;
  }

  public DatabaseQuery2 groupingSets(List<List<TypedField>> groupingSets) {
    this.groupingSets = groupingSets;
    return this;
  }

//  public DatabaseQuery2 aggregatedMeasure(String alias, String field, String agg) {
//    withMeasure(new AggregatedMeasure(alias, field, agg));
//    return this;
//  }
//
//  public DatabaseQuery2 expressionMeasure(String alias, String expression) {
//    withMeasure(new ExpressionMeasure(alias, expression));
//    return this;
//  }

  public DatabaseQuery2 withMeasure(CompiledMeasure m) {
    this.measures.add(m);
    return this;
  }

  public DatabaseQuery2 table(CompiledTable table) {
    this.table = table;
    return this;
  }

//  public DatabaseQuery2 table(String tableName) {
//    this.table = new TableDto(tableName);
//    return this;
//  }

  public DatabaseQuery2 subQuery(DatabaseQuery2 subQuery) {
    this.subQuery = subQuery;
    return this;
  }

  public DatabaseQuery2 whereCriteria(CompiledCriteria criteriaDto) {
    this.whereCriteriaDto = criteriaDto;
    return this;
  }

  public DatabaseQuery2 havingCriteria(CompiledCriteria criteriaDto) {
    this.havingCriteriaDto = criteriaDto;
    return this;
  }

  public DatabaseQuery2 limit(int limit) {
    this.limit = limit;
    return this;
  }

  public DatabaseQuery2 virtualTable(VirtualTableDto virtualTableDto) {
    this.virtualTableDto = virtualTableDto;
    return this;
  }
}
