package io.squashql.query.database;

import java.util.HashSet;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ExpressionMeasure;
import io.squashql.query.Measure;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.TableDto;

import java.util.ArrayList;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class DatabaseQuery {

  public TableDto table;
  public DatabaseQuery subQuery;
  public List<String> select = new ArrayList<>();
  public CriteriaDto criteriaDto = null;
  public Set<Measure> measures = new HashSet<>();
  public List<String> rollup = new ArrayList<>();

  public DatabaseQuery withSelect(String field) {
    this.select.add(field);
    return this;
  }

  public DatabaseQuery withRollup(String field) {
    this.rollup.add(field);
    return this;
  }

  public DatabaseQuery aggregatedMeasure(String alias, String field, String agg) {
    withMeasure(new AggregatedMeasure(alias, field, agg));
    return this;
  }

  public DatabaseQuery expressionMeasure(String alias, String expression) {
    withMeasure(new ExpressionMeasure(alias, expression));
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

  public DatabaseQuery criteria(CriteriaDto criteriaDto) {
    this.criteriaDto = criteriaDto;
    return this;
  }
}
