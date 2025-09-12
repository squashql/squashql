package io.squashql.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.squashql.query.Field;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class QueryJoinDto {

  public List<QueryDto> queries = new ArrayList<>();
  public TableDto table;
  @JsonIgnore
  public int current = 0;
  public Map<Field, OrderDto> orders;
  public int limit = -1;
  public CriteriaDto where;
  public Boolean minify;

  private QueryJoinDto(QueryDto q1) {
    this.queries.add(q1);
    this.table = new TableDto(String.format("__cte%d__", this.current++));
  }

  public static QueryJoinDto from(QueryDto q) {
    return new QueryJoinDto(q);
  }

  public QueryJoinDto join(QueryDto q, JoinType joinType, CriteriaDto criteriaDto) {
    this.queries.add(q);
    this.table.join(new TableDto(String.format("__cte%d__", this.current++)), joinType, criteriaDto);
    return this;
  }

  public QueryJoinDto join(QueryDto q, JoinType joinType) {
    return join(q, joinType, null);
  }

  public QueryJoinDto orderBy(Map<Field, OrderDto> orders) {
    this.orders = orders;
    return this;
  }

  public QueryJoinDto limit(int limit) {
    this.limit = limit;
    return this;
  }

  public QueryJoinDto where(CriteriaDto where) {
    this.where = where;
    return this;
  }
}
