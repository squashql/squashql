package io.squashql.query.join;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.squashql.query.Field;
import io.squashql.query.dto.*;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class QueryJoin {

  public List<QueryDto> queries = new ArrayList<>();
  public TableDto tableDto;
  @JsonIgnore
  public int current = 0;
  public Map<Field, OrderDto> orders;
  public int limit = -1;

  private QueryJoin(QueryDto q1) {
    this.queries.add(q1);
    this.tableDto = new TableDto(String.format("__cte%d__", this.current++));
  }

  public static QueryJoin from(QueryDto q) {
    return new QueryJoin(q);
  }

  public QueryJoin join(QueryDto q, JoinType joinType, CriteriaDto criteriaDto) {
    this.queries.add(q);
    this.tableDto.join(new TableDto(String.format("__cte%d__", this.current++)), joinType, criteriaDto);
    return this;
  }

  public QueryJoin join(QueryDto q, JoinType joinType) {
    return join(q, joinType, null);
  }

  public QueryJoin order(Map<Field, OrderDto> orders) {
    this.orders = orders;
    return this;
  }

  public QueryJoin limit(int limit) {
    this.limit = limit;
    return this;
  }
}
