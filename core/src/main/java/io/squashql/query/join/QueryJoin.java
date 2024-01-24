package io.squashql.query.join;

import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor // For Jackson
public class QueryJoin {

  public List<QueryDto> queries = new ArrayList<>();
  public TableDto tableDto;
  public int current = 0;

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
    this.queries.add(q);
    this.tableDto.join(new TableDto(String.format("__cte%d__", this.current++)), joinType, null);
    return this;
  }
}
