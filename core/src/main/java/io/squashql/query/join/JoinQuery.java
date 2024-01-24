package io.squashql.query.join;

import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;

import java.util.ArrayList;
import java.util.List;

public class JoinQuery {

  List<QueryDto> queries = new ArrayList<>();
  TableDto tableDto;
  int current = 0;

  private JoinQuery(QueryDto q1) {
    this.queries.add(q1);
    this.tableDto = new TableDto(String.format("__cte%d__", this.current++));
  }

  public static JoinQuery from(QueryDto q) {
    return new JoinQuery(q);
  }

  public JoinQuery join(QueryDto q, JoinType joinType, CriteriaDto criteriaDto) {
    this.queries.add(q);
    this.tableDto.join(new TableDto(String.format("__cte%d__", this.current++)), joinType, criteriaDto);
    return this;
  }

  public JoinQuery join(QueryDto q, JoinType joinType) {
    this.queries.add(q);
    this.tableDto.join(new TableDto(String.format("__cte%d__", this.current++)), joinType, null);
    return this;
  }
}
