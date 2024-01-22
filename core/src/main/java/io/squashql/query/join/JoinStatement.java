package io.squashql.query.join;

import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;

import java.util.ArrayList;
import java.util.List;

public class JoinStatement {

    List<QueryDto> queries = new ArrayList<>();
    TableDto tableDto;
    int current = 0;

    private JoinStatement(QueryDto q1) {
      this.queries.add(q1);
      this.tableDto = new TableDto(String.format("__cte%d__", this.current++));
      this.tableDto.isCte = true;
    }

    public static JoinStatement start(QueryDto q) {
      return new JoinStatement(q);
    }

    public JoinStatement join(QueryDto q, JoinType joinType, CriteriaDto criteriaDto) {
      this.queries.add(q);
      TableDto other = new TableDto(String.format("__cte%d__", this.current++));
      other.isCte = true;
      this.tableDto.join(other, joinType, criteriaDto);
      return this;
    }
  }
