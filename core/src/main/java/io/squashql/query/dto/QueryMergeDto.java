package io.squashql.query.dto;

import io.squashql.query.parameter.Parameter;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class QueryMergeDto {

  public List<QueryDto> queries = new ArrayList<>();
  public List<JoinType> joinTypes = new ArrayList<>();

  private QueryMergeDto(QueryDto q1) {
    this.queries.add(q1);
  }

  public static QueryMergeDto from(QueryDto q) {
    return new QueryMergeDto(q);
  }

  public QueryMergeDto join(QueryDto q, JoinType joinType) {
    this.queries.add(q);
    this.joinTypes.add(joinType);
    return this;
  }

  public QueryMergeDto withParameter(Parameter parameter) {
    this.queries.forEach(q -> q.withParameter(parameter.key(), parameter));
    return this;
  }
}
