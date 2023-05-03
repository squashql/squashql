package io.squashql.query.dto;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class QueryMergeDto {

  public QueryDto first;
  public QueryDto second;
  public JoinType joinType;
}
