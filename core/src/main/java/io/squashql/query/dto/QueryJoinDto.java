package io.squashql.query.dto;

import io.squashql.query.Field;
import io.squashql.query.join.QueryJoin;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class QueryJoinDto {

  public QueryJoin queryJoin;
  public Map<Field, OrderDto> orders;
  public int limit = -1;
}
