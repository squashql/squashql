package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class JoinDto {

  public TableDto table;
  public JoinType type; // inner|left
  public CriteriaDto joinCriteria;

  public JoinDto(TableDto table, JoinType type, CriteriaDto joinCriteria) {
    this.table = table;
    this.type = type;
    this.joinCriteria = joinCriteria;
  }
}
