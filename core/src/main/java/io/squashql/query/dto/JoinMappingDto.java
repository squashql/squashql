package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Mapping to perform an equi-join.
 */
@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class JoinMappingDto {

  private static final ConditionType defaultConditionType = ConditionType.EQ;

  public String from;
  public String to;
  public String fromTable;
  public String toTable;
  public ConditionType conditionType = defaultConditionType;

  public JoinMappingDto(String fromTable, String from, String toTable, String to) {
    this(fromTable, from, toTable, to, defaultConditionType);
  }

  public JoinMappingDto(String fromTable, String from, String toTable, String to, ConditionType conditionType) {
    this.from = from;
    this.to = to;
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.conditionType = conditionType;
  }
}
