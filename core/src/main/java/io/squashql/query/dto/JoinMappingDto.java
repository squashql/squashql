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
  public ConditionType conditionType = defaultConditionType;

  public JoinMappingDto(String from, String to) {
    this(from, to, defaultConditionType);
  }

  public JoinMappingDto(String from, String to, ConditionType conditionType) {
    this.from = from;
    this.to = to;
    this.conditionType = conditionType;
  }
}
