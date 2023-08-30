package io.squashql.query.dto;

import io.squashql.query.Field;
import io.squashql.query.TableField;
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

  public Field from;
  public Field to;
  public ConditionType conditionType;

  public JoinMappingDto(String from, String to) {
    this(new TableField(from), new TableField(to), ConditionType.EQ);
  }

  public JoinMappingDto(String from, String to, ConditionType conditionType) {
    this(new TableField(from), new TableField(to), conditionType);
  }

  public JoinMappingDto(Field from, Field to) {
    this(from, to, ConditionType.EQ);
  }

  public JoinMappingDto(Field from, Field to, ConditionType conditionType) {
    this.from = from;
    this.to = to;
    this.conditionType = conditionType;
  }
}
