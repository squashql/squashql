package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class NotConditionDto implements ConditionDto {

  /**
   * The condition to reverse
   */
  public ConditionDto c;

  public NotConditionDto(ConditionDto c) {
    this.c = c;
  }

  @Override
  public ConditionType type() {
    return ConditionType.NOT;
  }
}
