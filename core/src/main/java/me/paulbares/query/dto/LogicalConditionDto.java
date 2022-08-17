package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.EnumSet;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class LogicalConditionDto implements ConditionDto {

  private static final EnumSet<ConditionType> supportedTypes = EnumSet.of(ConditionType.AND, ConditionType.OR);

  public ConditionType type; // AND OR

  /**
   * The first sub-condition
   */
  public ConditionDto one;

  /**
   * The second sub-condition
   */
  public ConditionDto two;

  public LogicalConditionDto(ConditionType type, ConditionDto one, ConditionDto two) {
    if (!supportedTypes.contains(type)) {
      throw new IllegalArgumentException("Expected type of " + supportedTypes + " but " + type + " was found");
    }
    this.type = type;
    this.one = one;
    this.two = two;
  }

  @Override
  public ConditionType type() {
    return this.type;
  }
}
