package me.paulbares.query.dto;

import java.util.EnumSet;
import java.util.Objects;

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

  /**
   * For Jackson.
   */
  public LogicalConditionDto() {
  }

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LogicalConditionDto that = (LogicalConditionDto) o;
    return this.type == that.type && Objects.equals(this.one, that.one) && Objects.equals(this.two, that.two);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.type, this.one, this.two);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
            '{' +
            "type='" + this.type + '\'' +
            ", one=" + this.one +
            ", two=" + this.two +
            '}';
  }
}
