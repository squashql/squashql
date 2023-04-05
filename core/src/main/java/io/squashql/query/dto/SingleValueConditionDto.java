package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.EnumSet;

import static io.squashql.query.dto.ConditionType.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class SingleValueConditionDto implements ConditionDto {

  private static final EnumSet<ConditionType> supportedTypes = EnumSet.of(LT, LE, GT, GE, EQ, NEQ, LIKE);

  public ConditionType type;

  public Object value;

  public SingleValueConditionDto(ConditionType type, Object value) {
    if (!supportedTypes.contains(type)) {
      throw new IllegalArgumentException("Unexpected type for SVC: " + type);
    }
    this.type = type;
    this.value = value;
  }

  @Override
  public ConditionType type() {
    return this.type;
  }
}
