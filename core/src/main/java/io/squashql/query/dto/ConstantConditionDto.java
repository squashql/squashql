package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.EnumSet;

import static io.squashql.query.dto.ConditionType.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class ConstantConditionDto implements ConditionDto {

  public ConditionType type;

  private static final EnumSet<ConditionType> supportedTypes = EnumSet.of(NULL, NOT_NULL, TRUE, FALSE);

  public ConstantConditionDto(ConditionType type) {
    if (!supportedTypes.contains(type)) {
      throw new IllegalArgumentException("Unexpected type for SVC: " + type);
    }
    this.type = type;
  }

  @Override
  public ConditionType type() {
    return this.type;
  }
}
