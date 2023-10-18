package io.squashql.query.dto;

import io.squashql.query.Field;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static io.squashql.query.dto.ConditionType.IN;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class InConditionDto implements ConditionDto {

  public final ConditionType type = IN;

  public List<Field> values;

  public InConditionDto(List<Field> values) {
    this.values = values;
  }

  @Override
  public ConditionType type() {
    return this.type;
  }
}
