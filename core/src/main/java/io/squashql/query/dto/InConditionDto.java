package io.squashql.query.dto;

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

  public Set<Object> values;
  public boolean invert;

  public InConditionDto(Object value, boolean invert) {
    Set<Object> set = new HashSet<>();
    if (value.getClass().isArray()) {
      Object[] array = (Object[]) value;
      for (Object e : array) {
        set.add(e);
      }
    } else if (value instanceof Collection<?> collection) {
      set.addAll(collection);
    } else {
      throw new IllegalArgumentException("Unexpected value for in condition " + value);
    }
    this.values = set;
    this.invert = invert;
  }

  @Override
  public ConditionType type() {
    return IN;
  }
}
