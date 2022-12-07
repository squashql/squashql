package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static me.paulbares.query.dto.ConditionType.IN;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class InConditionDto implements ConditionDto {

  public final ConditionType type = IN;

  public Set<Object> values;

  public InConditionDto(Object value) {
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
  }

  @Override
  public ConditionType type() {
    return this.type;
  }
}
