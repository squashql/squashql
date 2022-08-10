package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static me.paulbares.query.dto.ConditionType.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class SingleValueConditionDto implements ConditionDto {

  public ConditionType type;

  public Object value;

  private static final EnumSet<ConditionType> supportedTypes = EnumSet.of(IN, LT, LE, GT, GE, EQ, NEQ);

  public SingleValueConditionDto(ConditionType type, Object value) {
    this.type = type;
    if (this.type == IN) {
      Set<Object> set = new HashSet<>();
      if (value.getClass().isArray()) {
        Object[] array = (Object[]) value;
        for (Object e : array) {
          set.add(e);
        }
      } else if (value instanceof Collection<?> collection) {
        set.addAll(collection);
      } else {
        throw new IllegalArgumentException("Unexpected value type for in condition " + value);
      }
      this.value = set;
    } else {
      this.value = value;
    }
  }

  @Override
  public ConditionType type() {
    return this.type;
  }
}
