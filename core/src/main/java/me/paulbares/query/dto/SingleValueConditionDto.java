package me.paulbares.query.dto;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static me.paulbares.query.dto.ConditionType.EQ;
import static me.paulbares.query.dto.ConditionType.GE;
import static me.paulbares.query.dto.ConditionType.GT;
import static me.paulbares.query.dto.ConditionType.IN;
import static me.paulbares.query.dto.ConditionType.LE;
import static me.paulbares.query.dto.ConditionType.LT;
import static me.paulbares.query.dto.ConditionType.NEQ;

public final class SingleValueConditionDto implements ConditionDto {

  public ConditionType type;

  public Object value;

  private static final EnumSet<ConditionType> supportedTypes = EnumSet.of(IN, LT, LE, GT, GE, EQ, NEQ);

  /**
   * For Jackson.
   */
  public SingleValueConditionDto() {
  }

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SingleValueConditionDto that = (SingleValueConditionDto) o;
    return Objects.equals(this.type, that.type) && Objects.equals(this.value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.type, this.value);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
            '{' +
            "type='" + this.type + '\'' +
            ", value=" + this.value +
            '}';
  }
}
