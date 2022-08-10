package me.paulbares.query.dto;

import java.util.List;
import java.util.Objects;

public final class ExplicitOrderDto implements OrderDto {

  public List<?> explicit;

  /**
   * For Jackson.
   */
  public ExplicitOrderDto() {
  }

  public ExplicitOrderDto(List<?> explicit) {
    this.explicit = explicit;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
            "{" +
            "explicit=" + explicit +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExplicitOrderDto that = (ExplicitOrderDto) o;
    return Objects.equals(this.explicit, that.explicit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.explicit);
  }
}
