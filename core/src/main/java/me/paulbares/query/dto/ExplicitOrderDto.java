package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class ExplicitOrderDto implements OrderDto {

  public List<?> explicit;

  public ExplicitOrderDto(List<?> explicit) {
    this.explicit = explicit;
  }
}
