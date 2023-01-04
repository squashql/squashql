package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public final class SimpleOrderDto implements OrderDto {

  public OrderKeywordDto order;

  public SimpleOrderDto(OrderKeywordDto order) {
    this.order = order;
  }
}
