package io.squashql.query.dto;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor // For Jackson
public final class SimpleOrderDto implements OrderDto {

  public OrderKeywordDto order;
  public NullsOrderDto nullsOrder;

  public SimpleOrderDto(OrderKeywordDto order) {
    this.order = order;
    this.nullsOrder = NullsOrderDto.FIRST;
  }

}
