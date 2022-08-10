package me.paulbares.query.dto;

import java.util.Objects;

public class SimpleOrderDto implements OrderDto {

  public OrderKeywordDto order;

  /**
   * For Jackson.
   */
  public SimpleOrderDto() {
  }

  public SimpleOrderDto(OrderKeywordDto order) {
    this.order = order;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
            "{" +
            "order=" + order +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SimpleOrderDto that = (SimpleOrderDto) o;
    return this.order == that.order;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.order);
  }
}
