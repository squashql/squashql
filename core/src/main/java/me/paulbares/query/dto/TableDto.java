package me.paulbares.query.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TableDto {

  public String name;

  public List<JoinDto> joins = new ArrayList<>();

  /**
   * For Jackson.
   */
  public TableDto() {
  }

  public TableDto(String name) {
    this.name = name;
  }

  public void join(TableDto other, String joinType, JoinMappingDto mapping) {
    this.joins.add(new JoinDto(other, joinType, mapping));
  }

  public void innerJoin(TableDto other, String from, String to) {
    this.joins.add(new JoinDto(other, "inner", new JoinMappingDto(from, to)));
  }

  public void leftJoin(TableDto other, String from, String to) {
    this.joins.add(new JoinDto(other, "left", new JoinMappingDto(from, to)));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableDto tableDto = (TableDto) o;
    return Objects.equals(this.name, tableDto.name) && Objects.equals(this.joins, tableDto.joins);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.name, this.joins);
  }

  @Override
  public String toString() {
    return "TableDto{" +
            "name='" + name + '\'' +
            ", joins=" + joins +
            '}';
  }
}
