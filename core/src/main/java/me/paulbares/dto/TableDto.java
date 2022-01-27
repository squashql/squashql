package me.paulbares.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableDto {

  public String name;

  public List<JoinDto> joins = new ArrayList<>();

  /**
   * For Jackson.
   */
  public TableDto() {
  }

  public TableDto(String name) {
    this(name, Collections.emptyList());
  }

  public TableDto(String name, JoinDto join) {
    this(name, Collections.singletonList(join));
  }

  public TableDto(String name, List<JoinDto> joins) {
    this.name = name;
    this.joins = new ArrayList<>(joins);
  }

  public void join(TableDto other, String joinType, JoinMappingDto mapping) {
    this.joins.add(new JoinDto(other, joinType, mapping));
  }
}
