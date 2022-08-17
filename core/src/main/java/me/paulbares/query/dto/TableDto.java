package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class TableDto {

  public String name;

  public List<JoinDto> joins = new ArrayList<>();

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
}
