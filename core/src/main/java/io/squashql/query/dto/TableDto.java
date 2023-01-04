package io.squashql.query.dto;

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

  public void join(TableDto other, JoinType joinType, JoinMappingDto mapping) {
    this.joins.add(new JoinDto(other, joinType, mapping));
  }

  public void join(TableDto other, JoinType joinType, List<JoinMappingDto> JoinMappingDtos) {
    this.joins.add(new JoinDto(other, joinType, JoinMappingDtos));
  }

  public void innerJoin(TableDto other, String from, String to) {
    this.joins.add(new JoinDto(other, JoinType.INNER, new JoinMappingDto(this.name, from, other.name, to)));
  }

  public void leftJoin(TableDto other, String from, String to) {
    this.joins.add(new JoinDto(other, JoinType.LEFT, new JoinMappingDto(this.name, from, other.name, to)));
  }
}
