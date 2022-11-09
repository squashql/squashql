package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class JoinDto {

  public TableDto table;
  public JoinType type; // inner|left
  public List<JoinMappingDto> mappings;

  public JoinDto(TableDto table, JoinType type, JoinMappingDto mapping) {
    this(table, type, Collections.singletonList(mapping));
  }

  public JoinDto(TableDto table, JoinType type, List<JoinMappingDto> mappings) {
    this.table = table;
    this.type = type;
    this.mappings = mappings;
  }
}
