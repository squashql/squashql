package me.paulbares.dto;

import java.util.Collections;
import java.util.List;

public class JoinDto {

  public TableDto table;
  public String type; // inner|left
  public List<JoinMappingDto> mappings;

  /**
   * For Jackson.
   */
  public JoinDto() {
  }

  public JoinDto(TableDto table, String type, JoinMappingDto mapping) {
    this(table, type, Collections.singletonList(mapping));
  }

  public JoinDto(TableDto table, String type, List<JoinMappingDto> mappings) {
    this.table = table;
    this.type = type;
    this.mappings = mappings;
  }
}
