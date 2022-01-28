package me.paulbares.query.dto;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JoinDto joinDto = (JoinDto) o;
    return Objects.equals(this.table, joinDto.table) && Objects.equals(this.type, joinDto.type) && Objects.equals(this.mappings,
            joinDto.mappings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.table, this.type, this.mappings);
  }

  @Override
  public String toString() {
    return "JoinDto{" +
            "table=" + table +
            ", type='" + type + '\'' +
            ", mappings=" + mappings +
            '}';
  }
}
