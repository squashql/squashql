package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class JoinMappingDto {

  public String from;
  public String to;
  public String fromTable;
  public String toTable;

  public JoinMappingDto(String fromTable, String from, String toTable, String to) {
    this.from = from;
    this.to = to;
    this.fromTable = fromTable;
    this.toTable = toTable;
  }
}
