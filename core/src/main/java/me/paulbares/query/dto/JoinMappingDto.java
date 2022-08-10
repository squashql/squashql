package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class JoinMappingDto {

  public String from;
  public String to;

  public JoinMappingDto(String from, String to) {
    this.from = from;
    this.to = to;
  }
}
