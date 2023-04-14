package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class VirtualTableDto {

  public String name;
  public List<String> fields;
  public List<List<Object>> records;

  public VirtualTableDto(String name, List<String> fields, List<List<Object>> records) {
    this.name = name;
    this.fields = fields;
    this.records = records;
  }
}
