package io.squashql.query.dto;

import io.squashql.store.Store;
import io.squashql.type.TableTypedField;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
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

  public static Store toStore(VirtualTableDto virtualTableDto) {
    List<TableTypedField> fields = new ArrayList<>();
    for (int i = 0; i < virtualTableDto.fields.size(); i++) {
      Class<?> klazz = virtualTableDto.records.get(0).get(i).getClass(); // take the first row to determine the type
      fields.add(new TableTypedField(virtualTableDto.name, virtualTableDto.fields.get(i), klazz, true));
    }
    return new Store(virtualTableDto.name, fields);
  }
}
