package io.squashql.query.dto;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.field.Field;
import io.squashql.query.field.TableField;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class GroupColumnSetDto implements ColumnSet {

  public Field newField;

  public Field field;

  public Map<String, List<String>> values = new LinkedHashMap<>();

  public GroupColumnSetDto(String name, Field field) {
    this.newField = new TableField(name);
    this.field = field;
  }

  public GroupColumnSetDto withNewGroup(String groupName, List<String> groupValues) {
    this.values.put(groupName, new ArrayList<>(groupValues));
    return this;
  }

  @Override
  public List<Field> getColumnsForPrefetching() {
    return List.of(this.field);
  }

  @Override
  public List<Field> getNewColumns() {
    return List.of(this.newField, this.field);
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return ColumnSetKey.GROUP;
  }
}
