package io.squashql.query;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.stream.Collectors;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class TableField implements Field {

  public String tableName;

  public String fieldName;

  /**
   * Should be "tableName.fieldName"
   */
  public String fullName;

  public TableField(String fullName) {
    this.fullName = fullName;
    setAttributes();
  }

  public TableField(String tableName, String fieldName) {
    this.tableName = tableName;
    this.fieldName = fieldName;
  }

  private void setAttributes() {
    if (this.fullName != null) {
      String[] split = this.fullName.split("\\.");
      if (split.length > 1) {
        this.tableName = split[0];
        this.fieldName = split[1];
      } else {
        this.fieldName = split[0];
      }
    }
  }

  @Override
  public String name() {
    return this.fullName;
  }

  /*
   * Syntactic sugar helpers.
   */

  public static Field tableField(final String tableName, final String name) {
    return new TableField(tableName);
  }

  public static Field tableField(final String name) {
    return new TableField(name);
  }

  public static List<Field> tableFields(String tableName, List<String> fields) {
    return fields.stream().map(f -> new TableField(tableName, f)).collect(Collectors.toList());
  }

  public static List<Field> tableFields(List<String> fields) {
    return fields.stream().map(TableField::new).collect(Collectors.toList());
  }


}
