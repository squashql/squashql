package io.squashql.query;

import io.squashql.query.database.SqlUtils;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Objects;
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

  public String alias;

  public TableField(String fullName) {
    this.fullName = fullName;
    setAttributes();
  }

  public TableField(String tableName, String fieldName) {
    this.tableName = tableName;
    this.fieldName = fieldName;
    setAttributes();
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
    } else {
      this.fullName = SqlUtils.getFieldFullName(Objects.requireNonNull(this.tableName), Objects.requireNonNull(this.fieldName));
    }
  }

  @Override
  public String name() {
    return this.fullName;
  }

  @Override
  public Field as(String alias) {
    return new TableField(this.tableName, this.fieldName, this.fullName, alias);
  }

  @Override
  public String alias() {
    return this.alias;
  }

  /*
   * Syntactic sugar helpers.
   */

  public static Field tableField(final String tableName, final String name) {
    return new TableField(tableName, name);
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
