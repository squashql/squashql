package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.function.Function;

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
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter) {
    setAttributes();
    if (CountMeasure.FIELD_NAME.equals(this.fieldName)) {
      return CountMeasure.FIELD_NAME;
    } else {
      return queryRewriter.getFieldFullName(new io.squashql.type.TableField(this.tableName, this.fieldName, Object.class));
    }
  }

  @Override
  public String name() {
    return this.fullName;
  }
}
