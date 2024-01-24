package io.squashql.type;

import io.squashql.query.CountMeasure;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;

import java.util.Objects;

public record TableTypedField(String store, String name, Class<?> type, String alias,
                              boolean cte) implements TypedField {

  public TableTypedField {
    Objects.requireNonNull(name);
    Objects.requireNonNull(type);
  }

  public TableTypedField(String store, String name, Class<?> type) {
    this(store, name, type, null, false);
  }

  public TableTypedField(String store, String name, Class<?> type, boolean cte) {
    this(store, name, type, null, cte);
  }

  @Override
  public String sqlExpression(QueryRewriter qr) {
    if (CountMeasure.FIELD_NAME.equals(this.name)) {
      return CountMeasure.FIELD_NAME;
    } else if (this.cte) {
      return SqlUtils.getFieldFullName(qr.cteName(this.store), qr.fieldName(this.name));
    } else {
      return SqlUtils.getFieldFullName(this.store == null ? null : qr.tableName(this.store), qr.fieldName(this.name));
    }
  }

  @Override
  public TypedField as(String alias) {
    return new TableTypedField(this.store, this.name, this.type, alias, this.cte);
  }
}
