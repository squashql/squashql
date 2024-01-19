package io.squashql.type;

import io.squashql.query.CountMeasure;
import io.squashql.query.database.QueryRewriter;

import java.util.Objects;

public record TableTypedField(String store, String name, Class<?> type, String alias) implements NamedTypedField {

  public TableTypedField {
    Objects.requireNonNull(name);
    Objects.requireNonNull(type);
  }

  public TableTypedField(String store, String name, Class<?> type) {
    this(store, name, type, null);
  }

  @Override
  public String sqlExpression(QueryRewriter qr) {
    if (CountMeasure.FIELD_NAME.equals(this.name)) {
      return CountMeasure.FIELD_NAME;
    } else {
      return qr.getFieldFullName(this);
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public NamedTypedField as(String alias) {
    return new TableTypedField(this.store, this.name, this.type, alias);
  }
}
