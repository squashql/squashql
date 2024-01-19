package io.squashql.query.builder;

import io.squashql.query.NamedField;

import java.util.List;

public interface CanAddRollup extends HasOrderBy, CanAddOrderBy, CanAddHaving {

  CanAddHaving rollup(NamedField... columns);

  default CanAddHaving rollup(List<NamedField> columns) {
    return rollup(columns.toArray(new NamedField[0]));
  }
}
