package io.squashql.query.builder;

import io.squashql.query.Field;
import java.util.List;

public interface CanAddRollup extends HasOrderBy, CanAddOrderBy, CanAddHaving {

  CanAddHaving rollup(Field... columns);

  default CanAddHaving rollup(List<Field> columns) {
    return rollup(columns.toArray(new Field[0]));
  }
}
