package io.squashql.query.builder;

import java.util.List;

public interface CanAddRollup extends HasOrderBy, CanAddOrderBy, CanAddHaving {

  CanAddHaving rollup(String... columns);

  default CanAddHaving rollup(List<String> columns) {
    return rollup(columns.toArray(new String[0]));
  }
}
