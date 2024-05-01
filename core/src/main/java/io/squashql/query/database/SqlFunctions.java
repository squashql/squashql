package io.squashql.query.database;

import java.util.Set;

public final class SqlFunctions {

  public static final Set<String> SUPPORTED_DATE_FUNCTIONS = Set.of("YEAR", "QUARTER", "MONTH");
  public static final Set<String> SUPPORTED_STRING_FUNCTIONS = Set.of("lower", "upper");

  private SqlFunctions() {
  }
}
