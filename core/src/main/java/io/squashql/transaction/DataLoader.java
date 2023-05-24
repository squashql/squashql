package io.squashql.transaction;

import java.util.List;

public interface DataLoader {

  String MAIN_SCENARIO_NAME = "base";

  String SCENARIO_FIELD_NAME = "scenario";

  void load(String scenario, String table, List<Object[]> tuples);

  default void load(String table, List<Object[]> tuples) {
    load(MAIN_SCENARIO_NAME, table, tuples);
  }

  void loadCsv(String scenario, String table, String path, String delimiter, boolean header);
}
