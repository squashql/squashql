package io.squashql.transaction;

import java.util.List;

public interface DataLoader {

  String MAIN_SCENARIO_NAME = "base";

  String SCENARIO_FIELD_NAME = "scenario";

  void load(String scenario, String store, List<Object[]> tuples);

  default void load(String store, List<Object[]> tuples) {
    load(MAIN_SCENARIO_NAME, store, tuples);
  }

  void loadCsv(String scenario, String store, String path, String delimiter, boolean header);
}
