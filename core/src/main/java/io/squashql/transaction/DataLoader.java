package io.squashql.transaction;

import java.util.List;

public interface DataLoader {

  String MAIN_SCENARIO_NAME = "base";

  String SCENARIO_FIELD_NAME = "scenario";

  void load(String table, List<Object[]> tuples);

  void loadCsv(String table, String path, String delimiter, boolean header);
}
