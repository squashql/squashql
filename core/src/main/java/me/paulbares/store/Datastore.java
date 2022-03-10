package me.paulbares.store;

import java.util.List;
import java.util.Map;

public interface Datastore {

  String MAIN_SCENARIO_NAME = "base";

  String SCENARIO_FIELD_NAME = "scenario";

  List<? extends Store> stores();

  Map<String, ? extends Store> storesByName();

  void load(String scenario, String store, List<Object[]> tuples);

  void loadCsv(String scenario, String store, String path, String delimiter, boolean header);
}
