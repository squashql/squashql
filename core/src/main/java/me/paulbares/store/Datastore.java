package me.paulbares.store;

import java.util.List;

public interface Datastore {

  String MAIN_SCENARIO_NAME = "base";

  List<? extends Store> stores();

  void load(String scenario, String store, List<Object[]> tuples);

  void loadCsv(String scenario, String store, String path, String delimiter, boolean header);
}
