package me.paulbares.store;

import java.util.List;

public interface Datastore {

  String MAIN_SCENARIO_NAME = "base";

  List<Store> stores();

  void load(String scenario, String store, List<Object[]> tuples);
}
