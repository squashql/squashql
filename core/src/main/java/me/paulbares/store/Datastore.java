package me.paulbares.store;

import java.util.Map;

public interface Datastore {

  String MAIN_SCENARIO_NAME = "base";

  String SCENARIO_FIELD_NAME = "scenario";

  Map<String, Store> storesByName();
}
