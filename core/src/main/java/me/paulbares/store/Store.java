package me.paulbares.store;

import java.util.List;

import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

public interface Store {

  String name();

  List<Field> getFields();

  default String scenarioFieldName() {
    return scenarioFieldName(name(), ".");
  }

  static String scenarioFieldName(String storeName, String separator) {
    return storeName.toLowerCase() + separator + SCENARIO_FIELD_NAME;
  }
}
