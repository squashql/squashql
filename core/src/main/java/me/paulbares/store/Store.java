package me.paulbares.store;

import java.util.List;

public record Store(String name, List<Field> fields) {

  public String scenarioFieldName() {
    return this.name + "_" + Datastore.SCENARIO_FIELD_NAME;
  }
}
