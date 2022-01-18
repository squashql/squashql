package me.paulbares.store;

import java.util.List;

public interface Datastore {

  String MAIN_SCENARIO_NAME = "base";

  List<Field> getFields();

  void load(String scenario, List<Object[]> tuples);
}
