package me.paulbares.store;

import java.util.List;

public interface Datastore {

  List<Field> getFields();

  void load(String scenario, List<Object[]> tuples);
}
