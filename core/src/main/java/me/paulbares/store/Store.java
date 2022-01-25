package me.paulbares.store;

import java.util.List;

public interface Store {
  String name();
  List<Field> getFields();

  void load(String scenario, List<Object[]> tuples);
}
