package me.paulbares;

import org.apache.spark.sql.types.StructField;

import java.util.List;

public interface Datastore {

  StructField[] getFields(); // FIXME should not be spark fields

  void load(String scenario, List<Object[]> tuples);
}
