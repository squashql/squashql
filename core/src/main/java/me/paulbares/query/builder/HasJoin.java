package me.paulbares.query.builder;

public interface HasJoin extends HasTable {
  HasJoin on(String fromTable, String from, String toTable, String to);
  HasStartedBuildingJoin left_outer_join(String tableName);
  HasStartedBuildingJoin inner_join(String tableName);
}
