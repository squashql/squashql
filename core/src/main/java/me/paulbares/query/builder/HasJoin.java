package me.paulbares.query.builder;

public interface HasJoin extends HasTable {
  HasJoin on(String fromTable, String from, String toTable, String to);
  HasStartedBuildingJoin leftOuterJoin(String tableName);
  HasStartedBuildingJoin innerJoin(String tableName);
}
