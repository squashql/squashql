package me.paulbares.query.builder;

public interface CanStartBuildingJoin {
  HasStartedBuildingJoin leftOuterJoin(String tableName);
  HasStartedBuildingJoin innerJoin(String tableName);
}
