package io.squashql.query.builder;

public interface HasStartedBuildingJoin {

  HasJoin on(String fromTable, String from, String toTable, String to);
}
