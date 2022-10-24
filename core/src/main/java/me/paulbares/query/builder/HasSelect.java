package me.paulbares.query.builder;

public interface HasSelect extends CanBeBuildQuery {

  CanBeBuildQuery limit(int limit);
}
