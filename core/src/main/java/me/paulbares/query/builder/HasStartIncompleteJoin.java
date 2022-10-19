package me.paulbares.query.builder;

public interface HasStartIncompleteJoin {
  HasStartJoin on(String fromTable, String from, String toTable, String to);
}
