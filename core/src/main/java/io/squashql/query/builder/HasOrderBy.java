package io.squashql.query.builder;

public interface HasOrderBy extends CanBeBuildQuery {

  CanBeBuildQuery limit(int limit);
}
