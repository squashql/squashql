package me.paulbares.query.builder;

import me.paulbares.query.dto.QueryDto;

public interface CanBeBuildQuery {

  QueryDto build();
}
