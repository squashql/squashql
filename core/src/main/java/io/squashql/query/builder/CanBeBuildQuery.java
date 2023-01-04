package io.squashql.query.builder;

import io.squashql.query.dto.QueryDto;

public interface CanBeBuildQuery {

  QueryDto build();
}
