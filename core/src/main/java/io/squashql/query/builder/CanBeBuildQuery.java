package io.squashql.query.builder;

import io.squashql.query.dto.GroupColumnSetDto;
import io.squashql.query.dto.QueryDto;

public interface CanBeBuildQuery {

  CanBeBuildQuery addGroupingSet(GroupColumnSetDto groupColumnSetDto);

  QueryDto build();
}
