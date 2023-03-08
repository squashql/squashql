package io.squashql.query.builder;

import io.squashql.query.dto.OrderKeywordDto;

import java.util.List;

public interface CanAddOrderBy {

  HasHaving orderBy(String column, OrderKeywordDto orderKeywordDto);

  HasHaving orderBy(String column, List<?> firstElements);
}
