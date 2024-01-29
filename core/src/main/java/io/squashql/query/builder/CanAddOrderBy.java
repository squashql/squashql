package io.squashql.query.builder;

import io.squashql.query.NamedField;
import io.squashql.query.dto.OrderKeywordDto;

import java.util.List;

public interface CanAddOrderBy {

  HasHaving orderBy(NamedField column, OrderKeywordDto orderKeywordDto);

  HasHaving orderBy(NamedField column, List<?> firstElements);
}
