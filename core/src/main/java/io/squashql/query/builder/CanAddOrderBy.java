package io.squashql.query.builder;

import io.squashql.query.Field;
import io.squashql.query.dto.OrderKeywordDto;

import java.util.List;

public interface CanAddOrderBy {

  HasHaving orderBy(Field column, OrderKeywordDto orderKeywordDto);

  HasHaving orderBy(Field column, List<?> firstElements);
}
