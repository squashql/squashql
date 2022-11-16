package me.paulbares.query.builder;

import me.paulbares.query.dto.OrderKeywordDto;

import java.util.List;

public interface CanAddOrderBy {

  HasSelectAndRollup orderBy(String column, OrderKeywordDto orderKeywordDto);

  HasSelectAndRollup orderBy(String column, List<?> firstElements);
}
