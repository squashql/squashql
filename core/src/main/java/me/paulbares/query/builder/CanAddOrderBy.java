package me.paulbares.query.builder;

import me.paulbares.query.dto.OrderKeywordDto;

import java.util.List;

public interface CanAddOrderBy {

  HasSelect orderBy(String column, OrderKeywordDto orderKeywordDto);

  HasSelect orderBy(String column, List<?> firstElements);
}
