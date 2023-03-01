package io.squashql.query.builder;

import io.squashql.query.dto.CriteriaDto;

public interface CanAddHaving extends HasOrderBy, CanAddOrderBy {

  HasHaving having(CriteriaDto criteriaDto);
}
