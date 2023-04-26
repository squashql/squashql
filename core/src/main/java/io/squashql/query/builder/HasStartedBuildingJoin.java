package io.squashql.query.builder;

import io.squashql.query.dto.CriteriaDto;

public interface HasStartedBuildingJoin {

  HasJoin on(CriteriaDto joinCriteriaDto);
}
