package io.squashql.query.builder;

import io.squashql.query.dto.VirtualTableDto;

public interface CanStartBuildingJoin {

  HasStartedBuildingJoin leftOuterJoin(String tableName);

  HasStartedBuildingJoin innerJoin(String tableName);

  HasStartedBuildingJoin innerJoin(VirtualTableDto virtualTableDto);
}
