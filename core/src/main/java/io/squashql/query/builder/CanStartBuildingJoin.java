package io.squashql.query.builder;

import io.squashql.query.dto.GroupColumnSetDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.VirtualTableDto;

public interface CanStartBuildingJoin {

  HasStartedBuildingJoin join(String tableName, JoinType joinType);

  HasStartedBuildingJoin join(VirtualTableDto virtualTableDto, JoinType joinType);

  HasJoin join(GroupColumnSetDto groupColumnSetDto);
}
