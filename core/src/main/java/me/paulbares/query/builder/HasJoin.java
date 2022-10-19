package me.paulbares.query.builder;

import me.paulbares.query.dto.TableDto;

public interface HasJoin extends HasTable {
  HasJoin on(String fromTable, String from, String toTable, String to);

  HasStartedBuildingJoin join(TableDto tableDto);
}
