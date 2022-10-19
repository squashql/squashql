package me.paulbares.query.builder;

import me.paulbares.query.dto.TableDto;

public interface HasStartJoin extends HasJoin {
  HasStartJoin on(String fromTable, String from, String toTable, String to);

  HasStartIncompleteJoin join(TableDto tableDto);
}
