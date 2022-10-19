package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.TableDto;

public interface HasFromTable extends HasCondition {
  HasJoin where(String field, ConditionDto conditionDto);

  HasStartIncompleteJoin join(TableDto tableDto);
}
