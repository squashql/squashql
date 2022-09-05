package me.paulbares.query;

import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Field;

import java.util.Map;
import java.util.Set;

public interface QueryCache {

  ColumnarTable createRawResult(Key scope);

  boolean contains(Measure measure, Key scope);

  void contributeToCache(Table result, Set<Measure> measures, Key scope);

  void contributeToResult(Table result, Set<Measure> measures, Key scope);

  void clear();

  CacheStatsDto stats();

  record QueryScope(TableDto tableDto, Set<Field> columns, Map<String, ConditionDto> conditions) implements Key {
  }

  record SubQueryScope(QueryDto subQueryDto, Set<Field> columns, Map<String, ConditionDto> conditions) implements Key {
  }

  /**
   * Marker interface.
   */
  interface Key {
    Set<Field> columns();
  }
}
