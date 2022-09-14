package me.paulbares.query;

import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Field;

import java.util.Map;
import java.util.Set;

public interface QueryCache {

  ColumnarTable createRawResult(QueryScope scope);

  boolean contains(Measure measure, QueryScope scope);

  void contributeToCache(Table result, Set<Measure> measures, QueryScope scope);

  void contributeToResult(Table result, Set<Measure> measures, QueryScope scope);

  void clear();

  CacheStatsDto stats();

  record TableScope(TableDto tableDto, Set<Field> columns, Map<String, ConditionDto> conditions) implements QueryScope {
  }

  record SubQueryScope(QueryDto subQueryDto, Set<Field> columns, Map<String, ConditionDto> conditions) implements QueryScope {
  }

  /**
   * Marker interface.
   */
  interface QueryScope {
    Set<Field> columns();
  }
}
