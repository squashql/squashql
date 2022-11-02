package me.paulbares.query;

import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Field;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface QueryCache {

  ColumnarTable createRawResult(PrefetchQueryScope scope, Function<String, Field> fieldSupplier);

  boolean contains(Measure measure, PrefetchQueryScope scope);

  void contributeToCache(Table result, Set<Measure> measures, PrefetchQueryScope scope);

  void contributeToResult(Table result, Set<Measure> measures, PrefetchQueryScope scope);

  void clear();

  CacheStatsDto stats();

  record TableScope(TableDto tableDto, Set<Field> columns, Map<String, ConditionDto> conditions) implements PrefetchQueryScope {
  }

  record SubQueryScope(QueryDto subQueryDto, Set<Field> columns, Map<String, ConditionDto> conditions) implements PrefetchQueryScope {
  }

  /**
   * Marker interface.
   */
  interface PrefetchQueryScope {
    Set<Field> columns();
  }
}
