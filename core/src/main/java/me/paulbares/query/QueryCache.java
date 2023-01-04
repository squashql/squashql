package me.paulbares.query;

import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.CriteriaDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Field;

import java.util.List;
import java.util.Set;

public interface QueryCache {

  ColumnarTable createRawResult(PrefetchQueryScope scope);

  boolean contains(Measure measure, PrefetchQueryScope scope);

  void contributeToCache(Table result, Set<Measure> measures, PrefetchQueryScope scope);

  void contributeToResult(Table result, Set<Measure> measures, PrefetchQueryScope scope);

  void clear();

  CacheStatsDto stats();

  record TableScope(TableDto tableDto, Set<Field> columns, CriteriaDto criteriaDto, List<Field> rollupColumns) implements PrefetchQueryScope {
  }

  record SubQueryScope(QueryDto subQueryDto, Set<Field> columns, CriteriaDto criteriaDto) implements PrefetchQueryScope {
  }

  /**
   * Marker interface.
   */
  interface PrefetchQueryScope {
    Set<Field> columns();
  }
}
