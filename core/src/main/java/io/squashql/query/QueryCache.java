package io.squashql.query;

import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;
import io.squashql.store.Field;

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
