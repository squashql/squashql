package io.squashql.query;

import io.squashql.query.dto.*;
import io.squashql.store.Field;

import java.util.List;
import java.util.Set;

public interface QueryCache {

  ColumnarTable createRawResult(PrefetchQueryScope scope);

  boolean contains(Measure measure, PrefetchQueryScope scope);

  void contributeToCache(Table result, Set<Measure> measures, PrefetchQueryScope scope);

  void contributeToResult(Table result, Set<Measure> measures, PrefetchQueryScope scope);

  void clear(SquashQLUser user);

  /**
   * For testing purpose only.
   */
  void clear();

  CacheStatsDto stats(SquashQLUser user);

  record TableScope(TableDto tableDto,
                    Set<Field> columns,
                    CriteriaDto whereCriteriaDto,
                    CriteriaDto havingCriteriaDto,
                    List<Field> rollupColumns,
                    VirtualTableDto virtualTableDto,
                    SquashQLUser user,
                    int limit) implements PrefetchQueryScope {
  }

  record SubQueryScope(QueryDto subQueryDto,
                       Set<Field> columns,
                       CriteriaDto whereCriteriaDto,
                       CriteriaDto havingCriteriaDto,
                       SquashQLUser user,
                       int limit) implements PrefetchQueryScope {
  }

  /**
   * Marker interface.
   */
  interface PrefetchQueryScope {
    Set<Field> columns();

    SquashQLUser user();
  }
}
