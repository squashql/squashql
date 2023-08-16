package io.squashql.query;

import io.squashql.query.dto.*;
import io.squashql.store.TypedField;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;

import java.util.List;
import java.util.Set;

public interface QueryCache {

  ColumnarTable createRawResult(PrefetchQueryScope scope);

  boolean contains(Measure measure, PrefetchQueryScope scope);

  void contributeToCache(Table result, Set<Measure> measures, PrefetchQueryScope scope);

  void contributeToResult(Table result, Set<Measure> measures, PrefetchQueryScope scope);

  /**
   * Invalidates the cache associated to the given user.
   *
   * @param user the user identifier
   */
  void clear(SquashQLUser user);

  /**
   * Invalidate the whole cache.
   */
  void clear();

  CacheStatsDto stats(SquashQLUser user);

  record TableScope(TableDto tableDto,
                    Set<TypedField> columns,
                    CriteriaDto whereCriteriaDto,
                    CriteriaDto havingCriteriaDto,
                    List<TypedField> rollupColumns,
                    List<List<TypedField>> groupingSets,
                    VirtualTableDto virtualTableDto,
                    SquashQLUser user,
                    int limit) implements PrefetchQueryScope {
  }

  record SubQueryScope(QueryDto subQueryDto,
                       Set<TypedField> columns,
                       CriteriaDto whereCriteriaDto,
                       CriteriaDto havingCriteriaDto,
                       SquashQLUser user,
                       int limit) implements PrefetchQueryScope {
  }

  /**
   * Marker interface.
   */
  interface PrefetchQueryScope {
    Set<TypedField> columns();

    SquashQLUser user();
  }
}
