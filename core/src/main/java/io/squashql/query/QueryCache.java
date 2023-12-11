package io.squashql.query;

import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.compiled.CompiledTable;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.TypedField;

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

  record TableScope(CompiledTable table,
                    Set<TypedField> columns,
                    CompiledCriteria whereCriteria,
                    CompiledCriteria havingCriteria,
                    List<TypedField> rollupColumns,
                    List<List<TypedField>> groupingSets,
                    VirtualTableDto virtualTable,
                    SquashQLUser user,
                    int limit) implements PrefetchQueryScope {
  }

  record SubQueryScope(QueryExecutor.QueryScope subQuery,
                       Set<TypedField> columns,
                       CompiledCriteria whereCriteriaDto,
                       CompiledCriteria havingCriteriaDto,
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
