package io.squashql.query;

import io.squashql.query.dto.CacheStatsDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;

import java.util.Set;

public class EmptyQueryCache implements QueryCache {

  public static final QueryCache INSTANCE = new EmptyQueryCache();

  private EmptyQueryCache() {
  }

  @Override
  public ColumnarTable createRawResult(PrefetchQueryScope scope) {
    throw new IllegalStateException();
  }

  @Override
  public boolean contains(Measure measure, PrefetchQueryScope scope) {
    return false;
  }

  @Override
  public void contributeToCache(Table result, Set<Measure> measures, PrefetchQueryScope scope) {
    // NOOP
  }

  @Override
  public void contributeToResult(Table result, Set<Measure> measures, PrefetchQueryScope scope) {
    // NOOP
  }

  @Override
  public void clear(SquashQLUser user) {
  }

  @Override
  public void clear() {
  }

  @Override
  public CacheStatsDto stats(SquashQLUser user) {
    return new CacheStatsDto(-1, -1, -1);
  }
}
