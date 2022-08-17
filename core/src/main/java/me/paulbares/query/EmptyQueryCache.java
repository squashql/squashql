package me.paulbares.query;

import me.paulbares.query.dto.CacheStatsDto;

import java.util.Set;

public class EmptyQueryCache implements QueryCache {

  public static final QueryCache INSTANCE = new EmptyQueryCache();

  private EmptyQueryCache() {
  }

  @Override
  public ColumnarTable createRawResult(QueryScope scope) {
    throw new IllegalStateException();
  }

  @Override
  public boolean contains(Measure measure, QueryScope scope) {
    return false;
  }

  @Override
  public void contributeToCache(Table result, Set<Measure> measures, QueryScope scope) {
    // NOOP
  }

  @Override
  public void contributeToResult(Table result, Set<Measure> measures, QueryScope scope) {
    // NOOP
  }

  @Override
  public void clear() {
  }

  @Override
  public CacheStatsDto stats() {
    return new CacheStatsDto(-1, -1, -1);
  }
}
