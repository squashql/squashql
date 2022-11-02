package me.paulbares.query;

import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.store.Field;

import java.util.Set;
import java.util.function.Function;

public class EmptyQueryCache implements QueryCache {

  public static final QueryCache INSTANCE = new EmptyQueryCache();

  private EmptyQueryCache() {
  }

  @Override
  public ColumnarTable createRawResult(PrefetchQueryScope scope, Function<String, Field> fieldSupplier) {
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
  public void clear() {
  }

  @Override
  public CacheStatsDto stats() {
    return new CacheStatsDto(-1, -1, -1);
  }
}
