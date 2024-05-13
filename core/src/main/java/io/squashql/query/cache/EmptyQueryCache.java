package io.squashql.query.cache;

import io.squashql.query.SquashQLUser;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;

import java.util.Set;

public class EmptyQueryCache implements QueryCache {

  public static final QueryCache INSTANCE = new EmptyQueryCache();

  private EmptyQueryCache() {
  }

  @Override
  public ColumnarTable createRawResult(QueryCacheKey scope) {
    throw new IllegalStateException();
  }

  @Override
  public boolean contains(CompiledMeasure measure, QueryCacheKey scope) {
    return false;
  }

  @Override
  public void contributeToCache(Table result, Set<CompiledMeasure> measures, QueryCacheKey scope) {
    // NOOP
  }

  @Override
  public void contributeToResult(Table result, Set<CompiledMeasure> measures, QueryCacheKey scope) {
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

  @Override
  public String getHistogram() {
    return "";
  }
}
