package io.squashql.query.cache;

import io.squashql.query.SquashQLUser;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class GlobalCache implements QueryCache {

  private static final SquashQLUser ANONYMOUS = new SquashQLUser() {
  };

  protected final Map<SquashQLUser, CaffeineQueryCache> cacheByUser = new ConcurrentHashMap<>();

  protected final Supplier<CaffeineQueryCache> cacheSupplier;

  public GlobalCache(Supplier<CaffeineQueryCache> cacheSupplier) {
    this.cacheSupplier = cacheSupplier;
  }

  protected static SquashQLUser user(SquashQLUser user) {
    return user == null ? ANONYMOUS : user;
  }

  protected CaffeineQueryCache getCache(QueryCacheKey scope) {
    return this.cacheByUser.computeIfAbsent(user(scope.user()), u -> this.cacheSupplier.get());
  }

  @Override
  public ColumnarTable createRawResult(QueryCacheKey scope) {
    return getCache(scope).createRawResult(scope);
  }

  @Override
  public boolean contains(CompiledMeasure measure, QueryCacheKey scope) {
    return getCache(scope).contains(measure, scope);
  }

  @Override
  public void contributeToCache(Table result, Set<CompiledMeasure> measures, QueryCacheKey scope) {
    getCache(scope).contributeToCache(result, measures, scope);
  }

  @Override
  public void contributeToResult(Table result, Set<CompiledMeasure> measures, QueryCacheKey scope) {
    getCache(scope).contributeToResult(result, measures, scope);
  }

  @Override
  public void clear(SquashQLUser user) {
    this.cacheByUser.remove(user(user));
  }

  @Override
  public void clear() {
    this.cacheByUser.clear();
  }

  @Override
  public CacheStatsDto stats(SquashQLUser user) {
    return this.cacheByUser.computeIfAbsent(user(user), u -> this.cacheSupplier.get()).stats();
  }

  @Override
  public String getHistogram() {
    List<int[]> histograms = new ArrayList<>();
    this.cacheByUser.forEach((user, cache) -> histograms.add(cache.getHistogramInteger()));

    int[] counts = new int[CaffeineQueryCache.histogram.length];
    for (int i = 0; i < CaffeineQueryCache.histogram.length; i++) {
      int j = i;
      histograms.forEach(h -> counts[j] += h[j]);
    }
    return CaffeineQueryCache.getHistogramHumanRepresentation(counts);
  }
}
