package io.squashql.query;

import io.squashql.query.dto.CacheStatsDto;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class GlobalCache implements QueryCache {

  private static final SquashQLUser NULL_USER = new SquashQLUser() {
  };

  protected final Map<SquashQLUser, CaffeineQueryCache> cacheByUser = new ConcurrentHashMap<>();

  protected final Supplier<CaffeineQueryCache> cacheSupplier;

  public GlobalCache(Supplier<CaffeineQueryCache> cacheSupplier) {
    this.cacheSupplier = cacheSupplier;
  }

  private static SquashQLUser user(SquashQLUser user) {
    return user == null ? NULL_USER : user;
  }

  protected CaffeineQueryCache getCache(PrefetchQueryScope scope) {
    return this.cacheByUser.computeIfAbsent(user(scope.user()), u -> this.cacheSupplier.get());
  }

  @Override
  public ColumnarTable createRawResult(PrefetchQueryScope scope) {
    return getCache(scope).createRawResult(scope);
  }

  @Override
  public boolean contains(Measure measure, PrefetchQueryScope scope) {
    return getCache(scope).contains(measure, scope);
  }

  @Override
  public void contributeToCache(Table result, Set<Measure> measures, PrefetchQueryScope scope) {
    getCache(scope).contributeToCache(result, measures, scope);
  }

  @Override
  public void contributeToResult(Table result, Set<Measure> measures, PrefetchQueryScope scope) {
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
}
