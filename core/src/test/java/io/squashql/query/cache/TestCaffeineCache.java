package io.squashql.query.cache;

import io.squashql.query.BasicUser;
import io.squashql.query.Header;
import io.squashql.query.SquashQLUser;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.compiled.MaterializedTable;
import io.squashql.query.database.QueryScope;
import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.table.Table;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

public class TestCaffeineCache {

  private static final IntSupplier s = new AtomicInteger()::getAndIncrement;

  @Test
  void testHistogramCaffeineQueryCache() {
    CaffeineQueryCache cache = new CaffeineQueryCache();
    cache.contributeToCache(new FakeTable(1, 2), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(10, 2), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(100, 20), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(200, 1), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(100, 20), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(100, 20), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(100, 200), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(1000, 2000), Set.of(), newKey());

    String histogram = cache.getHistogram();
    Assertions.assertThat(histogram).isEqualTo("[0-100:2],[100-1000:1],[1000-10000:3],[10000-50000:1],[50000-100000:0],[100000-200000:0],[200000-500000:0],[500000-1000000:0],[1000000<:1]");
  }

  @Test
  void testHistogramGlobalCache() {
    GlobalCache cache = new GlobalCache(CaffeineQueryCache::new);

    BasicUser paul = new BasicUser("paul");
    BasicUser peter = new BasicUser("peter");
    cache.contributeToCache(new FakeTable(1, 2), Set.of(), newKey(paul)); // size 2
    cache.contributeToCache(new FakeTable(10, 2), Set.of(), newKey(peter)); // 20
    cache.contributeToCache(new FakeTable(100, 20), Set.of(), newKey(paul)); // 2000
    cache.contributeToCache(new FakeTable(200, 1), Set.of(), newKey(paul)); // 200
    cache.contributeToCache(new FakeTable(100, 20), Set.of(), newKey(peter)); // 2000

    String histogram = cache.getHistogram();
    Assertions.assertThat(histogram).isEqualTo("[0-100:2],[100-1000:1],[1000-10000:2],[10000-50000:0],[50000-100000:0],[100000-200000:0],[200000-500000:0],[500000-1000000:0],[1000000<:0]");
  }

  private static QueryCache.QueryCacheKey newKey() {
    return newKey(GlobalCache.user(null));
  }

  private static QueryCache.QueryCacheKey newKey(SquashQLUser user) {
    return new QueryCache.QueryCacheKey(
            new QueryScope(new MaterializedTable("table-" + s.getAsInt(), List.of()), List.of(), null, null, List.of(), Set.of(), List.of(), List.of(), 10),
            user);
  }

  private static class FakeTable implements Table {

    private final int count;
    private final List<Header> headers;

    private FakeTable(int count, int nbOfHeaders) {
      this.count = count;
      this.headers = IntStream.range(0, nbOfHeaders).mapToObj(i -> new Header("name-" + i, String.class, false)).toList();
    }

    @Override
    public ObjectArrayDictionary pointDictionary() {
      return null;
    }

    @Override
    public List<Header> headers() {
      return this.headers;
    }

    @Override
    public Set<CompiledMeasure> measures() {
      return Set.of();
    }

    @Override
    public void addAggregates(Header header, CompiledMeasure measure, List<Object> values) {

    }

    @Override
    public void transferAggregates(Table from, CompiledMeasure measure) {

    }

    @Override
    public int count() {
      return this.count;
    }

    @Override
    public Iterator<List<Object>> iterator() {
      return null;
    }
  }
}
