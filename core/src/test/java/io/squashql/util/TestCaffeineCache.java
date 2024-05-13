package io.squashql.util;

import io.squashql.query.Header;
import io.squashql.query.cache.CaffeineQueryCache;
import io.squashql.query.cache.QueryCache;
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
  void testHistogram() {
    CaffeineQueryCache cache = new CaffeineQueryCache();
    cache.contributeToCache(new FakeTable(1, 2), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(100, 20), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(100, 20), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(100, 20), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(100, 200), Set.of(), newKey());
    cache.contributeToCache(new FakeTable(1000, 2000), Set.of(), newKey());

    String histogram = cache.getHistogram();
    String[] lines = histogram.split(System.lineSeparator());
    Assertions.assertThat(lines[1]).isEqualTo("[100:1],[1000:3],[10000:1],[50000:0],[100000:0],[200000:0],[500000:0],[1000000:1]");
  }

  private static QueryCache.QueryCacheKey newKey() {
    return new QueryCache.QueryCacheKey(
            new QueryScope(new MaterializedTable("table-" + s.getAsInt(), List.of()), List.of(), null, null, List.of(), Set.of(), List.of(), List.of(), 10),
            null);
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
