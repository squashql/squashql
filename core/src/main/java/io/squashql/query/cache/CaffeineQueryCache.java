package io.squashql.query.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.ConcurrentStatsCounter;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.util.concurrent.Striped;
import io.squashql.query.CountMeasure;
import io.squashql.query.Header;
import io.squashql.query.SquashQLUser;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static io.squashql.query.compiled.CompiledAggregatedMeasure.COMPILED_COUNT;

public class CaffeineQueryCache implements QueryCache {

  public static final int MAX_SIZE;
  public static final int EXPIRATION_DURATION; // in minutes

  static {
    String size = System.getProperty("io.squashql.cache.size", Integer.toString(32));
    MAX_SIZE = Integer.parseInt(size);
    String duration = System.getProperty("io.squashql.cache.duration", Integer.toString(5));
    EXPIRATION_DURATION = Integer.parseInt(duration);
  }

  private volatile StatsCounter scopeCounter = new ConcurrentStatsCounter();
  private volatile StatsCounter measureCounter = new ConcurrentStatsCounter();
  private final Striped<ReadWriteLock> lock;

  /**
   * The cached results.
   */
  private final Cache<QueryCacheKey, DelegateTable> results;

  public CaffeineQueryCache() {
    this(MAX_SIZE, (a, b, c) -> {
    });
  }

  public CaffeineQueryCache(int maxSize, RemovalListener<QueryCacheKey, Table> evictionListener) {
    this.results = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(Duration.ofMinutes(EXPIRATION_DURATION))
            .recordStats(() -> this.scopeCounter)
            // Use removalListener and not evictionListener because evictionListener is called before updating the stats
            .removalListener(evictionListener)
            .build();
    this.lock = Striped.readWriteLock(Runtime.getRuntime().availableProcessors() * 4);
  }

  @Override
  public ColumnarTable createRawResult(QueryCacheKey key) {
    Set<TypedField> columns = new LinkedHashSet<>(key.scope().columns());
    List<Header> headers = new ArrayList<>(columns.stream().map(column -> new Header(SqlUtils.squashqlExpression(column), column.type(), false)).toList());
    headers.add(new Header(CountMeasure.ALIAS, long.class, true));

    List<List<Object>> values = new ArrayList<>();
    Table table = this.results.getIfPresent(key);
    return executeRead(table, () -> {
      for (TypedField f : columns) {
        values.add(table.getColumnValues(SqlUtils.squashqlExpression(f)));
      }
      values.add(table.getColumnValues(COMPILED_COUNT.alias()));
      return new ColumnarTable(headers, Collections.singleton(COMPILED_COUNT), values);
    });
  }

  @Override
  public boolean contains(CompiledMeasure measure, QueryCacheKey scope) {
    Table table = this.results.getIfPresent(scope);
    if (table != null) {
      return executeRead(table, () -> table.measures().contains(measure));
    }
    return false;
  }

  @Override
  public void contributeToCache(Table result, Set<CompiledMeasure> measures, QueryCacheKey scope) {
    Table cache = this.results.get(scope, s -> {
      this.measureCounter.recordMisses(measures.size());
      if (result instanceof ColumnarTable ct) {
        return new DelegateTable(ct.copy());
      } else {
        return new DelegateTable(result);
      }
    });

    executeWrite(cache, () -> {
      for (CompiledMeasure measure : measures) {
        if (!cache.measures().contains(measure)) {
          // Not in the previousResult, add it.
          cache.transferAggregates(result, measure);
          this.measureCounter.recordMisses(1);
        }
      }
    });
  }

  @Override
  public void contributeToResult(Table result, Set<CompiledMeasure> measures, QueryCacheKey scope) {
    if (measures.isEmpty()) {
      return;
    }
    Table cache = this.results.getIfPresent(scope);
    if (cache != null) {
      executeRead(cache, () -> {
        for (CompiledMeasure measure : measures) {
          result.transferAggregates(cache, measure);
          this.measureCounter.recordHits(1);
        }
        return null;
      });
    }
  }

  @Override
  public CacheStatsDto stats(SquashQLUser user) {
    // Not supposed to be called.
    throw new IllegalStateException();
  }

  public CacheStatsDto stats() {
    CacheStats snapshot = this.measureCounter.snapshot();
    CacheStats of = CacheStats.of(
            snapshot.hitCount(),
            snapshot.missCount(),
            0,
            0,
            0,
            this.scopeCounter.snapshot().evictionCount(),
            0);
    return new CacheStatsDto(of.hitCount(), of.missCount(), this.scopeCounter.snapshot().evictionCount());
  }

  @Override
  public void clear(SquashQLUser user) {
    // Not supposed to be called.
    throw new IllegalStateException();
  }

  @Override
  public void clear() {
    this.results.invalidateAll();
    this.measureCounter = new ConcurrentStatsCounter();
    this.scopeCounter = new ConcurrentStatsCounter();
  }

  private <V> V executeRead(Table t, Callable<V> callable) {
    Lock l = this.lock.get(t).readLock();
    try {
      l.lock();
      return callable.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      l.unlock();
    }
  }

  private void executeWrite(Table t, Runnable runnable) {
    Lock l = this.lock.get(t).writeLock();
    try {
      l.lock();
      runnable.run();
    } finally {
      l.unlock();
    }
  }

  /**
   * A wrapper around another {@link Table} to make sure this implementation does not override {@link Object#hashCode()}
   * and {@link Object#equals(Object)} to work with the {@link Striped striped lock}.
   */
  @AllArgsConstructor
  private final class DelegateTable implements Table {

    private final Table underlying;

    @Override
    public boolean equals(Object o) {
      return super.equals(o); // DO NOT CHANGE IT
    }

    @Override
    public int hashCode() {
      return super.hashCode(); // DO NOT CHANGE IT
    }

    @Override
    public List<Header> headers() {
      return this.underlying.headers();
    }

    @Override
    public Set<CompiledMeasure> measures() {
      return this.underlying.measures();
    }

    @Override
    public void transferAggregates(Table from, CompiledMeasure measure) {
      this.underlying.transferAggregates(from, measure);
    }

    @Override
    public ObjectArrayDictionary pointDictionary() {
      return this.underlying.pointDictionary();
    }

    @Override
    public Iterator<List<Object>> iterator() {
      return this.underlying.iterator();
    }

    @Override
    public void addAggregates(Header header, CompiledMeasure measure, List<Object> values) {
      this.underlying.addAggregates(header, measure, values);
    }

    @Override
    public int count() {
      return this.underlying.count();
    }
  }

  public String getHistogram() {
    int[] histogram = new int[]{100, 1000, 10_000, 50_000, 100_000, 200_000, 500_000, 1_000_000};
    ConcurrentMap<QueryCacheKey, DelegateTable> map = this.results.asMap();
    MutableIntList l = new IntArrayList();
    for (DelegateTable value : map.values()) {
      int nbCells = executeRead(value, () -> value.count() * value.headers().size());
      l.add(nbCells);
    }

    int[] counts = getCountByHist(l, histogram);

    StringBuilder sb = new StringBuilder();
    sb
            .append("distribution (cardinality: number of cells)")
            .append(System.lineSeparator());
    for (int i = 0; i < histogram.length; i++) {
      sb
              .append("[")
              .append(histogram[i])
              .append(":")
              .append(counts[i])
              .append("]");
      if (i < histogram.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  public static int[] getCountByHist(MutableIntList l, int[] hist) {
    int[] counts = new int[hist.length];
    l.forEach(value -> {
      int i = 0;
      for (int j = 0; j < hist.length; j++) {
        if (value > hist[j]) {
          i = j;
        } else {
          break;
        }
      }
      counts[i]++;
    });
    return counts;
  }
}
