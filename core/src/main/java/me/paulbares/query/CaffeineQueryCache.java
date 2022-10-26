package me.paulbares.query;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.ConcurrentStatsCounter;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.store.TypedField;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

public class CaffeineQueryCache implements QueryCache {

  public static final int MAX_SIZE = 32;

  private volatile StatsCounter scopeCounter = new ConcurrentStatsCounter();
  private volatile StatsCounter measureCounter = new ConcurrentStatsCounter();

  /**
   * The cached results.
   */
  private final Cache<PrefetchQueryScope, Table> results;

  public CaffeineQueryCache() {
    this(MAX_SIZE, (a, b, c) -> {
    });
  }

  public CaffeineQueryCache(int maxSize, RemovalListener<PrefetchQueryScope, Table> evictionListener) {
    this.results = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(Duration.ofMinutes(5))
            .recordStats(() -> this.scopeCounter)
            // Use removalListener and not evictionListener because evictionListener is called before updating the stats
            .removalListener(evictionListener)
            .build();
  }

  @Override
  public ColumnarTable createRawResult(PrefetchQueryScope scope) {
    Set<Field> columns = scope.columns();
    List<TypedField> headers = new ArrayList<>(columns.stream().map(c -> new TypedField(c.name(), String.class)).toList());
    headers.add(new TypedField(CountMeasure.ALIAS, long.class));

    List<List<Object>> values = new ArrayList<>();
    Table table = this.results.getIfPresent(scope);
    for (Field f : columns) {
      values.add(table.getColumnValues(f.name()));
    }
    values.add(table.getAggregateValues(CountMeasure.INSTANCE));
    return new ColumnarTable(
            headers,
            Collections.singletonList(CountMeasure.INSTANCE),
            new int[]{headers.size() - 1},
            IntStream.range(0, headers.size() - 1).toArray(),
            values);
  }

  @Override
  public boolean contains(Measure measure, PrefetchQueryScope scope) {
    Table table = this.results.getIfPresent(scope);
    if (table != null) {
      return table.measures().indexOf(measure) >= 0;
    }
    return false;
  }

  @Override
  public void contributeToCache(Table result, Set<Measure> measures, PrefetchQueryScope scope) {
    Table cache = this.results.get(scope, s -> {
      this.measureCounter.recordMisses(measures.size());
      return result;
    });

    for (Measure measure : measures) {
      if (cache.measures().indexOf(measure) < 0) {
        // Not in the previousResult, add it.
        List<Object> aggregateValues = result.getAggregateValues(measure);
        TypedField field = result.getField(measure);
        cache.addAggregates(field, measure, aggregateValues);
        this.measureCounter.recordMisses(1);
      }
    }
  }

  @Override
  public void contributeToResult(Table result, Set<Measure> measures, PrefetchQueryScope scope) {
    if (measures.isEmpty()) {
      return;
    }
    Table cacheResult = this.results.getIfPresent(scope);
    for (Measure measure : measures) {
      List<Object> aggregateValues = cacheResult.getAggregateValues(measure);
      TypedField field = cacheResult.getField(measure);
      result.addAggregates(field, measure, aggregateValues);
      this.measureCounter.recordHits(1);
    }
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
  public void clear() {
    this.results.invalidateAll();
    this.measureCounter = new ConcurrentStatsCounter();
    this.scopeCounter = new ConcurrentStatsCounter();
  }
}
