package me.paulbares.query;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.CacheStats;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Field;

import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class QueryCache {

  /**
   * The cached results.
   */
  private final Cache<QueryScope, Table> results = Caffeine.newBuilder()
          .maximumSize(32)
          .expireAfterWrite(Duration.ofMinutes(5))
          .build();

  // Statistics
  static final Supplier<AbstractCache.SimpleStatsCounter> CACHE_STATS_COUNTER = () -> new AbstractCache.SimpleStatsCounter();
  private volatile AbstractCache.SimpleStatsCounter counter = CACHE_STATS_COUNTER.get();

  public ColumnarTable createRawResult(QueryScope scope) {
    List<Field> headers = new ArrayList<>(scope.columns);
    headers.add(new Field(CountMeasure.ALIAS, long.class));

    List<List<Object>> values = new ArrayList<>();
    Table table = this.results.getIfPresent(scope);
    for (Field f : scope.columns) {
      values.add(table.getColumnValues(f.name()));
    }
    values.add(table.getAggregateValues(CountMeasure.INSTANCE));
    this.counter.recordHits(1);
    return new ColumnarTable(
            headers,
            Collections.singletonList(CountMeasure.INSTANCE),
            new int[]{headers.size() - 1},
            IntStream.range(0, headers.size() - 1).toArray(),
            values);
  }

  public boolean contains(Measure measure, QueryScope scope) {
    Table table = this.results.getIfPresent(scope);
    if (table != null) {
      return table.measures().indexOf(measure) >= 0;
    }
    return false;
  }

  public void contributeToCache(Table result, Set<Measure> measures, QueryScope scope) {
    Table cache = this.results.get(scope, s -> {
      this.counter.recordMisses(measures.size());
      return result;
    });

    for (Measure measure : measures) {
      if (cache.measures().indexOf(measure) < 0) {
        // Not in the previousResult, add it.
        List<Object> aggregateValues = result.getAggregateValues(measure);
        Field field = result.getField(measure);
        cache.addAggregates(field, measure, aggregateValues);
        this.counter.recordMisses(1);
      }
    }
  }

  public void contributeToResult(Table result, Set<Measure> measures, QueryScope scope) {
    if (measures.isEmpty()) {
      return;
    }
    Table cacheResult = this.results.getIfPresent(scope);
    for (Measure measure : measures) {
      List<Object> aggregateValues = cacheResult.getAggregateValues(measure);
      Field field = cacheResult.getField(measure);
      result.addAggregates(field, measure, aggregateValues);
      this.counter.recordHits(1);
    }
  }

  public CacheStats stats() {
    return this.counter.snapshot();
  }

  public void clear() {
    this.results.invalidateAll();
    this.counter = CACHE_STATS_COUNTER.get();
  }

  public record QueryScope(TableDto tableDto, Set<Field> columns, Map<String, ConditionDto> conditions) {
  }
}
