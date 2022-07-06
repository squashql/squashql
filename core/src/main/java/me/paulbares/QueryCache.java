package me.paulbares;

import com.google.common.cache.AbstractCache;
import com.google.common.cache.CacheStats;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.CountMeasure;
import me.paulbares.query.Measure;
import me.paulbares.query.Table;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Field;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class QueryCache {

  private final Map<QueryScope, Map<Field, List<Object>>> cache = new ConcurrentHashMap<>();
  private final Map<Measure, Field> fieldByMeasure = new ConcurrentHashMap<>();

  static final Supplier<AbstractCache.SimpleStatsCounter> CACHE_STATS_COUNTER = () -> new AbstractCache.SimpleStatsCounter();
  private volatile AbstractCache.SimpleStatsCounter counter = CACHE_STATS_COUNTER.get();

  public ColumnarTable createRawResult(QueryScope scope) {
    List<Field> headers = new ArrayList<>(scope.columns);
    headers.add(new Field(CountMeasure.ALIAS, long.class));
    List<List<Object>> values = new ArrayList<>();

    int size = headers.size();
    for (int i = 0; i < size; i++) {
      Map<Field, List<Object>> valuesByField = this.cache.get(scope);
      assert valuesByField != null;
      List<Object> v = valuesByField.get(headers.get(i));
      assert v != null;
      values.add(v);
    }
    this.counter.recordHits(1);
    return new ColumnarTable(
            headers,
            Collections.singletonList(CountMeasure.INSTANCE),
            new int[]{size - 1},
            IntStream.range(0, size - 1).toArray(),
            values);
  }

  public boolean contains(Measure measure, QueryScope scope) {
    Map<Field, List<Object>> valuesByField = this.cache.get(scope);
    if (valuesByField != null) {
      Field field = this.fieldByMeasure.get(measure);
      return field != null && valuesByField.containsKey(field);
    }
    return false;
  }

  public void contributeToCache(Table result, Set<Measure> measures, QueryScope scope) {
    this.cache.compute(scope, (k, v) -> {
      Map<Field, List<Object>> points = v == null ? new HashMap<>() : v;
      for (Field column : scope.columns()) {
        List<Object> values = result.getColumn(result.index(column));
        points.put(column, values);
      }
      for (Measure measure : measures) {
        Field field = result.getField(measure);
        points.put(result.getField(measure), result.getAggregateValues(measure));
        this.fieldByMeasure.put(measure, field);
        this.counter.recordMisses(1);
      }
      return points;
    });
  }

  public void contributeToResult(Table result, Set<Measure> measures, QueryScope scope) {
    for (Measure measure : measures) {
      Field field = this.fieldByMeasure.get(measure);
      result.addAggregates(field, measure, this.cache.get(scope).get(field));
      this.counter.recordHits(1);
    }
  }

  public CacheStats stats() {
    return this.counter.snapshot();
  }

  public void clear() {
    this.fieldByMeasure.clear();
    this.cache.clear();
    this.counter = CACHE_STATS_COUNTER.get();
  }

  public record QueryScope(TableDto tableDto, Set<Field> columns, Map<String, ConditionDto> conditions) {
  }
}
