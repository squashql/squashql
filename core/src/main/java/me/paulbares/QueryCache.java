package me.paulbares;

import me.paulbares.query.ColumnarTable;
import me.paulbares.query.CountMeasure;
import me.paulbares.query.Measure;
import me.paulbares.query.Table;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.store.Field;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class QueryCache {

  private final Map<QueryScope, Map<Field, List<Object>>> cache = new ConcurrentHashMap<>();
  private final Map<AggregateCacheKey, List<Object>> aggregateCache = new ConcurrentHashMap<>();
  private final Map<Measure, Field> fieldByMeasure = new ConcurrentHashMap<>();

  public ColumnarTable createResult(QueryScope scope) {
    List<Field> headers = new ArrayList<>(scope.columns);
    headers.add(new Field(CountMeasure.ALIAS, long.class));
    List<List<Object>> values = new ArrayList<>();

    int size = headers.size();
    for (int i = 0; i < size; i++) {
      if (i < size - 1) {
        Map<Field, List<Object>> valuesByField = this.cache.get(scope);
        assert valuesByField != null;
        List<Object> v = valuesByField.get(headers.get(i));
        assert v != null;
        values.add(v);
      } else {
        values.add(this.aggregateCache.get(new AggregateCacheKey(CountMeasure.INSTANCE, scope)));
      }
    }
    return new ColumnarTable(
            headers,
            Collections.singletonList(CountMeasure.INSTANCE),
            new int[]{size - 1},
            IntStream.range(0, size - 2).toArray(),
            values);
  }

  public boolean contains(Measure measure,
                          QueryScope scope) {
    AggregateCacheKey key = new AggregateCacheKey(measure, scope);
    return this.aggregateCache.containsKey(key);
  }

  public void contributeToCache(Table result, Set<Measure> measures, QueryScope scope) {
    for (Measure measure : measures) {
      List<Object> aggregateValues = result.getAggregateValues(measure);
      this.aggregateCache.put(new AggregateCacheKey(measure, scope), aggregateValues);
      this.fieldByMeasure.put(measure, result.getField(measure));
    }
    this.cache.computeIfAbsent(scope, k -> {
      Map<Field, List<Object>> points = new HashMap<>();
      for (Field column : scope.columns()) {
        List<Object> values = result.getColumn(result.index(column));
        points.put(column, values);
      }
      return points;
    });
  }

  public void contributeToResult(Table result, Set<Measure> measures, QueryScope scope) {
    for (Measure measure : measures) {
      result.addAggregates(
              this.fieldByMeasure.get(measure),
              measure,
              this.aggregateCache.get(new AggregateCacheKey(measure, scope)));
    }
  }

  public record AggregateCacheKey(Measure measure, QueryScope queryScope) {
  }

  public record QueryScope(Set<Field> columns,
                           Map<String, ConditionDto> conditions) {
  }
}
