package me.paulbares.query.agg;

import me.paulbares.query.AggregatedMeasure;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CopyAggregator implements Aggregator {

  private final ObjectIntHashMap<AggregatedMeasure> measureByIndex;
  private final List<Field> fields;
  private final Map<AggregatedMeasure, Field> fieldByAggMeasure;
  private final List<List<Object>> aggregates;

  public CopyAggregator(List<AggregatedMeasure> measures, List<Field> fields) {
    this.fields = fields;
    this.aggregates = new ArrayList<>();
    this.measureByIndex = new ObjectIntHashMap<>();
    this.fieldByAggMeasure = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      AggregatedMeasure m = measures.get(i);
      this.measureByIndex.put(m, i);
      Field f = fields.get(i);
      this.fieldByAggMeasure.put(m, f);
    }
  }

  @Override
  public void aggregate(int rowIndex, List<Object> values) {
    if (rowIndex >= this.aggregates.size()) {
      this.aggregates.add(new ArrayList<>(values));
    } else {
      throw new RuntimeException("operation not supported");
    }
  }

  public Object getAggregate(AggregatedMeasure measure, int rowIndex) {
    return this.aggregates.get(rowIndex).get(this.measureByIndex.get(measure));
  }

  public Field getField(AggregatedMeasure measure) {
    return this.fieldByAggMeasure.get(measure);
  }

  public List<Object> getAggregates(int rowIndex) {
    return this.aggregates.get(rowIndex);
  }
}
