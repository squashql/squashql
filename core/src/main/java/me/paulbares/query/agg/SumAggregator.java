package me.paulbares.query.agg;

import me.paulbares.query.AggregatedMeasure;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SumAggregator {

  private final ObjectIntHashMap<AggregatedMeasure> measureByIndex;
  private final List<Field> fields;
  private final Map<AggregatedMeasure, Field> fieldByAggMeasure;
  private final List<List<Object>> aggregates;

  public SumAggregator(List<AggregatedMeasure> measures, List<Field> fields) {
    this.fields = fields;
    this.aggregates = new ArrayList<>();

    this.measureByIndex = new ObjectIntHashMap<>();
    this.fieldByAggMeasure = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      AggregatedMeasure m = measures.get(i);
      this.measureByIndex.put(m, i);
      Field f = fields.get(i);
      this.fieldByAggMeasure.put(m, f);
      if (!m.aggregationFunction.equals(AggregationFunction.SUM)) {
        throw new IllegalStateException("Aggregation function " + m.aggregationFunction + " not supported. Only sum is supported");
      }
      if (!isTypeSupported(f)) {
        throw new IllegalStateException("Type " + f.type() + " is not supported");
      }
    }

  }

  public void aggregate(int rowIndex, List<Object> values) {
    if (rowIndex >= this.aggregates.size()) {
      this.aggregates.add(new ArrayList<>(values));
    } else {
      List<Object> oldValues = this.aggregates.get(rowIndex);
      for (int i = 0; i < values.size(); i++) {
        Field f = this.fields.get(i);

        if (f.type().equals(double.class)) {
          oldValues.set(i, (double) oldValues.get(i) + (double) values.get(i));
        } else if (f.type().equals(long.class)) {
          oldValues.set(i, (long) oldValues.get(i) + (long) values.get(i));
        } else if (f.type().equals(int.class)) {
          oldValues.set(i, (int) oldValues.get(i) + (int) values.get(i));
        }
      }
    }
  }

  public Object getAggregate(AggregatedMeasure measure, int rowIndex) {
    return this.aggregates.get(rowIndex).get(this.measureByIndex.get(measure));
  }

  public Field getField(AggregatedMeasure measure) {
    return  this.fieldByAggMeasure.get(measure);
  }

  public List<Object> getAggregates(int rowIndex) {
    return this.aggregates.get(rowIndex);
  }

  private boolean isTypeSupported(Field field) {
    Class<?> t = field.type();
    return t.equals(double.class) || t.equals(Double.class)
            || t.equals(int.class) || t.equals(Integer.class)
            || t.equals(long.class) || t.equals(Long.class);
  }
}
