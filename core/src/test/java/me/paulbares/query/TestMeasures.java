package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMeasures {

  @Test
  void testAggregatedMeasure() {
    Assertions.assertThatThrownBy(() -> new AggregatedMeasure("null", null, AggregationFunction.SUM))
            .isInstanceOf(NullPointerException.class);
    Assertions.assertThatThrownBy(() -> new AggregatedMeasure("null", "field", null))
            .isInstanceOf(NullPointerException.class);
    Assertions.assertThatThrownBy(() -> new AggregatedMeasure(null, "field", AggregationFunction.SUM))
            .isInstanceOf(NullPointerException.class);
  }
}
