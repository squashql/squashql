package me.paulbares.jackson;

import me.paulbares.query.Measure;
import me.paulbares.query.Query;
import me.paulbares.query.context.Totals;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestQueryS13n {

  @Test
  void testRoundTrip() {
    Query query = new Query()
            .addSingleCoordinate("scenario", "s1")
            .addCoordinates("city", "paris", "london")
            .addWildcardCoordinate("ean")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum")
            .addExpressionMeasure("alias1", "firstMyExpression")
            .addExpressionMeasure("alias2", "secondMyExpression")
            .addContext(Totals.KEY, Totals.VISIBLE_BOTTOM);

    String serialize = JacksonUtil.serialize(query);
    Query deserialize = JacksonUtil.deserialize(serialize, Query.class);

    Assertions.assertThat(deserialize.coordinates).isEqualTo(query.coordinates);
    Assertions.assertThat(deserialize.measures).containsExactlyInAnyOrder(query.measures.toArray(new Measure[0]));
    Assertions.assertThat(deserialize.context).isEqualTo(query.context);
  }
}
