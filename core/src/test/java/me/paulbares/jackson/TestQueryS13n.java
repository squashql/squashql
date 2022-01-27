package me.paulbares.jackson;

import me.paulbares.query.Measure;
import me.paulbares.dto.QueryDto;
import me.paulbares.query.context.Totals;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestQueryS13n {

  @Test
  void testRoundTrip() {
    QueryDto query = new QueryDto()
            .addSingleCoordinate("scenario", "s1")
            .addCoordinates("city", "paris", "london")
            .addWildcardCoordinate("ean")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum")
            .addExpressionMeasure("alias1", "firstMyExpression")
            .addExpressionMeasure("alias2", "secondMyExpression")
            .addContext(Totals.KEY, Totals.VISIBLE_BOTTOM);

    String serialize = JacksonUtil.serialize(query);
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);

    Assertions.assertThat(deserialize.coordinates).isEqualTo(query.coordinates);
    Assertions.assertThat(deserialize.measures).containsExactlyInAnyOrder(query.measures.toArray(new Measure[0]));
    Assertions.assertThat(deserialize.context).isEqualTo(query.context);
  }
}
