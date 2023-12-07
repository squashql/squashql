package io.squashql.table;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.Header;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.squashql.table.ATestMergeTables.orderRows;

public class TestTable {

  @Test
  void testTransferAggregates() {
    Header pop = new Header("population.avg", double.class, true);
    Header city = new Header("city", String.class, false);
    Header country = new Header("country", String.class, false);
    AggregatedMeasure popAvg = new AggregatedMeasure("population.avg", "population", "avg");
    ColumnarTable table = new ColumnarTable(
            List.of(country, city, pop),
            Set.of(popAvg),
            List.of(
                    Arrays.asList("france", "france", "spain", "spain"),
                    Arrays.asList("paris", "toulouse", "madrid", "barcelona"),
                    Arrays.asList(1d, 2d, 3d, 4d)));
    // Make sure the order of rows in table 2 is different.
    Header emission = new Header("co2emission.avg", double.class, true);
    AggregatedMeasure emissionAvg = new AggregatedMeasure("co2emission.avg", "co2emission", "avg");
    ColumnarTable from = new ColumnarTable(
            List.of(country, city, emission),
            Set.of(emissionAvg),
            List.of(
                    Arrays.asList("spain", "spain", "france", "france"),
                    Arrays.asList("madrid", "barcelona", "paris", "toulouse"),
                    Arrays.asList(0.1, 0.2, 0.3, 0.4)));

    Table result = new ColumnarTable(
            List.of(country, city, pop, emission),
            Set.of(popAvg,
                    emissionAvg),
            List.of(
                    Arrays.asList("france", "france", "spain", "spain"),
                    Arrays.asList("paris", "toulouse", "madrid", "barcelona"),
                    Arrays.asList(1d, 2d, 3d, 4d),
                    Arrays.asList(0.3d, 0.4d, 0.1d, 0.2d)));

    table.transferAggregates(from, emissionAvg);
    Assertions.assertThat(orderRows(table)).containsExactlyInAnyOrderElementsOf(orderRows(result));
  }
}
