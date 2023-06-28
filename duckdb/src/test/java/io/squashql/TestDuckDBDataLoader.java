package io.squashql;

import io.squashql.query.*;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.transaction.DuckDBDataLoader;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TestDuckDBDataLoader {

  @Test
  void testCreateAndLoadTableFromTableObject() {
    Table table = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(2, 3, 6, 5))));

    DuckDBDatastore ds = new DuckDBDatastore();
    DuckDBDataLoader loader = new DuckDBDataLoader(ds);
    loader.createOrReplaceTable("myTable", table);

    QueryExecutor executor = new QueryExecutor(new DuckDBQueryEngine(ds));
    Table result = executor.execute("select * from myTable");
    Assertions.assertThat(result).containsExactly(
            List.of("MN", "A", 2),
            List.of("MN", "B", 3),
            List.of("MDD", "A", 6),
            List.of("MDD", "C", 5));
  }
}
