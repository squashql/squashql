package io.squashql.query.database;

import com.google.auth.oauth2.ServiceAccountCredentials;
import io.squashql.BigQueryDatastore;
import io.squashql.BigQueryServiceAccountDatastore;
import io.squashql.BigQueryUtil;
import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Table;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static io.squashql.transaction.TransactionManager.SCENARIO_FIELD_NAME;

public class TestBigQueryEngine {

  @Test
  void testSqlGenerationWithRollup() {
    String category = "category";
    String scenario = SCENARIO_FIELD_NAME;
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(scenario)
            .withSelect(category)
            .withRollup(scenario)
            .withRollup(category)
            .aggregatedMeasure("price.sum", "price", "sum")
            .aggregatedMeasure("price.avg", "price", "avg")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName");
    BigQueryEngine bqe = new BigQueryEngine(datastore) {
      @Override
      protected Function<String, Field> createFieldSupplier() {
        return name -> switch (name) {
          case SCENARIO_FIELD_NAME -> new Field(name, String.class);
          case "category" -> new Field(name, long.class);
          case "price" -> new Field(name, double.class);
          default -> throw new IllegalStateException("Unexpected value: " + name);
        };
      }
    };
    String sqlStatement = bqe.createSqlStatement(query);
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`scenario`, '___null___'), coalesce(`category`, " + BigQueryUtil.getNullValue(new Field(category, long.class)) + ")," +
                    " sum(`price`) as `price.sum`, avg(`price`) as `price.avg`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`scenario`, '___null___'), coalesce(`category`, " + BigQueryUtil.getNullValue(new Field(category, long.class)) + "))");

  }

  @Test
  void testSqlGenerationWithPartialRollup() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect("col1")
            .withSelect("col2")
            .withSelect("col3")
            .withRollup("col2")
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName");
    BigQueryEngine bqe = new BigQueryEngine(datastore) {
      @Override
      protected Function<String, Field> createFieldSupplier() {
        return name -> new Field(name, String.class);
      }
    };
    String sqlStatement = bqe.createSqlStatement(query);
    // The order in the rollup is important to fetch the right (sub)totals
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`col1`, '___null___'), coalesce(`col2`, '___null___'), coalesce(`col3`, '___null___')," +
                    " sum(`price`) as `price.sum`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`col1`, '___null___'), coalesce(`col3`, '___null___'), coalesce(`col2`, '___null___'))");

    query = new DatabaseQuery()
            .withSelect("col1")
            .withSelect("col2")
            .withSelect("col3")
            .withRollup("col3")
            .withRollup("col2")
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");
    sqlStatement = bqe.createSqlStatement(query);
    // The order in the rollup is important to fetch the right (sub)totals
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`col1`, '___null___'), coalesce(`col2`, '___null___'), coalesce(`col3`, '___null___')," +
                    " sum(`price`) as `price.sum`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`col1`, '___null___'), coalesce(`col3`, '___null___'), coalesce(`col2`, '___null___'))");
  }

  @Test
  void testPartialRollup() {
    String category = "category";
    String scenario = SCENARIO_FIELD_NAME;
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(scenario)
            .withSelect(category)
            .withRollup(category)
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName");
    BigQueryEngine bqe = new BigQueryEngine(datastore) {
      @Override
      protected Function<String, Field> createFieldSupplier() {
        return name -> new Field(name, String.class);
      }
    };
    String sqlStatement = bqe.createSqlStatement(query);
    // Statement is the same as full rollup because BQ does not support partial rollup
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`scenario`, '___null___'), coalesce(`category`, '___null___')," +
                    " sum(`price`) as `price.sum`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`scenario`, '___null___'), coalesce(`category`, '___null___'))");

    List<List<Object>> values = List.of(
            new ArrayList<>(Arrays.asList(null, "main", "main", "main", "1", "1", "1")),
            new ArrayList<>(Arrays.asList(null, null, "A", "B", null, "A", "B")),
            new ArrayList<>(Arrays.asList(4, 2, 1, 1, 2, 1, 1)));

    ColumnarTable input = new ColumnarTable(
            List.of(new Header(new Field(scenario, String.class), false),
                    new Header(new Field(category, String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            values);
    Table output = bqe.postProcessDataset(input, query);
    Assertions.assertThat(output).containsExactly(
            List.of("main", SQLTranslator.TOTAL_CELL, 2),
            List.of("main", "A", 1),
            List.of("main", "B", 1),
            List.of("1", SQLTranslator.TOTAL_CELL, 2),
            List.of("1", "A", 1),
            List.of("1", "B", 1));
  }
}
