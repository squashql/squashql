package io.squashql.query.database;

import com.google.auth.oauth2.ServiceAccountCredentials;
import io.squashql.BigQueryDatastore;
import io.squashql.BigQueryServiceAccountDatastore;
import io.squashql.BigQueryUtil;
import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Table;
import io.squashql.query.dto.*;
import io.squashql.store.Field;
import io.squashql.store.Store;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.function.Function;

import static io.squashql.query.dto.JoinType.INNER;
import static io.squashql.transaction.TransactionManager.SCENARIO_FIELD_NAME;

public class TestBigQueryEngine {

  final Function<String, Field> fieldSupplier = name -> switch (name) {
    case "category" -> new Field("baseStore", name, long.class);
    case "price" -> new Field("baseStore", name, double.class);
    default -> new Field("baseStore", name, String.class);
  };

  @Test
  void testSqlGenerationWithRollup() {
    String category = "category";
    String scenario = SCENARIO_FIELD_NAME;
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fieldSupplier.apply(scenario))
            .withSelect(this.fieldSupplier.apply(category))
            .withRollup(this.fieldSupplier.apply(scenario))
            .withRollup(this.fieldSupplier.apply(category))
            .aggregatedMeasure("price.sum", "price", "sum")
            .aggregatedMeasure("price.avg", "price", "avg")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fieldSupplier.apply(SCENARIO_FIELD_NAME),
                TestBigQueryEngine.this.fieldSupplier.apply(category),
                TestBigQueryEngine.this.fieldSupplier.apply("price")
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query);
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`myProjectId.myDatasetName.baseStore`.`scenario`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`category`, " + BigQueryUtil.getNullValue(long.class) + ")," +
                    " sum(`price`) as `price.sum`, avg(`price`) as `price.avg`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`scenario`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`category`, " + BigQueryUtil.getNullValue(long.class) + "))");
  }

  @Test
  void testSqlGenerationWithPartialRollup() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fieldSupplier.apply("col1"))
            .withSelect(this.fieldSupplier.apply("col2"))
            .withSelect(this.fieldSupplier.apply("col3"))
            .withRollup(this.fieldSupplier.apply("col2"))
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fieldSupplier.apply("col1"),
                TestBigQueryEngine.this.fieldSupplier.apply("col2"),
                TestBigQueryEngine.this.fieldSupplier.apply("col3"),
                TestBigQueryEngine.this.fieldSupplier.apply("col4")
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query);
    // The order in the rollup is important to fetch the right (sub)totals
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`myProjectId.myDatasetName.baseStore`.`col1`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col2`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col3`, '___null___')," +
                    " sum(`price`) as `price.sum`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`col1`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col3`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col2`, '___null___'))");

    query = new DatabaseQuery()
            .withSelect(this.fieldSupplier.apply("col1"))
            .withSelect(this.fieldSupplier.apply("col2"))
            .withSelect(this.fieldSupplier.apply("col3"))
            .withRollup(this.fieldSupplier.apply("col3"))
            .withRollup(this.fieldSupplier.apply("col2"))
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");
    sqlStatement = bqe.createSqlStatement(query);
    // The order in the rollup is important to fetch the right (sub)totals
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`myProjectId.myDatasetName.baseStore`.`col1`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col2`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col3`, '___null___')," +
                    " sum(`price`) as `price.sum`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`col1`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col3`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col2`, '___null___'))");
  }

  @Test
  void testPartialRollup() {
    String category = "category";
    String scenario = SCENARIO_FIELD_NAME;
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fieldSupplier.apply(scenario))
            .withSelect(this.fieldSupplier.apply(category))
            .withRollup(this.fieldSupplier.apply(category))
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fieldSupplier.apply(SCENARIO_FIELD_NAME),
                TestBigQueryEngine.this.fieldSupplier.apply(category),
                TestBigQueryEngine.this.fieldSupplier.apply("price")
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query);
    // Statement is the same as full rollup because BQ does not support partial rollup
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`myProjectId.myDatasetName.baseStore`.`scenario`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`category`, -9223372036854775808)," +
                    " sum(`price`) as `price.sum`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`scenario`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`category`, -9223372036854775808))");

    List<List<Object>> values = List.of(
            new ArrayList<>(Arrays.asList(null, "main", "main", "main", "1", "1", "1")),
            new ArrayList<>(Arrays.asList(null, null, "A", "B", null, "A", "B")),
            new ArrayList<>(Arrays.asList(4d, 2d, 1d, 1d, 2d, 1d, 1d)));

    Field scenarioField = this.fieldSupplier.apply(scenario);
    Field categoryField = this.fieldSupplier.apply(category);
    ColumnarTable input = new ColumnarTable(
            List.of(new Header(SqlUtils.getFieldFullName(scenarioField), scenarioField.type(), false),
                    new Header(SqlUtils.getFieldFullName(categoryField), categoryField.type(), false),
                    new Header("price.sum", double.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            values);
    Table output = bqe.postProcessDataset(input, query);
    Assertions.assertThat(output).containsExactly(
            List.of("main", SQLTranslator.TOTAL_CELL, 2d),
            List.of("main", "A", 1d),
            List.of("main", "B", 1d),
            List.of("1", SQLTranslator.TOTAL_CELL, 2d),
            List.of("1", "A", 1d),
            List.of("1", "B", 1d));
  }

  @Test
  void testSqlGenerationWithRollupAndCte() {
    String category = "category";
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fieldSupplier.apply(category))
            .withRollup(this.fieldSupplier.apply(category))
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");
    VirtualTableDto virtual = new VirtualTableDto(
            "virtual",
            List.of("a", "b"),
            List.of(List.of(0, "0"), List.of(1, "1")));
    query.virtualTableDto = virtual;
    query.table.joins.add(new JoinDto(new TableDto(virtual.name), INNER, new JoinMappingDto("baseStore.category", virtual.name + ".a", ConditionType.EQ)));

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fieldSupplier.apply(category),
                TestBigQueryEngine.this.fieldSupplier.apply("price")
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query);
    Assertions.assertThat(sqlStatement)
            .isEqualTo("with virtual as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select coalesce(`myProjectId.myDatasetName.baseStore`.`category`, -9223372036854775808), sum(`price`) as `price.sum` " +
                    "from `myProjectId.myDatasetName.baseStore` " +
                    "inner join virtual on `myProjectId.myDatasetName.baseStore`.`category` = virtual.`a` " +
                    "group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`category`, -9223372036854775808))");
  }
}
