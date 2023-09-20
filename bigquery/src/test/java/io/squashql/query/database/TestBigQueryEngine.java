package io.squashql.query.database;

import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.dto.JoinType.INNER;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

import com.google.auth.oauth2.ServiceAccountCredentials;
import io.squashql.BigQueryDatastore;
import io.squashql.BigQueryServiceAccountDatastore;
import io.squashql.BigQueryUtil;
import io.squashql.query.AggregatedMeasure;
import io.squashql.query.Field;
import io.squashql.query.Header;
import io.squashql.query.QueryExecutor;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.JoinDto;
import io.squashql.query.dto.JoinMappingDto;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.store.Store;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBigQueryEngine {

  final Function<Field, TableTypedField> fp = field -> switch (field.name()) {
    case "category" -> new TableTypedField("baseStore", field.name(), long.class);
    case "price" -> new TableTypedField("baseStore", field.name(), double.class);
    default -> new TableTypedField("baseStore", field.name(), String.class);
  };

  @Test
  void testSqlGenerationWithRollup() {
    Field category = tableField("category");
    Field scenario = tableField(SCENARIO_FIELD_NAME);
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fp.apply(scenario))
            .withSelect(this.fp.apply(category))
            .rollup(List.of(this.fp.apply(scenario), this.fp.apply(category)))
            .aggregatedMeasure("price.sum", "price", "sum")
            .aggregatedMeasure("price.avg", "price", "avg")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fp.apply(scenario),
                TestBigQueryEngine.this.fp.apply(category),
                TestBigQueryEngine.this.fp.apply(tableField("price"))
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query, null);
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`myProjectId.myDatasetName.baseStore`.`scenario`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`category`, " + BigQueryUtil.getNullValue(long.class) + ")," +
                    " sum(`price`) as `price.sum`, avg(`price`) as `price.avg`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`scenario`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`category`, " + BigQueryUtil.getNullValue(long.class) + "))");
  }

  @Test
  void testSqlGenerationWithPartialRollup() {
    final Field col1 = tableField("col1");
    final Field col2 = tableField("col2");
    final Field col3 = tableField("col3");
    final Field col4 = tableField("col4");
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fp.apply(col1))
            .withSelect(this.fp.apply(col2))
            .withSelect(this.fp.apply(col3))
            .rollup(List.of(this.fp.apply(col2)))
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fp.apply(col1),
                TestBigQueryEngine.this.fp.apply(col2),
                TestBigQueryEngine.this.fp.apply(col3),
                TestBigQueryEngine.this.fp.apply(col4)
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query, null);
    // The order in the rollup is important to fetch the right (sub)totals
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`myProjectId.myDatasetName.baseStore`.`col1`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col2`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col3`, '___null___')," +
                    " sum(`price`) as `price.sum`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`col1`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col3`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col2`, '___null___'))");

    query = new DatabaseQuery()
            .withSelect(this.fp.apply(col1))
            .withSelect(this.fp.apply(col2))
            .withSelect(this.fp.apply(col3))
            .rollup(List.of(this.fp.apply(col3), this.fp.apply(col2)))
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");
    sqlStatement = bqe.createSqlStatement(query, null);
    // The order in the rollup is important to fetch the right (sub)totals
    Assertions.assertThat(sqlStatement)
            .isEqualTo("select coalesce(`myProjectId.myDatasetName.baseStore`.`col1`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col2`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col3`, '___null___')," +
                    " sum(`price`) as `price.sum`" +
                    " from `myProjectId.myDatasetName.baseStore`" +
                    " group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`col1`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col3`, '___null___'), coalesce(`myProjectId.myDatasetName.baseStore`.`col2`, '___null___'))");
  }

  @Test
  void testPartialRollup() {
    Field category = tableField("category");
    Field scenario = tableField(SCENARIO_FIELD_NAME);
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fp.apply(scenario))
            .withSelect(this.fp.apply(category))
            .rollup(List.of(this.fp.apply(category)))
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fp.apply(scenario),
                TestBigQueryEngine.this.fp.apply(category),
                TestBigQueryEngine.this.fp.apply(tableField("price"))
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query, null);
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

    TypedField scenarioField = this.fp.apply(scenario);
    TypedField categoryField = this.fp.apply(category);
    ColumnarTable input = new ColumnarTable(
            List.of(new Header(SqlUtils.expression(scenarioField), scenarioField.type(), false),
                    new Header(SqlUtils.expression(categoryField), categoryField.type(), false),
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
    Field category = tableField("category");
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fp.apply(category))
            .rollup(List.of(this.fp.apply(category)))
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
                TestBigQueryEngine.this.fp.apply(category),
                TestBigQueryEngine.this.fp.apply(tableField("price"))
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query, null);
    Assertions.assertThat(sqlStatement)
            .isEqualTo("with virtual as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select coalesce(`myProjectId.myDatasetName.baseStore`.`category`, -9223372036854775808), sum(`price`) as `price.sum` " +
                    "from `myProjectId.myDatasetName.baseStore` " +
                    "inner join virtual on `myProjectId.myDatasetName.baseStore`.`category` = virtual.`a` " +
                    "group by rollup(coalesce(`myProjectId.myDatasetName.baseStore`.`category`, -9223372036854775808))");
  }

  @Test
  void testSqlGenerationWithGroupingSets() {
    Field sc = tableField("spending category");
    Field ssc = tableField("spending subcategory");
    Field continent = tableField("continent");
    Field country = tableField("country");
    Field city = tableField("city");
    QueryDto queryDto = Query
            .from("baseStore")
            .select(List.of(continent, country, city, sc, ssc), List.of())
            .build();
    queryDto = QueryExecutor.prepareQuery(
            queryDto,
            new QueryExecutor.PivotTableContext(new PivotTableQueryDto(queryDto, List.of(continent, country, city), List.of(sc, ssc))));
    List<List<Field>> groupingSets = List.of(List.of(),
            List.of(sc),
            List.of(sc, ssc),
            List.of(continent, sc),
            List.of(continent, sc, ssc),
            List.of(continent, country, sc),
            List.of(continent, country, sc, ssc),
            List.of(continent),
            List.of(continent, country),
            List.of(continent, country, city),
            List.of(continent, country, city, sc),
            List.of(continent, country, city, sc, ssc));
    // Make sure the list of grouping sets makes sense and correspond to the real world use case (pivot table)
    Assertions.assertThat(queryDto.groupingSets).containsExactlyInAnyOrderElementsOf(groupingSets);

    List<List<TypedField>> gp = new ArrayList<>();
    for (List<Field> groupingSet : groupingSets) {
      List<TypedField> l = new ArrayList<>();
      gp.add(l);
      if (groupingSet.isEmpty()) {
        continue; // GT
      }
      for (Field s : groupingSet) {
        l.add(this.fp.apply(s));
      }
    }

    DatabaseQuery query = new DatabaseQuery()
            .withSelect(this.fp.apply(continent))
            .withSelect(this.fp.apply(country))
            .withSelect(this.fp.apply(city))
            .withSelect(this.fp.apply(sc))
            .withSelect(this.fp.apply(ssc))
            .groupingSets(gp)
            .aggregatedMeasure("count", "*", "count")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "pid", "ds") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fp.apply(continent),
                TestBigQueryEngine.this.fp.apply(country),
                TestBigQueryEngine.this.fp.apply(city),
                TestBigQueryEngine.this.fp.apply(sc),
                TestBigQueryEngine.this.fp.apply(ssc))
        ));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    List<Field> rows = tableFields(List.of("continent", "country", "city"));
    List<Field> columns = tableFields(List.of("spending category", "spending subcategory"));
    QueryExecutor.PivotTableContext context = new QueryExecutor.PivotTableContext(new PivotTableQueryDto(queryDto, rows, columns));
    context.init(this.fp::apply);
    String sqlStatement = bqe.createSqlStatement(query, context);

    String continentCol = "coalesce(`pid.ds.baseStore`.`continent`, '___null___')";
    String countryCol = "coalesce(`pid.ds.baseStore`.`country`, '___null___')";
    String cityCol = "coalesce(`pid.ds.baseStore`.`city`, '___null___')";
    String scCol = "coalesce(`pid.ds.baseStore`.`spending category`, '___null___')";
    String sscCol = "coalesce(`pid.ds.baseStore`.`spending subcategory`, '___null___')";
    String base = "select " +
            continentCol + ", " +
            countryCol + ", " +
            cityCol + ", " +
            scCol + ", " +
            sscCol + ", " +
            "count(*) as `count` from `pid.ds.baseStore` group by";
    String sql = String.format(
            "%1$s rollup(%2$s, %3$s, %4$s, %5$s, %6$s) " +
                    "union distinct " +
                    "%1$s rollup(%5$s, %6$s, %2$s, %3$s, %4$s) " +
                    "union distinct " +
                    "%1$s rollup(%2$s, %5$s, %6$s, %3$s, %4$s) " +
                    "union distinct " +
                    "%1$s rollup(%2$s, %3$s, %5$s, %6$s, %4$s)",
            base, continentCol, countryCol, cityCol, scCol, sscCol);
    Assertions.assertThat(sqlStatement).isEqualTo(sql);
  }

  @Test
  void testSqlGenerationWithDateFunctions() {
    TableTypedField dateField = new TableTypedField("baseStore", "date", LocalDate.class);
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(new FunctionTypedField(dateField, "YEAR"))
            .withSelect(new FunctionTypedField(dateField, "QUARTER"))
            .withSelect(new FunctionTypedField(dateField, "MONTH"))
            .aggregatedMeasure("price.sum", "price", "sum")
            .table("baseStore");

    BigQueryDatastore datastore = new BigQueryServiceAccountDatastore(Mockito.mock(ServiceAccountCredentials.class), "myProjectId", "myDatasetName") {
      @Override
      public Map<String, Store> storesByName() {
        return Map.of("baseStore", new Store("baseStore", List.of(
                TestBigQueryEngine.this.fp.apply(tableField(SCENARIO_FIELD_NAME))
        )));
      }
    };
    BigQueryEngine bqe = new BigQueryEngine(datastore);
    String sqlStatement = bqe.createSqlStatement(query, null);
    System.out.println(sqlStatement);
    String sql = "select " +
            "EXTRACT(YEAR FROM `myProjectId.myDatasetName.baseStore`.`date`), " +
            "EXTRACT(QUARTER FROM `myProjectId.myDatasetName.baseStore`.`date`), " +
            "EXTRACT(MONTH FROM `myProjectId.myDatasetName.baseStore`.`date`), sum(`price`) as `price.sum` " +
            "from `myProjectId.myDatasetName.baseStore` group by " +
            "EXTRACT(YEAR FROM `myProjectId.myDatasetName.baseStore`.`date`), " +
            "EXTRACT(QUARTER FROM `myProjectId.myDatasetName.baseStore`.`date`), " +
            "EXTRACT(MONTH FROM `myProjectId.myDatasetName.baseStore`.`date`)";
    Assertions.assertThat(sqlStatement).isEqualTo(sql);
  }
}
