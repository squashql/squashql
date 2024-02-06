package io.squashql;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.*;
import io.squashql.query.builder.Query;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.PivotTableQueryMergeDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.QueryMergeDto;
import io.squashql.table.PivotTable;
import io.squashql.table.PivotTableUtils;
import io.squashql.transaction.DuckDBDataLoader;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class TestPivotTable {

  protected DuckDBDatastore datastore;
  protected DuckDBQueryEngine queryEngine;
  protected DuckDBDataLoader dl;
  protected QueryExecutor executor;

  private final String storeSpending = "store_spending";
  private final String storePopulation = "store_population";
  private final TableField city = new TableField(this.storeSpending, "city");
  private final TableField country = new TableField(this.storeSpending, "country");
  private final TableField continent = new TableField(this.storeSpending, "continent");
  private final TableField spendingCategory = new TableField(this.storeSpending, "spending category");
  private final TableField spendingSubcategory = new TableField(this.storeSpending, "spending subcategory");
  private final TableField amount = new TableField(this.storeSpending, "amount");
  private final TableField population = new TableField(this.storePopulation, "population");
  private final TableField countryPop = new TableField(this.storePopulation, "country");
  private final TableField continentPop = new TableField(this.storePopulation, "continent");

  void setup(Map<String, List<TableTypedField>> fieldsByStore, Runnable dataLoading) {
    this.datastore = new DuckDBDatastore();
    this.dl = new DuckDBDataLoader(this.datastore);
    fieldsByStore.forEach(this.dl::createOrReplaceTable);
    this.queryEngine = new DuckDBQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    dataLoading.run();
  }

  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField city = new TableTypedField(this.storeSpending, "city", String.class);
    TableTypedField country = new TableTypedField(this.storeSpending, "country", String.class);
    TableTypedField continent = new TableTypedField(this.storeSpending, "continent", String.class);
    TableTypedField spendingCategory = new TableTypedField(this.storeSpending, "spending category", String.class);
    TableTypedField spendingSubcategory = new TableTypedField(this.storeSpending, "spending subcategory", String.class);
    TableTypedField amount = new TableTypedField(this.storeSpending, "amount", double.class);
    TableTypedField population = new TableTypedField(this.storePopulation, "population", double.class);
    TableTypedField countryPop = new TableTypedField(this.storePopulation, "country", String.class);
    TableTypedField continentPop = new TableTypedField(this.storePopulation, "continent", String.class);
    return Map.of(
            this.storeSpending, List.of(city, country, continent, spendingCategory, spendingSubcategory, amount),
            this.storePopulation, List.of(countryPop, continentPop, population));
  }

  protected void loadData() {
    this.dl.load(this.storeSpending, List.of(
            new Object[]{"paris", "france", "eu", "minimum expenditure", "car", 1d},
            new Object[]{"paris", "france", "eu", "minimum expenditure", "housing", 2d},
            new Object[]{"paris", "france", "eu", "extra", "hobbies", 1d},
            new Object[]{"lyon", "france", "eu", "minimum expenditure", "car", 1d},
            new Object[]{"lyon", "france", "eu", "minimum expenditure", "housing", 2d},
            new Object[]{"lyon", "france", "eu", "extra", "hobbies", 1d},
            new Object[]{"london", "uk", "eu", "minimum expenditure", "car", 2d},
            new Object[]{"london", "uk", "eu", "minimum expenditure", "housing", 2d},
            new Object[]{"london", "uk", "eu", "extra", "hobbies", 5d},

            new Object[]{"nyc", "usa", "am", "minimum expenditure", "car", 8d},
            new Object[]{"nyc", "usa", "am", "minimum expenditure", "housing", 12d},
            new Object[]{"nyc", "usa", "am", "extra", "hobbies", 6d},

            new Object[]{"la", "usa", "am", "minimum expenditure", "car", 7d},
            new Object[]{"la", "usa", "am", "minimum expenditure", "housing", 2d},
            new Object[]{"la", "usa", "am", "extra", "hobbies", 4d}
    ));


    this.dl.load(this.storePopulation, List.of(
            new Object[]{"france", "eu", 70},
            new Object[]{"uk", "eu", 65},
            new Object[]{"usa", "am", 330}
    ));
  }

  @Test
  void testDrillingAcrossFullName() {
    setup(getFieldsByStore(), this::loadData);

    Measure amount = Functions.sum("amount", this.amount);
    Measure pop = Functions.sum("population", this.population);

    List<Measure> measuresSpending = List.of(amount);
    QueryDto query1 = Query
            .from(this.storeSpending)
            .select(List.of(this.spendingCategory.as("category"), this.continent.as("continent"), this.country.as("country")), measuresSpending)
            .build();

    QueryDto query2 = Query
            .from(this.storePopulation)
            .select(List.of(this.continentPop.as("continent"), this.countryPop.as("country")), List.of(pop))
            .build();

    QueryMergeDto queryMerge = QueryMergeDto.from(query1).join(query2, JoinType.LEFT);
    List<Field> rows = List.of(new AliasedField("continent"), new AliasedField("country"));
    List<Field> columns = List.of(this.spendingCategory.as("category"));

    PivotTable pivotTable = this.executor.executePivotQueryMerge(new PivotTableQueryMergeDto(queryMerge, rows, columns, true), null);
    List<Map<String, Object>> cells = PivotTableUtils.generateCells(pivotTable);
    String expectedCells = """
            [
                          {
                            "amount": 56.0,
                            "population": 465.0
                          },
                          {
                            "continent": "am",
                            "amount": 39.0,
                            "population": 330.0
                          },
                          {
                            "continent": "am",
                            "country": "usa",
                            "amount": 39.0,
                            "population": 330.0
                          },
                          {
                            "continent": "eu",
                            "amount": 17.0,
                            "population": 135.0
                          },
                          {
                            "continent": "eu",
                            "country": "france",
                            "amount": 8.0,
                            "population": 70.0
                          },
                          {
                            "continent": "eu",
                            "country": "uk",
                            "amount": 9.0,
                            "population": 65.0
                          },
                          {
                            "amount": 17.0,
                            "category": "extra"
                          },
                          {
                            "continent": "am",
                            "amount": 10.0,
                            "category": "extra"
                          },
                          {
                            "continent": "am",
                            "country": "usa",
                            "amount": 10.0,
                            "category": "extra"
                          },
                          {
                            "continent": "eu",
                            "amount": 7.0,
                            "category": "extra"
                          },
                          {
                            "continent": "eu",
                            "country": "france",
                            "amount": 2.0,
                            "category": "extra"
                          },
                          {
                            "continent": "eu",
                            "country": "uk",
                            "amount": 5.0,
                            "category": "extra"
                          },
                          {
                            "amount": 39.0,
                            "category": "minimum expenditure"
                          },
                          {
                            "continent": "am",
                            "amount": 29.0,
                            "category": "minimum expenditure"
                          },
                          {
                            "continent": "am",
                            "country": "usa",
                            "amount": 29.0,
                            "category": "minimum expenditure"
                          },
                          {
                            "continent": "eu",
                            "amount": 10.0,
                            "category": "minimum expenditure"
                          },
                          {
                            "continent": "eu",
                            "country": "france",
                            "amount": 6.0,
                            "category": "minimum expenditure"
                          },
                          {
                            "continent": "eu",
                            "country": "uk",
                            "amount": 4.0,
                            "category": "minimum expenditure"
                          }
                        ]
                        """;
    Assertions.assertThat(cells).isEqualTo(JacksonUtil.deserialize(expectedCells, List.class));
  }
}
