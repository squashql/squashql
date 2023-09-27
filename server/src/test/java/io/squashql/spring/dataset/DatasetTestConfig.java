package io.squashql.spring.dataset;

import com.google.common.collect.ImmutableList;
import io.squashql.DuckDBDatastore;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.AggregatedMeasure;
import io.squashql.query.Header;
import io.squashql.query.QueryExecutor;
import io.squashql.query.SquashQLUser;
import io.squashql.query.builder.Query;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.SimpleTableDto;
import io.squashql.store.Store;
import io.squashql.table.PivotTable;
import io.squashql.transaction.DuckDBDataLoader;
import io.squashql.type.TableTypedField;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.squashql.query.TableField.tableFields;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

@TestConfiguration
public class DatasetTestConfig {

  @Bean
  public DuckDBQueryEngine queryEngine() {
    return new DuckDBQueryEngine(createTestDatastoreWithData());
  }

  /**
   * Display the result of the query in a pivot table accessible in the browser at this address http://localhost:8080.
   */
  @Bean
  public void displayPivotTable() {
    QueryExecutor queryExecutor = new QueryExecutor(queryEngine());
    QueryDto query = Query.from("our_prices")
            .select(tableFields(List.of("ean", "pdv", "scenario")), List.of(new AggregatedMeasure("count", "*", "count")))
            .build();
    PivotTable pt = queryExecutor.execute(new PivotTableQueryDto(query, tableFields(List.of("pdv", "ean")), tableFields(List.of("scenario"))));
    pt.show();
    toJson(pt);
  }

  /**
   * Adapt to antvis/s2 format. See examples https://s2.antv.vision/en/examples/basic/pivot/#grid.
   * <p>
   * +------------------------+--------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+
   * |               scenario |     scenario |          Grand Total |               MDD up |        MN & MDD down |          MN & MDD up |                MN up |                 base |
   * |                    pdv |          ean | _contributors_count_ | _contributors_count_ | _contributors_count_ | _contributors_count_ | _contributors_count_ | _contributors_count_ |
   * +------------------------+--------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+
   * |            Grand Total |  Grand Total |                   20 |                    4 |                    4 |                    4 |                    4 |                    4 |
   * |              ITM Balma |        Total |                   10 |                    2 |                    2 |                    2 |                    2 |                    2 |
   * | ITM Toulouse and Drive |        Total |                   10 |                    2 |                    2 |                    2 |                    2 |                    2 |
   * |              ITM Balma | ITMella 250g |                    5 |                    1 |                    1 |                    1 |                    1 |                    1 |
   * | ITM Toulouse and Drive | ITMella 250g |                    5 |                    1 |                    1 |                    1 |                    1 |                    1 |
   * |              ITM Balma | Nutella 250g |                    5 |                    1 |                    1 |                    1 |                    1 |                    1 |
   * | ITM Toulouse and Drive | Nutella 250g |                    5 |                    1 |                    1 |                    1 |                    1 |                    1 |
   * +------------------------+--------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+
   */
  public static void toJson(PivotTable pivotTable) {
    List<String> list = pivotTable.table.headers().stream().map(Header::name).toList();

    SimpleTableDto simpleTable = SimpleTableDto.builder()
            .rows(ImmutableList.copyOf(pivotTable.table.iterator()))
            .columns(list)
            .build();

    Map<String, Object> data = Map.of("rows", pivotTable.rows, "columns", pivotTable.columns, "values", pivotTable.values, "table", simpleTable);
    String encodedString = Base64.getEncoder().encodeToString(JacksonUtil.serialize(data).getBytes(StandardCharsets.UTF_8));
    System.out.println("http://localhost:8080?data=" + encodedString);
  }

  public static final AtomicReference<SquashQLUser> squashQLUserSupplier = new AtomicReference<>();

  @Bean
  public Supplier<SquashQLUser> squashQLUserSupplier() {
    return () -> squashQLUserSupplier.get();
  }

  public static DuckDBDatastore createTestDatastoreWithData() {
    TableTypedField ean = new TableTypedField("our_prices", "ean", String.class);
    TableTypedField pdv = new TableTypedField("our_prices", "pdv", String.class);
    TableTypedField price = new TableTypedField("our_prices", "price", double.class);
    TableTypedField qty = new TableTypedField("our_prices", "quantity", int.class);
    TableTypedField capdv = new TableTypedField("our_prices", "capdv", double.class);

    TableTypedField compEan = new TableTypedField("their_prices", "competitor_ean", String.class);
    TableTypedField compConcurrentPdv = new TableTypedField("their_prices", "competitor_concurrent_pdv", String.class);
    TableTypedField compBrand = new TableTypedField("their_prices", "competitor_brand", String.class);
    TableTypedField compConcurrentEan = new TableTypedField("their_prices", "competitor_concurrent_ean", String.class);
    TableTypedField compPrice = new TableTypedField("their_prices", "competitor_price", double.class);

    Store our_price_store = new Store("our_prices", List.of(ean, pdv, price, qty, capdv));
    Store their_prices_store = new Store("their_prices", List.of(compEan, compConcurrentPdv, compBrand,
            compConcurrentEan, compPrice));
    Store our_stores_their_stores_store = new Store("our_stores_their_stores", List.of(
            new TableTypedField("our_stores_their_stores", "our_store", String.class),
            new TableTypedField("our_stores_their_stores", "their_store", String.class)
    ));

    DuckDBDatastore datastore = new DuckDBDatastore();
    DuckDBDataLoader tm = new DuckDBDataLoader(datastore);

    tm.createOrReplaceTable(our_price_store.name(), our_price_store.fields());
    tm.createOrReplaceTable(their_prices_store.name(), their_prices_store.fields(), false);
    tm.createOrReplaceTable(our_stores_their_stores_store.name(), our_stores_their_stores_store.fields(), false);

    tm.load(MAIN_SCENARIO_NAME,
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000, 10_000d},
                    new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000, 10_000d},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 10d, 1000, 10_000d},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 10d, 1000, 10_000d}
            ));
    tm.load("MN up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000, 11_000d},
                    new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000, 10_000d},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 11d, 1000, 11_000d},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 10d, 1000, 10_000d}
            ));
    tm.load("MDD up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000, 10_000d},
                    new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000, 11_000d},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 10d, 1000, 10_000d},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 11d, 1000, 11_000d}
            ));
    tm.load("MN & MDD up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000, 11_000d},
                    new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000, 11_000d},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 11d, 1000, 11_000d},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 11d, 1000, 11_000d}
            ));
    tm.load("MN & MDD down",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 9d, 1000, 9_000d},
                    new Object[]{"ITMella 250g", "ITM Balma", 9d, 1000, 9_000d},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 9d, 1000, 9_000d},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 9d, 1000, 9_000d}
            ));

    tm.load("their_prices", List.of(
            new Object[]{"Nutella 250g", "Leclerc Rouffiac", "Leclerc", "Nutella 250g", 9d},
            new Object[]{"Nutella 250g", "Auchan Toulouse", "Auchan", "Nutella 250g", 11d},
            new Object[]{"Nutella 250g", "Auchan Ponts Jumeaux", "Auchan", "Nutella 250g", 11d},
            new Object[]{"Nutella 250g", "Auchan Launaguet", "Auchan", "Nutella 250g", 9d},
            new Object[]{"ITMella 250g", "Leclerc Rouffiac", "Leclerc", "LeclercElla", 9d},
            new Object[]{"ITMella 250g", "Auchan Toulouse", "Auchan", "AuchanElla", 11d},
            new Object[]{"ITMella 250g", "Auchan Launaguet", "Auchan", "AuchanElla", 9d}
    ));

    tm.load("our_stores_their_stores", List.of(
            new Object[]{"ITM Balma", "Leclerc Rouffiac"},
            new Object[]{"ITM Balma", "Auchan Toulouse"},
            new Object[]{"ITM Balma", "Auchan Ponts Jumeaux"},
            new Object[]{"ITM Toulouse and Drive", "Auchan Launaguet"},
            new Object[]{"ITM Toulouse and Drive", "Auchan Toulouse"},
            new Object[]{"ITM Toulouse and Drive", "Auchan Ponts Jumeaux"}
    ));
    return datastore;
  }
}
