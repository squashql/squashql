package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkStore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static org.apache.spark.sql.functions.col;

/**
 * This test verifies the use case described here https://docs.google
 * .com/spreadsheets/d/1ueOrfiEcyJAqzYFPSEJKqoFUN6Nl1i7Tbd40JLHuwgU/edit#gid=0
 * is supported
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestITM {

  protected SparkDatastore datastore;

  protected QueryEngine queryEngine;

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field pdv = new Field("pdv", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);

    Field compEan = new Field("competitor_ean", String.class);
    Field compConcurrentPdv = new Field("competitor_concurrent_pdv", String.class);
    Field compBrand = new Field("competitor_brand", String.class);
    Field compConcurrentEan = new Field("competitor_concurrent_ean", String.class);
    Field compPrice = new Field("competitor_price", double.class);

    List<SparkStore> stores = new ArrayList<>();
    SparkStore our_price_store = new SparkStore("our_prices", List.of(ean, pdv, price, qty),
            col("price").multiply(col("quantity")).as(
                    "capdv"));
    stores.add(our_price_store);
    SparkStore their_prices_store = new SparkStore("their_prices", List.of(compEan, compConcurrentPdv, compBrand,
            compConcurrentEan, compPrice));
    stores.add(their_prices_store);
    SparkStore our_stores_their_stores_store = new SparkStore("our_stores_their_stores", List.of(
            new Field("our_store", String.class),
            new Field("their_store", String.class)
    ));
    stores.add(our_stores_their_stores_store);

    this.datastore = new SparkDatastore(stores.toArray(new SparkStore[0]));

    this.datastore.load(MAIN_SCENARIO_NAME,
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 10d, 1000}
            ));
    this.datastore.load("MN up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 10d, 1000}
            ));
    this.datastore.load("MDD up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 11d, 1000}
            ));
    this.datastore.load("MN & MDD up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 11d, 1000}
            ));
    this.datastore.load("MN & MDD down",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 9d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 9d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 9d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 9d, 1000}
            ));

    this.datastore.load(MAIN_SCENARIO_NAME,
            "their_prices", List.of(
                    new Object[]{"Nutella 250g", "Leclerc Rouffiac", "Leclerc", "Nutella 250g", 9d},
                    new Object[]{"Nutella 250g", "Auchan Toulouse", "Auchan", "Nutella 250g", 11d},
                    new Object[]{"Nutella 250g", "Auchan Ponts Jumeaux", "Auchan", "Nutella 250g", 11d},
                    new Object[]{"ITMella 250g", "Leclerc Rouffiac", "Leclerc", "LeclercElla", 9d},
                    new Object[]{"ITMella 250g", "Auchan Toulouse", "Auchan", "AuchanElla", 11d}
            ));

    this.datastore.load(MAIN_SCENARIO_NAME,
            "our_stores_their_stores", List.of(
                    new Object[]{"ITM Balma", "Leclerc Rouffiac"},
                    new Object[]{"ITM Balma", "Auchan Toulouse"},
                    new Object[]{"ITM Balma", "Auchan Ponts Jumeaux"},
                    new Object[]{"ITM Toulouse and Drive", "Auchan Launaguet"},
                    new Object[]{"ITM Toulouse and Drive", "Auchan Toulouse"},
                    new Object[]{"ITM Toulouse and Drive", "Auchan Ponts Jumeaux"}
            ));

    this.queryEngine = new SparkQueryEngine(this.datastore);

//    for (SparkStore store : stores) {
//      store.get().show();
//    }
//      our_price_store.get()
//              .join(our_stores_their_stores_store.get(), functions.col("pdv").equalTo(functions.col("our_store")))
//              .join(their_prices_store.get(), functions.col("their_store").equalTo(functions.col("competitor_concurrent_pdv")))
//              .show();
      ;
  }

  @Test
  void test() {
    var our = QueryBuilder.table("our_prices");
    var their = QueryBuilder.table("their_prices");
    var our_to_their = QueryBuilder.table("our_stores_their_stores");
    our.innerJoin(our_to_their, "pdv", "our_store");
    our_to_their.innerJoin(their, "their_store", "competitor_concurrent_pdv");

    var query = QueryBuilder
            .query()
            .table(our)
            .wildcardCoordinate(Datastore.SCENARIO_FIELD_NAME)
            .wildcardCoordinate("ean")
            .aggregatedMeasure("capdv", "sum")
            .expressionMeasure("capdv_concurrents", "sum(competitor_price * quantity)")
            .expressionMeasure("indice_prix", "sum(capdv) / sum(competitor_price * quantity)");

    Table table = this.queryEngine.execute(query);
    table.show();
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("MN & MDD up", "Nutella 250g", 33000d, 31000d, 1.064516129032258d),
            List.of("MN & MDD up", "ITMella 250g", 22000d, 20000d, 1.1d),

            List.of("MN up", "Nutella 250g", 33000d, 31000d, 1.064516129032258d),
            List.of("MN up", "ITMella 250g", 20000d, 20000d, 1.0d),

            List.of("MDD up", "ITMella 250g", 22000d, 20000d, 1.1d),
            List.of("MDD up", "Nutella 250g", 30000d, 31000d, 0.967741935483871d),

            List.of("MN & MDD down", "Nutella 250g", 27000d, 31000d, 0.8709677419354839d),
            List.of("MN & MDD down", "ITMella 250g", 18000d, 20000d, 0.9d),

            List.of(MAIN_SCENARIO_NAME, "ITMella 250g", 20000d, 20000d, 1d),
            List.of(MAIN_SCENARIO_NAME, "Nutella 250g", 30000d, 31000d, 0.967741935483871d));
  }
}
