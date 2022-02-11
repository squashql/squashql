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
 * This test verifies the use case described here https://docs.google.com/spreadsheets/d/1ueOrfiEcyJAqzYFPSEJKqoFUN6Nl1i7Tbd40JLHuwgU/edit#gid=0
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
        Field compPdv = new Field("competitor_pdv", String.class);
        Field compConcurrentPdv = new Field("competitor_concurrent_pdv", String.class);
        Field compBrand = new Field("competitor_brand", String.class);
        Field compConcurrentEan = new Field("competitor_concurrent_ean", String.class);
        Field compPrice = new Field("competitor_price", double.class);

        List<SparkStore> stores = new ArrayList<>();
        stores.add(new SparkStore("our_prices", List.of(ean, pdv, price, qty), col("price").multiply(col("quantity")).as("capdv")));
        stores.add(new SparkStore("their_prices", List.of(compEan, compPdv, compConcurrentPdv, compBrand, compConcurrentEan, compPrice)));

        this.datastore = new SparkDatastore(stores.toArray(new SparkStore[0]));

        this.datastore.load(MAIN_SCENARIO_NAME,
                "our_prices", List.of(
                        new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000},
                        new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000}
                ));
        this.datastore.load("MN up",
                "our_prices", List.of(
                        new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000},
                        new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000}
                ));
        this.datastore.load("MDD up",
                "our_prices", List.of(
                        new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000},
                        new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000}
                ));
        this.datastore.load("MN & MDD up",
                "our_prices", List.of(
                        new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000},
                        new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000}
                ));
        this.datastore.load("MN & MDD down",
                "our_prices", List.of(
                        new Object[]{"Nutella 250g", "ITM Balma", 9d, 1000},
                        new Object[]{"ITMella 250g", "ITM Balma", 9d, 1000}
                ));

        this.datastore.load(MAIN_SCENARIO_NAME,
                "their_prices", List.of(
                        new Object[]{"Nutella 250g", "ITM Balma", "Leclerc Rouffiac", "Leclerc", "Nutella 250g", 9d},
                        new Object[]{"Nutella 250g", "Auchan Toulouse", "Leclerc Rouffiac", "Auchan", "Nutella 250g", 11d},
                        new Object[]{"Nutella 250g", "ITM Balma", "Auchan Ponts Jumeaux", "Auchan", "Nutella 250g", 11d},
                        new Object[]{"ITMella 250g", "ITM Balma", "Leclerc Rouffiac", "Leclerc", "LeclercElla", 9d},
                        new Object[]{"ITMella 250g", "ITM Balma", "Auchan Toulouse", "Auchan", "AuchanElla", 11d}
                ));

        this.queryEngine = new SparkQueryEngine((SparkDatastore) this.datastore);
    }

    @Test
    void test() {
        var our = QueryBuilder.table("our_prices");
        var their = QueryBuilder.table("their_prices");
        our.innerJoin(their, "ean", "competitor_ean");

        var query = QueryBuilder.query().table(our);
        query
                .wildcardCoordinate(Datastore.SCENARIO_FIELD_NAME)
                .wildcardCoordinate("ean")
                .aggregatedMeasure("capdv", "sum")
                .expressionMeasure("capdv_concurrents", "sum(competitor_price * quantity)")
                .expressionMeasure("indice_prix", "sum(capdv) / sum(competitor_price * quantity)")
        ;

        this.datastore.get("our_prices").show();
        this.datastore.get("their_prices").show();


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
