package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.database.SparkQueryEngine;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.TypedField;
import me.paulbares.transaction.SparkTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

/**
 * This test verifies the use case described here https://docs.google
 * .com/spreadsheets/d/1ueOrfiEcyJAqzYFPSEJKqoFUN6Nl1i7Tbd40JLHuwgU/edit#gid=0
 * is supported
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestITM {

  protected SparkDatastore datastore;

  protected QueryEngine queryEngine;

  protected QueryExecutor queryExecutor;

  @BeforeAll
  void setup() {
    TypedField ean = new TypedField("ean", String.class);
    TypedField pdv = new TypedField("pdv", String.class);
    TypedField price = new TypedField("price", double.class);
    TypedField qty = new TypedField("quantity", int.class);
    TypedField capdv = new TypedField("capdv", double.class);

    TypedField compEan = new TypedField("competitor_ean", String.class);
    TypedField compConcurrentPdv = new TypedField("competitor_concurrent_pdv", String.class);
    TypedField compBrand = new TypedField("competitor_brand", String.class);
    TypedField compConcurrentEan = new TypedField("competitor_concurrent_ean", String.class);
    TypedField compPrice = new TypedField("competitor_price", double.class);

    this.datastore = new SparkDatastore();

    SparkTransactionManager tm = new SparkTransactionManager(this.datastore.spark);
    tm.createTemporaryTable("our_prices", List.of(ean, pdv, price, qty, capdv));
    tm.createTemporaryTable("their_prices", List.of(compEan, compConcurrentPdv, compBrand, compConcurrentEan, compPrice), false);
    tm.createTemporaryTable("our_stores_their_stores", List.of(
            new TypedField("our_store", String.class),
            new TypedField("their_store", String.class)
    ), false);

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

    tm.load(MAIN_SCENARIO_NAME,
            "their_prices", List.of(
                    new Object[]{"Nutella 250g", "Leclerc Rouffiac", "Leclerc", "Nutella 250g", 9d},
                    new Object[]{"Nutella 250g", "Auchan Toulouse", "Auchan", "Nutella 250g", 11d},
                    new Object[]{"Nutella 250g", "Auchan Ponts Jumeaux", "Auchan", "Nutella 250g", 11d},
                    new Object[]{"Nutella 250g", "Auchan Launaguet", "Auchan", "Nutella 250g", 9d},
                    new Object[]{"ITMella 250g", "Leclerc Rouffiac", "Leclerc", "LeclercElla", 9d},
                    new Object[]{"ITMella 250g", "Auchan Toulouse", "Auchan", "AuchanElla", 11d},
                    new Object[]{"ITMella 250g", "Auchan Launaguet", "Auchan", "AuchanElla", 9d}
            ));

    tm.load(MAIN_SCENARIO_NAME,
            "our_stores_their_stores", List.of(
                    new Object[]{"ITM Balma", "Leclerc Rouffiac"},
                    new Object[]{"ITM Balma", "Auchan Toulouse"},
                    new Object[]{"ITM Balma", "Auchan Ponts Jumeaux"},
                    new Object[]{"ITM Toulouse and Drive", "Auchan Launaguet"},
                    new Object[]{"ITM Toulouse and Drive", "Auchan Toulouse"},
                    new Object[]{"ITM Toulouse and Drive", "Auchan Ponts Jumeaux"}
            ));

    this.queryEngine = new SparkQueryEngine(this.datastore);
    this.queryExecutor = new QueryExecutor(this.queryEngine);
  }

  @Test
  void test() {
    var our = new TableDto("our_prices");
    var their = new TableDto("their_prices");
    var our_to_their = new TableDto("our_stores_their_stores");
    our.innerJoin(our_to_their, "pdv", "our_store");
    our_to_their.innerJoin(their, "their_store", "competitor_concurrent_pdv");

    var query = new QueryDto()
            .table(our)
            .withColumn(TransactionManager.SCENARIO_FIELD_NAME)
            .withColumn("ean")
            .aggregatedMeasure("p", "capdv", "sum")
            .expressionMeasure("capdv_concurrents", "sum(competitor_price * quantity)")
            .expressionMeasure("indice_prix", "sum(capdv) / sum(competitor_price * quantity)");

    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("MN & MDD up", "Nutella 250g", 110000d, 102000d, 1.0784313725490196),
            List.of("MN & MDD up", "ITMella 250g", 110000d, 102000d, 1.0784313725490196),

            List.of("MN up", "Nutella 250g", 110000d, 102000d, 1.0784313725490196),
            List.of("MN up", "ITMella 250g", 100000d, 102000d, 0.9803921568627451d),

            List.of("MDD up", "ITMella 250g", 110000d, 102000d, 1.0784313725490196d),
            List.of("MDD up", "Nutella 250g", 100000d, 102000d, 0.9803921568627451d),

            List.of("MN & MDD down", "Nutella 250g", 90000d, 102000d, 0.8823529411764706),
            List.of("MN & MDD down", "ITMella 250g", 90000d, 102000d, 0.8823529411764706),

            List.of(MAIN_SCENARIO_NAME, "ITMella 250g", 100000d, 102000d, 0.9803921568627451d),
            List.of(MAIN_SCENARIO_NAME, "Nutella 250g", 100000d, 102000d, 0.9803921568627451d));
  }
}
