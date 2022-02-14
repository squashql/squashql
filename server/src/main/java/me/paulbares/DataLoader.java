package me.paulbares;

import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.List;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static org.apache.spark.sql.functions.col;

/**
 * --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 * <p>
 * The first dataset used.
 */
public class DataLoader {

  public static SparkDatastore createTestDatastoreWithData() {
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
    stores.add(new SparkStore("our_prices", List.of(ean, pdv, price, qty), col("price").multiply(col("quantity")).as(
            "capdv")));
    stores.add(new SparkStore("their_prices", List.of(compEan, compPdv, compConcurrentPdv, compBrand,
            compConcurrentEan, compPrice)));

    SparkDatastore datastore = new SparkDatastore(stores.toArray(new SparkStore[0]));

    datastore.load(MAIN_SCENARIO_NAME,
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000}
            ));
    datastore.load("MN up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000}
            ));
    datastore.load("MDD up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000}
            ));
    datastore.load("MN & MDD up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000}
            ));
    datastore.load("MN & MDD down",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 9d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 9d, 1000}
            ));

    datastore.load(MAIN_SCENARIO_NAME,
            "their_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", "Leclerc Rouffiac", "Leclerc", "Nutella 250g", 9d},
                    new Object[]{"Nutella 250g", "Auchan Toulouse", "Leclerc Rouffiac", "Auchan", "Nutella 250g", 11d},
                    new Object[]{"Nutella 250g", "ITM Balma", "Auchan Ponts Jumeaux", "Auchan", "Nutella 250g", 11d},
                    new Object[]{"ITMella 250g", "ITM Balma", "Leclerc Rouffiac", "Leclerc", "LeclercElla", 9d},
                    new Object[]{"ITMella 250g", "ITM Balma", "Auchan Toulouse", "Auchan", "AuchanElla", 11d}
            ));
    return datastore;
  }
}