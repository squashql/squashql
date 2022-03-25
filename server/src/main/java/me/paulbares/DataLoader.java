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
    Field compConcurrentPdv = new Field("competitor_concurrent_pdv", String.class);
    Field compBrand = new Field("competitor_brand", String.class);
    Field compConcurrentEan = new Field("competitor_concurrent_ean", String.class);
    Field compPrice = new Field("competitor_price", double.class);

    List<SparkStore> stores = new ArrayList<>();
    SparkStore our_price_store = new SparkStore("our_prices", List.of(ean, pdv, price, qty),
            col("price").multiply(col("quantity")).as("capdv"));
    stores.add(our_price_store);
    SparkStore their_prices_store = new SparkStore("their_prices", List.of(compEan, compConcurrentPdv, compBrand,
            compConcurrentEan, compPrice));
    stores.add(their_prices_store);
    SparkStore our_stores_their_stores_store = new SparkStore("our_stores_their_stores", List.of(
            new Field("our_store", String.class),
            new Field("their_store", String.class)
    ));
    stores.add(our_stores_their_stores_store);

    SparkDatastore datastore = new SparkDatastore(stores.toArray(new SparkStore[0]));

    datastore.load(MAIN_SCENARIO_NAME,
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 10d, 1000}
            ));
    datastore.load("MN up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 10d, 1000}
            ));
    datastore.load("MDD up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 10d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 11d, 1000}
            ));
    datastore.load("MN & MDD up",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 11d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 11d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 11d, 1000}
            ));
    datastore.load("MN & MDD down",
            "our_prices", List.of(
                    new Object[]{"Nutella 250g", "ITM Balma", 9d, 1000},
                    new Object[]{"ITMella 250g", "ITM Balma", 9d, 1000},
                    new Object[]{"Nutella 250g", "ITM Toulouse and Drive", 9d, 1000},
                    new Object[]{"ITMella 250g", "ITM Toulouse and Drive", 9d, 1000}
            ));

    datastore.load(MAIN_SCENARIO_NAME,
            "their_prices", List.of(
                    new Object[]{"Nutella 250g", "Leclerc Rouffiac", "Leclerc", "Nutella 250g", 9d},
                    new Object[]{"Nutella 250g", "Auchan Toulouse", "Auchan", "Nutella 250g", 11d},
                    new Object[]{"Nutella 250g", "Auchan Ponts Jumeaux", "Auchan", "Nutella 250g", 11d},
                    new Object[]{"Nutella 250g", "Auchan Launaguet", "Auchan", "Nutella 250g", 9d},
                    new Object[]{"ITMella 250g", "Leclerc Rouffiac", "Leclerc", "LeclercElla", 9d},
                    new Object[]{"ITMella 250g", "Auchan Toulouse", "Auchan", "AuchanElla", 11d},
                    new Object[]{"ITMella 250g", "Auchan Launaguet", "Auchan", "AuchanElla", 9d}
            ));

    datastore.load(MAIN_SCENARIO_NAME,
            "our_stores_their_stores", List.of(
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