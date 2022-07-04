package me.paulbares;

import me.paulbares.store.Field;
import me.paulbares.store.Store;
import me.paulbares.transaction.SparkTransactionManager;

import java.util.List;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

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
    Field capdv = new Field("capdv", double.class);

    Field compEan = new Field("competitor_ean", String.class);
    Field compConcurrentPdv = new Field("competitor_concurrent_pdv", String.class);
    Field compBrand = new Field("competitor_brand", String.class);
    Field compConcurrentEan = new Field("competitor_concurrent_ean", String.class);
    Field compPrice = new Field("competitor_price", double.class);

    Store our_price_store = new Store("our_prices", List.of(ean, pdv, price, qty, capdv));
    Store their_prices_store = new Store("their_prices", List.of(compEan, compConcurrentPdv, compBrand,
            compConcurrentEan, compPrice));
    Store our_stores_their_stores_store = new Store("our_stores_their_stores", List.of(
            new Field("our_store", String.class),
            new Field("their_store", String.class)
    ));

    SparkDatastore datastore = new SparkDatastore();
    SparkTransactionManager tm = new SparkTransactionManager(datastore.spark);

    tm.createTemporaryTable(our_price_store.name(), our_price_store.fields());
    tm.createTemporaryTable(their_prices_store.name(), their_prices_store.fields(), null);
    tm.createTemporaryTable(our_stores_their_stores_store.name(), our_stores_their_stores_store.fields(), null);

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
    return datastore;
  }
}
