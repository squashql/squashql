package me.paulbares.jackson;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkStore;
import me.paulbares.store.Field;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

public class TestDataset {

  private static SparkDatastore ds;

  @BeforeAll
  static void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);
    SparkStore store = new SparkStore("products", List.of(ean, category, price, qty));
    ds = new SparkDatastore(store);
    ds.load(MAIN_SCENARIO_NAME, store.name(),
            List.of(
                    new Object[]{"bottle", "drink", 2d, 10},
                    new Object[]{"cookie", "food", 3d, 20},
                    new Object[]{"shirt", "cloth", 10d, 3}
            ));
    // TODO assert ds content
  }
}
