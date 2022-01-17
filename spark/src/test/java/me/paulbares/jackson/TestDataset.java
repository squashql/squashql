package me.paulbares.jackson;

import com.fasterxml.jackson.annotation.JsonAlias;
import me.paulbares.SparkDatastore;
import me.paulbares.store.Field;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;
import java.util.Objects;

import static me.paulbares.SparkDatastore.MAIN_SCENARIO_NAME;

public class TestDataset {

  private static SparkDatastore ds;

  @BeforeAll
  static void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);
    ds = new SparkDatastore(List.of(ean, category, price, qty));

    ds.load(MAIN_SCENARIO_NAME, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  private static class StructType {

    public String ean;
    @JsonAlias("sum(price)")
    public double sumPrice;
    @JsonAlias("sum(quantity)")
    public int sumQuantity;

    public StructType() {
    }

    public StructType(String ean, double sumPrice, int sumQuantity) {
      this.ean = ean;
      this.sumPrice = sumPrice;
      this.sumQuantity = sumQuantity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StructType that = (StructType) o;
      return Objects.equals(ean, that.ean) && Objects.equals(sumPrice, that.sumPrice) && Objects.equals(sumQuantity, that.sumQuantity);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ean, sumPrice, sumQuantity);
    }
  }
}
