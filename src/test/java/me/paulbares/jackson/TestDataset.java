package me.paulbares.jackson;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.core.JsonProcessingException;
import me.paulbares.SparkDatastore;
import me.paulbares.Field;
import me.paulbares.query.Query;
import me.paulbares.query.spark.SparkQueryEngine;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

  @Test
  void testSerialization() throws JsonProcessingException {
    Query query = new Query()
            .addWildcardCoordinate("ean")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum");

    String serialize = JacksonUtil.datasetToJSON(new SparkQueryEngine(ds).execute(query));
    System.out.println(serialize);
    StructType[] structTypes = JacksonUtil.mapper.readValue(serialize, StructType[].class);
    Assertions.assertThat(structTypes).containsExactlyInAnyOrder(
            new StructType("shirt", 10d, 3),
            new StructType("cookie", 3d, 20),
            new StructType("bottle", 2d, 10)
    );
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
