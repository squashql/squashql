package io.squashql.table;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.Header;
import io.squashql.query.dto.JoinType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.squashql.query.database.SQLTranslator.TOTAL_CELL;
import static io.squashql.table.ATestMergeTables.orderRows;

class TestMergeThreeTables {

  @Test
  void mergeThreeTables() {
    /*
    | typology    | Turnover | Margin |
    |-------------|----------|--------|
    | ___total___ | 2950     | 450    |
    | MDD         | 1000     | 220    |
    | MN          | 2500     | 180    |
    | PP          | 450      | 50     |
    */
    Table table1 = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("Turnover", double.class, true),
                    new Header("Margin", double.class, true)),
            Set.of(new AggregatedMeasure("Turnover", "unit_turnover", "sum"),
                    new AggregatedMeasure("Margin", "unit_margin", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "MDD", "MN", "PP")),
                    new ArrayList<>(Arrays.asList(2950, 1000, 2500, 450)),
                    new ArrayList<>(Arrays.asList(450, 220, 180, 50))));
    /*
    | typology    | category    | PriceVariation |
    |-------------|-------------|----------------|
    | ___total___ | ___total___ | 0.09           |
    | MDD         | ___total___ | 0.15           |
    | MDD         | A           | 0.15           |
    | MN          | ___total___ | -0.01          |
    | MN          | B           | 0.02           |
    | MN          | C           | -0.05          |
    */
    Table table2 = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("PriceVariation", double.class, true)),
            Set.of(new AggregatedMeasure("PriceVariation", "price_variation", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "MDD", "MDD", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, TOTAL_CELL, "A", TOTAL_CELL, "B", "C")),
                    new ArrayList<>(Arrays.asList(0.09, 0.15, 0.15, -0.01, 0.02, -0.05))));

    /*
    | company     | PriceIndex |
    |-------------|------------|
    | ___total___ | 101.5      |
    | CARREFOUR   | 99.27      |
    | LECLERC     | 105.1      |
    | SUPER U     | 101        |
    */
    Table table3 = new ColumnarTable(
            List.of(new Header("company", String.class, false),
                    new Header("PriceIndex", double.class, true)),
            Set.of(new AggregatedMeasure("PriceIndex", "price_index", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "CARREFOUR", "LECLERC", "SUPER U")),
                    new ArrayList<>(Arrays.asList(101.5, 99.27, 105.1, 101))));


    /*
    | typology    | category    | company     | Turnover | Margin | PriceVariation | PriceIndex |
    |-------------|-------------|-------------|----------|--------|----------------|------------|
    | ___total___ | ___total___ | ___total___ | 2950     | 450    | 0.09           | 101.5      |
    | ___total___ | ___total___ | CARREFOUR   | null     | null   | null           | 99.27      |
    | ___total___ | ___total___ | LECLERC     | null     | null   | null           | 105.1      |
    | ___total___ | ___total___ | SUPER U     | null     | null   | null           | 101        |
    | MDD         | ___total___ | ___total___ | 1000     | 220    | 0.15           | null       |
    | MDD         | A           | ___total___ | null     | null   | 0.15           | null       |
    | MN          | ___total___ | ___total___ | 2500     | 180    | -0.01          | null       |
    | MN          | B           | ___total___ | null     | null   | 0.02           | null       |
    | MN          | C           | ___total___ | null     | null   | -0.05          | null       |
    | PP          | ___total___ | ___total___ | 450      | 50     | null           | null       |
    */
    Table expectedTable = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("Turnover", double.class, true),
                    new Header("Margin", double.class, true),
                    new Header("PriceVariation", double.class, true),
                    new Header("PriceIndex", double.class, true)),
            Set.of(new AggregatedMeasure("Turnover", "unit_turnover", "sum"),
                    new AggregatedMeasure("Margin", "unit_margin", "sum"),
                    new AggregatedMeasure("PriceVariation", "price_variation", "avg"),
                    new AggregatedMeasure("PriceIndex", "price_index", "avg")),
            List.of(
                    new ArrayList<>(
                            Arrays.asList(TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, "MDD", "MDD",
                                    "MN", "MN", "MN", "PP")),
                    new ArrayList<>(
                            Arrays.asList(TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL,
                                    "A", TOTAL_CELL, "B", "C", TOTAL_CELL)),
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "CARREFOUR", "LECLERC", "SUPER U", TOTAL_CELL,
                            TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL)),
                    new ArrayList<>(Arrays.asList(2950, null, null, null, 1000, null, 2500, null, null, 450)),
                    new ArrayList<>(Arrays.asList(450, null, null, null, 220, null, 180, null, null, 50)),
                    new ArrayList<>(Arrays.asList(0.09, null, null, null, 0.15, 0.15, -0.01, 0.02, -0.05, null)),
                    new ArrayList<>(Arrays.asList(101.5, 99.27, 105.1, 101, null, null, null, null, null, null))));
    Table mergedTable = MergeTables.mergeTables(List.of(table1, table2, table3), JoinType.FULL);
    Assertions.assertThat(orderRows(mergedTable)).isEqualTo(orderRows(expectedTable));
  }

  @Test
  void mergeThreeUnorderedTables() {
    /*
    | typology    | Turnover | Margin |
    |-------------|----------|--------|
    | MN          | 2500     | 180    |
    | MDD         | 1000     | 220    |
    | PP          | 450      | 50     |
    | ___total___ | 2950     | 450    |
    */
    Table table1 = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("Turnover", double.class, true),
                    new Header("Margin", double.class, true)),
            Set.of(new AggregatedMeasure("Turnover", "unit_turnover", "sum"),
                    new AggregatedMeasure("Margin", "unit_margin", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MDD", "PP", TOTAL_CELL)),
                    new ArrayList<>(Arrays.asList(2500, 1000, 450, 2950)),
                    new ArrayList<>(Arrays.asList(180, 220, 50, 450))));
    /*
    | PriceVariation | category    | typology    |
    |----------------|-------------|-------------|
    | 0.09           | ___total___ | ___total___ |
    | -0.01          | ___total___ | MN          |
    | 0.02           | B           | MN          |
    | -0.05          | C           | MN          |
    | 0.15           | ___total___ | MDD         |
    | 0.15           | A           | MDD         |
    */
    Table table2 = new ColumnarTable(
            List.of(new Header("PriceVariation", double.class, true),
                    new Header("category", String.class, false),
                    new Header("typology", String.class, false)),
            Set.of(new AggregatedMeasure("PriceVariation", "price_variation", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList(0.09, -0.01, 0.02, -0.05, 0.15, 0.15)),
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, TOTAL_CELL, "B", "C", TOTAL_CELL, "A")),
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "MN", "MN", "MN", "MDD", "MDD"))));

    /*
    | company     | PriceIndex |
    |-------------|------------|
    | ___total___ | 101.5      |
    | CARREFOUR   | 99.27      |
    | LECLERC     | 105.1      |
    | SUPER U     | 101        |
    */
    Table table3 = new ColumnarTable(
            List.of(new Header("company", String.class, false),
                    new Header("PriceIndex", double.class, true)),
            Set.of(new AggregatedMeasure("PriceIndex", "price_index", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "CARREFOUR", "LECLERC", "SUPER U")),
                    new ArrayList<>(Arrays.asList(101.5, 99.27, 105.1, 101))));


    /*
    | typology    | category    | company     | Turnover | Margin | PriceVariation | PriceIndex |
    |-------------|-------------|-------------|----------|--------|----------------|------------|
    | ___total___ | ___total___ | ___total___ | 2950     | 450    | 0.09           | 101.5      |
    | ___total___ | ___total___ | CARREFOUR   | null     | null   | null           | 99.27      |
    | ___total___ | ___total___ | LECLERC     | null     | null   | null           | 105.1      |
    | ___total___ | ___total___ | SUPER U     | null     | null   | null           | 101        |
    | MDD         | ___total___ | ___total___ | 1000     | 220    | 0.15           | null       |
    | MDD         | A           | ___total___ | null     | null   | 0.15           | null       |
    | MN          | ___total___ | ___total___ | 2500     | 180    | -0.01          | null       |
    | MN          | B           | ___total___ | null     | null   | 0.02           | null       |
    | MN          | C           | ___total___ | null     | null   | -0.05          | null       |
    | PP          | ___total___ | ___total___ | 450      | 50     | null           | null       |
    */
    Table expectedTable = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("Turnover", double.class, true),
                    new Header("Margin", double.class, true),
                    new Header("PriceVariation", double.class, true),
                    new Header("PriceIndex", double.class, true)),
            Set.of(new AggregatedMeasure("Turnover", "unit_turnover", "sum"),
                    new AggregatedMeasure("Margin", "unit_margin", "sum"),
                    new AggregatedMeasure("PriceVariation", "price_variation", "avg"),
                    new AggregatedMeasure("PriceIndex", "price_index", "avg")),
            List.of(
                    new ArrayList<>(
                            Arrays.asList(TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, "MDD", "MDD",
                                    "MN", "MN", "MN", "PP")),
                    new ArrayList<>(
                            Arrays.asList(TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL,
                                    "A", TOTAL_CELL, "B", "C", TOTAL_CELL)),
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "CARREFOUR", "LECLERC", "SUPER U", TOTAL_CELL,
                            TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, TOTAL_CELL)),
                    new ArrayList<>(Arrays.asList(2950, null, null, null, 1000, null, 2500, null, null, 450)),
                    new ArrayList<>(Arrays.asList(450, null, null, null, 220, null, 180, null, null, 50)),
                    new ArrayList<>(Arrays.asList(0.09, null, null, null, 0.15, 0.15, -0.01, 0.02, -0.05, null)),
                    new ArrayList<>(Arrays.asList(101.5, 99.27, 105.1, 101, null, null, null, null, null, null))));
    Table mergedTable = MergeTables.mergeTables(List.of(table1, table2, table3), JoinType.FULL);
    Assertions.assertThat(orderRows(mergedTable)).isEqualTo(orderRows(expectedTable));
  }
}
