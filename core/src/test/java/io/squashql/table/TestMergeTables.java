package io.squashql.table;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Table;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.squashql.query.database.SQLTranslator.TOTAL_CELL;

class TestMergeTables {

  @Test
  void mergeWithEmptyLeftTable() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MN       | A        | 20        |
    | MN       | B        | 25        |
    | MDD      | A        | 12        |
    | MDD      | C        | 5         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5))));

    Table mergedTable = MergeTables.mergeTables(null, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(rightTable);
  }

  @Test
  void mergeWithEmptyRightTable() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MN       | A        | 20        |
    | MN       | B        | 25        |
    | MDD      | A        | 12        |
    | MDD      | C        | 5         |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5))));

    Table mergedTable = MergeTables.mergeTables(leftTable, null);
    Assertions.assertThat(mergedTable).isEqualTo(leftTable);
  }

  @Test
  void mergeFailWithCommonMeasures() {
    /*
    | typology | price.sum |
    |----------|-----------|
    | MN       | 20        |
    | MDD      | 12        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MDD")),
                    new ArrayList<>(Arrays.asList(20, 12))));
    /*
    | category | price.sum |
    |----------|-----------|
    | A        | 2.3       |
    | B        | 3         |
    | C        | 5         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("A", "B", "C")),
                    new ArrayList<>(Arrays.asList(2.3, 3, 5))));

    Assertions.assertThatThrownBy(() -> MergeTables.mergeTables(leftTable, rightTable))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void mergeTablesWithSameColumns() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MN       | A        | 20        |
    | MN       | B        | 25        |
    | MDD      | A        | 12        |
    | MDD      | C        | 5         |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5))));
    /*
    | typology | category | price.avg |
    |----------|----------|-----------|
    | MN       | A        | 2.3       |
    | MN       | B        | 3         |
    | MDD      | A        | 6         |
    | MDD      | C        | 5         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(2.3, 3, 6, 5))));

    /*
    | typology | category | price.sum | price.avg |
    |----------|----------|-----------|-----------|
    | MDD      | A        | 12        | 6         |
    | MDD      | C        | 5         | 5         |
    | MN       | A        | 20        | 2.3       |
    | MN       | B        | 25        | 3         |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "C", "A", "B")),
                    new ArrayList<>(Arrays.asList(12, 5, 20, 25)),
                    new ArrayList<>(Arrays.asList(6, 5, 2.3, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void mergeTablesWithSameColumnsButDifferentValues() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | C        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("C", "A", "B")),
                    new ArrayList<>(Arrays.asList(5, 20, 25))));
    /*
    | typology | category | price.avg |
    |----------|----------|-----------|
    | MDD      | A        | 6         |
    | MN       | A        | 2.3       |
    | MN       | B        | 3         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "A", "B")),
                    new ArrayList<>(Arrays.asList(6, 2.3, 3))));

    /*
    | typology | category | price.sum | price.avg |
    |----------|----------|-----------|-----------|
    | MDD      | A        | null      | 6         |
    | MDD      | C        | 5         | null      |
    | MN       | A        | 20        | 2.3       |
    | MN       | B        | 25        | 3         |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "C", "A", "B")),
                    new ArrayList<>(Arrays.asList(null, 5, 20, 25)),
                    new ArrayList<>(Arrays.asList(6, null, 2.3, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void mergeTablesWithDifferentColumns() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | C        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("C", "A", "B")),
                    new ArrayList<>(Arrays.asList(5, 20, 25))));
    /*
    | typology | category | company | price.avg |
    |----------|----------|---------|-----------|
    | MDD      | A        | null    | 6         |
    | MN       | A        | LECLERC | 2.3       |
    | MN       | A        | null    | 4         |
    | MN       | B        | SUPER U | 3         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("company", String.class), false),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "A", "A", "B")),
                    new ArrayList<>(Arrays.asList(null, "LECLERC", null, "SUPER U")),
                    new ArrayList<>(Arrays.asList(6, 2.3, 4, 3))));

    /*
    | typology | category | company     | price.sum | price.avg |
    |----------|----------|-------------|-----------|-----------|
    | MDD      | A        | null        | null      | 6         |
    | MDD      | C        | ___total___ | 5         | null      |
    | MN       | A        | ___total___ | 20        | null      |
    | MN       | A        | LECLERC     | null      | 2.3       |
    | MN       | A        | null        | null      | 4         |
    | MN       | B        | ___total___ | 25        | null      |
    | MN       | B        | SUPER U     | null      | 3         |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("company", String.class), false),
                    new Header(new Field("price.sum", int.class), true),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "C", "A", "A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList(null, TOTAL_CELL, TOTAL_CELL, "LECLERC", null, TOTAL_CELL,
                            "SUPER U")),
                    new ArrayList<>(Arrays.asList(null, 5, 20, null, null, 25, null)),
                    new ArrayList<>(Arrays.asList(6, null, null, 2.3, 4, null, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void mergeTablesWithDifferentColumnsAndTotalValues() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | C        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("C", "A", "B")),
                    new ArrayList<>(Arrays.asList(5, 20, 25))));
    /*
    | typology | category | company     | price.avg |
    |----------|----------|-------------|-----------|
    | MN       | A        | ___total___ | 4         |
    | MN       | A        | LECLERC     | 2.3       |
    | MN       | B        | SUPER U     | 3         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("company", String.class), false),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "A", "B")),
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "LECLERC", "SUPER U")),
                    new ArrayList<>(Arrays.asList(4, 2.3, 3))));

    /*
    | typology | category | company     | price.sum | price.avg |
    |----------|----------|-------------|-----------|-----------|
    | MDD      | C        | ___total___ | 5         | null      |
    | MN       | A        | ___total___ | 20        | 4         |
    | MN       | A        | LECLERC     | null      | 2.3       |
    | MN       | B        | ___total___ | 25        | null      |
    | MN       | B        | SUPER U     | null      | 3         |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("company", String.class), false),
                    new Header(new Field("price.sum", int.class), true),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("C", "A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, TOTAL_CELL, "LECLERC", TOTAL_CELL, "SUPER U")),
                    new ArrayList<>(Arrays.asList(5, 20, null, 25, null)),
                    new ArrayList<>(Arrays.asList(null, 4, 2.3, null, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void mergeTablesWithBothCommonAndDifferentColumns() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | A        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "A", "B")),
                    new ArrayList<>(Arrays.asList(5, 20, 25))));
    /*
    | typology | company     | price.avg |
    |----------|-------------|-----------|
    | MDD      | CARREFOUR   | 6.8       |
    | MN       | ___total___ | 4         |
    | MN       | LECLERC     | 2.3       |
    | MN       | SUPER U     | 3         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("company", String.class), false),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("CARREFOUR", TOTAL_CELL, "LECLERC", "SUPER U")),
                    new ArrayList<>(Arrays.asList(6.8, 4, 2.3, 3))));

    /*
    | typology | category    | company     | price.sum | price.avg |
    |----------|-------------|-------------|-----------|-----------|
    | MDD      | ___total___ | CARREFOUR   | null      | 6.8       |
    | MDD      | A           | ___total___ | 5         | null      |
    | MN       | ___total___ | ___total___ | null      | 4         |
    | MN       | ___total___ | LECLERC     | null      | 2.3       |
    | MN       | ___total___ | SUPER U     | null      | 3         |
    | MN       | A           | ___total___ | 20        | null      |
    | MN       | B           | ___total___ | 25        | null      |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("company", String.class), false),
                    new Header(new Field("price.sum", int.class), true),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(
                            Arrays.asList(TOTAL_CELL, "A", TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, "A", "B")),
                    new ArrayList<>(Arrays.asList("CARREFOUR", TOTAL_CELL, TOTAL_CELL, "LECLERC", "SUPER U",
                            TOTAL_CELL, TOTAL_CELL)),
                    new ArrayList<>(Arrays.asList(null, 5, null, null, null, 20, 25)),
                    new ArrayList<>(Arrays.asList(6.8, null, 4, 2.3, 3, null, null))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void mergeTablesWithTotals() {
    /*
    | typology    | category    | price.sum |
    |-------------|-------------|-----------|
    | ___total___ | ___total___ | 27        |
    | MDD         | ___total___ | 15        |
    | MDD         | B           | 15        |
    | MN          | ___total___ | 12        |
    | MN          | A           | 12        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "MDD", "MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, TOTAL_CELL, "B", TOTAL_CELL, "A")),
                    new ArrayList<>(Arrays.asList(27, 15, 15, 12, 12))));
    /*
    | typology    | price.avg |
    |-------------|-----------|
    | ___total___ | 5.3       |
    | MDD         | 2.3       |
    | PP          | 3         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "MDD", "PP")),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, 3))));

    /*
    | typology    | category    | price.sum | price.avg |
    |-------------|-------------|-----------|-----------|
    | ___total___ | ___total___ | 27        | 5.3       |
    | MDD         | ___total___ | 15        | 2.3       |
    | MDD         | B           | 15        | null      |
    | MN          | ___total___ | 12        | null      |
    | MN          | A           | 12        | null      |
    | PP          | ___total___ | null      | 3         |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "MDD", "MDD", "MN", "MN", "PP")),
                    new ArrayList<>(
                            Arrays.asList(TOTAL_CELL, TOTAL_CELL, "B", TOTAL_CELL, "A", TOTAL_CELL)),
                    new ArrayList<>(Arrays.asList(27, 15, 15, 12, 12, null)),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, null, null, null, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void mergeTablesWithoutCommonColumns() {
    /*
    | typology    | price.sum |
    |-------------|-----------|
    | ___total___ | 45        |
    | MDD         | 15        |
    | MN          | 12        |
    | PP          | 18        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("price.sum", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "MDD", "MN", "PP")),
                    new ArrayList<>(Arrays.asList(45, 15, 12, 18))));
    /*
    | category    | price.avg |
    |-------------|-----------|
    | ___total___ | 5.3       |
    | A           | 2.3       |
    | B           | 3         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Header(new Field("category", String.class), false),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, "A", "B")),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, 3))));

    /*
    | typology    | category    | price.sum | price.avg |
    |-------------|-------------|-----------|-----------|
    | ___total___ | ___total___ | 45        | 5.3       |
    | ___total___ | A           | null      | 2.3       |
    | ___total___ | B           | null      | 3         |
    | MDD         | ___total___ | 15        | null      |
    | MN          | ___total___ | 12        | null      |
    | PP          | ___total___ | 18        | null      |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("price.sum", int.class), true),
                    new Header(new Field("price.avg", int.class), true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList(TOTAL_CELL, TOTAL_CELL, TOTAL_CELL, "MDD", "MN", "PP")),
                    new ArrayList<>(
                            Arrays.asList(TOTAL_CELL, "A", "B", TOTAL_CELL, TOTAL_CELL, TOTAL_CELL)),
                    new ArrayList<>(Arrays.asList(45, null, null, 15, 12, 18)),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, 3, null, null, null))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

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
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("Turnover", double.class), true),
                    new Header(new Field("Margin", double.class), true)),
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
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("PriceVariation", double.class), true)),
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
            List.of(new Header(new Field("company", String.class), false),
                    new Header(new Field("PriceIndex", double.class), true)),
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
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("company", String.class), false),
                    new Header(new Field("Turnover", double.class), true),
                    new Header(new Field("Margin", double.class), true),
                    new Header(new Field("PriceVariation", double.class), true),
                    new Header(new Field("PriceIndex", double.class), true)),
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
    Table mergedTable = MergeTables.mergeTables(List.of(table1, table2, table3));
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
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
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("Turnover", double.class), true),
                    new Header(new Field("Margin", double.class), true)),
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
            List.of(new Header(new Field("PriceVariation", double.class), true),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("typology", String.class), false)),
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
            List.of(new Header(new Field("company", String.class), false),
                    new Header(new Field("PriceIndex", double.class), true)),
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
            List.of(new Header(new Field("typology", String.class), false),
                    new Header(new Field("category", String.class), false),
                    new Header(new Field("company", String.class), false),
                    new Header(new Field("Turnover", double.class), true),
                    new Header(new Field("Margin", double.class), true),
                    new Header(new Field("PriceVariation", double.class), true),
                    new Header(new Field("PriceIndex", double.class), true)),
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
    Table mergedTable = MergeTables.mergeTables(List.of(table1, table2, table3));
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }
}
