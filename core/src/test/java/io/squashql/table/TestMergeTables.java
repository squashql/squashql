package io.squashql.table;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Table;
import io.squashql.store.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMergeTables {

  @Test
  void merge_with_empty_left_table() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MN       | A        | 20        |
    | MN       | B        | 25        |
    | MDD      | A        | 12        |
    | MDD      | C        | 5         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {2},
            new int[] {0, 1},
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5))));

    Table mergedTable = MergeTables.mergeTables(null, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(rightTable);
  }

  @Test
  void merge_with_empty_right_table() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MN       | A        | 20        |
    | MN       | B        | 25        |
    | MDD      | A        | 12        |
    | MDD      | C        | 5         |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {2},
            new int[] {0, 1},
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5))));

    Table mergedTable = MergeTables.mergeTables(leftTable, null);
    Assertions.assertThat(mergedTable).isEqualTo(leftTable);
  }

  @Test
  void merge_fail_with_common_measures() {
    /*
    | typology | price.sum |
    |----------|-----------|
    | MN       | 20        |
    | MDD      | 12        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {1},
            new int[] {0},
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
            List.of(new Field("category", String.class), new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {1},
            new int[] {0},
            List.of(
                    new ArrayList<>(Arrays.asList("A", "B", "C")),
                    new ArrayList<>(Arrays.asList(2.3, 3, 5))));

    Assertions.assertThatThrownBy(() -> MergeTables.mergeTables(leftTable, rightTable))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void merge_tables_with_same_columns() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MN       | A        | 20        |
    | MN       | B        | 25        |
    | MDD      | A        | 12        |
    | MDD      | C        | 5         |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {2},
            new int[] {0, 1},
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {2},
            new int[] {0, 1},
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(2.3, 3, 6, 5))));

    /*
    | typology | category | price.sum | price.avg |
    |----------|----------|-----------|-----------|
    | MN       | A        | 20        | 2.3       |
    | MN       | B        | 25        | 3         |
    | MDD      | A        | 12        | 6         |
    | MDD      | C        | 5         | 5         |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class), new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {2, 3},
            new int[] {0, 1},
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5)),
                    new ArrayList<>(Arrays.asList(2.3, 3, 6, 5))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void merge_tables_with_same_columns_but_different_values() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | C        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {2},
            new int[] {0, 1},
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {2},
            new int[] {0, 1},
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class), new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {2, 3},
            new int[] {0, 1},
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "C", "A", "B")),
                    new ArrayList<>(Arrays.asList(null, 5, 20, 25)),
                    new ArrayList<>(Arrays.asList(6, null, 2.3, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void merge_tables_with_different_columns() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | C        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {2},
            new int[] {0, 1},
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("company", String.class), new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {3},
            new int[] {0, 1, 2},
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("company", String.class), new Field("price.sum", int.class),
                    new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {3, 4},
            new int[] {0, 1, 2},
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "C", "A", "A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList(null, "___total___", "___total___", "LECLERC", null, "___total___",
                            "SUPER U")),
                    new ArrayList<>(Arrays.asList(null, 5, 20, null, null, 25, null)),
                    new ArrayList<>(Arrays.asList(6, null, null, 2.3, 4, null, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void merge_tables_with_different_columns_and_total_values() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | C        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {2},
            new int[] {0, 1},
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("company", String.class), new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {3},
            new int[] {0, 1, 2},
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "A", "B")),
                    new ArrayList<>(Arrays.asList("___total___", "LECLERC", "SUPER U")),
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("company", String.class), new Field("price.sum", int.class),
                    new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {3, 4},
            new int[] {0, 1, 2},
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("C", "A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList("___total___", "___total___", "LECLERC", "___total___", "SUPER U")),
                    new ArrayList<>(Arrays.asList(5, 20, null, 25, null)),
                    new ArrayList<>(Arrays.asList(null, 4, 2.3, null, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void merge_tables_with_totals() {
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {2},
            new int[] {0, 1},
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "MDD", "MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("___total___", "___total___", "B", "___total___", "A")),
                    new ArrayList<>(Arrays.asList(27, 15, 15, 12, 12))));
    /*
    | typology    | price.avg |
    |-------------|-----------|
    | ___total___ | 5.3       |
    | MDD         | 2.3       |
    | PP          | 3         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {1},
            new int[] {0},
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "MDD", "PP")),
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class), new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {2, 3},
            new int[] {0, 1},
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "MDD", "MDD", "MN", "MN", "PP")),
                    new ArrayList<>(
                            Arrays.asList("___total___", "___total___", "B", "___total___", "A", "___total___")),
                    new ArrayList<>(Arrays.asList(27, 15, 15, 12, 12, null)),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, null, null, null, 3))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

  @Test
  void merge_tables_without_common_columns() {
    /*
    | typology    | price.sum |
    |-------------|-----------|
    | ___total___ | 45        |
    | MDD         | 15        |
    | MN          | 12        |
    | PP          | 18        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Field("typology", String.class), new Field("price.sum", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum")),
            new int[] {1},
            new int[] {0},
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "MDD", "MN", "PP")),
                    new ArrayList<>(Arrays.asList(45, 15, 12, 18))));
    /*
    | category    | price.avg |
    |-------------|-----------|
    | ___total___ | 5.3       |
    | A           | 2.3       |
    | B           | 3         |
     */
    Table rightTable = new ColumnarTable(
            List.of(new Field("category", String.class), new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {1},
            new int[] {0},
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "A", "B")),
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
            List.of(new Field("typology", String.class), new Field("category", String.class),
                    new Field("price.sum", int.class), new Field("price.avg", int.class)),
            List.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            new int[] {2, 3},
            new int[] {0, 1},
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "___total___", "___total___", "MDD", "MN", "PP")),
                    new ArrayList<>(
                            Arrays.asList("___total___", "A", "B", "___total___", "___total___", "___total___")),
                    new ArrayList<>(Arrays.asList(45, null, null, 15, 12, 18)),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, 3, null, null, null))));
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable).isEqualTo(expectedTable);
  }

}
