package io.squashql.table;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Table;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TestMergeTablesPaul {

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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.avg", int.class, true)),
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "C", "A", "B")),
                    new ArrayList<>(Arrays.asList(null, 5, 20, 25)),
                    new ArrayList<>(Arrays.asList(6, null, 2.3, 3))));
    Table mergedTable = MergeTablesPaul.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);

    expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "price.avg", "price.sum"));
    mergedTable = MergeTablesPaul.mergeTables(rightTable, leftTable);
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(2.3, 3., 6., 5.))));

    /*
    | typology | category | price.sum | price.avg |
    |----------|----------|-----------|-----------|
    | MN       | A        | 20        | 2.3       |
    | MN       | B        | 25        | 3         |
    | MDD      | A        | 12        | 6         |
    | MDD      | C        | 5         | 5         |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5)),
                    new ArrayList<>(Arrays.asList(2.3, 3., 6., 5.))));
    Table mergedTable = MergeTablesPaul.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);

    expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "price.avg", "price.sum"));
    mergedTable = MergeTablesPaul.mergeTables(rightTable, leftTable);
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.avg", int.class, true)),
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "C", "A", "A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList(null, "___total___", "___total___", "LECLERC", null, "___total___",
                            "SUPER U")),
                    new ArrayList<>(Arrays.asList(null, 5, 20, null, null, 25, null)),
                    new ArrayList<>(Arrays.asList(6, null, null, 2.3, 4, null, 3))));
    Table mergedTable = MergeTablesPaul.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);

    expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "company", "price.avg", "price.sum"));
    mergedTable = MergeTablesPaul.mergeTables(rightTable, leftTable);
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("C", "A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList("___total___", "___total___", "LECLERC", "___total___", "SUPER U")),
                    new ArrayList<>(Arrays.asList(5, 20, null, 25, null)),
                    new ArrayList<>(Arrays.asList(null, 4, 2.3, null, 3))));
    Table mergedTable = MergeTablesPaul.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);

    expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "company", "price.avg", "price.sum"));
    mergedTable = MergeTablesPaul.mergeTables(rightTable, leftTable);
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);
  }

  @Test
  void merge_tables_with_both_common_and_different_columns() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | A        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
     */
    Table leftTable = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
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
            List.of(new Header("typology", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("CARREFOUR", "___total___", "LECLERC", "SUPER U")),
                    new ArrayList<>(Arrays.asList(6.8, 4., 2.3, 3.))));

    /*
    | typology | category    | company     | price.sum | price.avg |
    |----------|-------------|-------------|-----------|-----------|
    | MDD      | ___total___ | CARREFOUR   | null      | 6.8       |
    | MN       | ___total___ | ___total___ | null      | 4         |
    | MN       | ___total___ | LECLERC     | null      | 2.3       |
    | MN       | ___total___ | SUPER U     | null      | 3         |
    | MDD      | A           | ___total___ | 5         | null      |
    | MN       | A           | ___total___ | 20        | null      |
    | MN       | B           | ___total___ | 25        | null      |
     */
    Table expectedTable = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MDD", "MN", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(
                            Arrays.asList("___total___", "A", "___total___", "___total___", "___total___", "A", "B")),
                    new ArrayList<>(Arrays.asList("CARREFOUR", "___total___", "___total___", "LECLERC", "SUPER U",
                            "___total___", "___total___")),
                    new ArrayList<>(Arrays.asList(null, 5, null, null, null, 20, 25)),
                    new ArrayList<>(Arrays.asList(6.8, null, 4., 2.3, 3., null, null))));
    Table mergedTable = MergeTablesPaul.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);

    expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "company", "category", "price.avg", "price.sum"));
    mergedTable = MergeTablesPaul.mergeTables(rightTable, leftTable);
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
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
            List.of(new Header("typology", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "MDD", "PP")),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, 3.))));

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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "MDD", "MDD", "MN", "MN", "PP")),
                    new ArrayList<>(
                            Arrays.asList("___total___", "___total___", "B", "___total___", "A", "___total___")),
                    new ArrayList<>(Arrays.asList(27, 15, 15, 12, 12, null)),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, null, null, null, 3.))));
    Table mergedTable = MergeTablesPaul.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);

    expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "price.avg", "price.sum"));
    mergedTable = MergeTablesPaul.mergeTables(rightTable, leftTable);
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);
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
            List.of(new Header("typology", String.class, false),
                    new Header("price.sum", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
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
            List.of(new Header("category", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
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
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "___total___", "___total___", "MDD", "MN", "PP")),
                    new ArrayList<>(
                            Arrays.asList("___total___", "A", "B", "___total___", "___total___", "___total___")),
                    new ArrayList<>(Arrays.asList(45, null, null, 15, 12, 18)),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, 3, null, null, null))));
    Table mergedTable = MergeTablesPaul.mergeTables(leftTable, rightTable);
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);

    expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("category", "typology", "price.avg", "price.sum"));
    mergedTable = MergeTablesPaul.mergeTables(rightTable, leftTable);
    Assertions.assertThat(mergedTable).containsExactlyInAnyOrderElementsOf(expectedTable);
  }

  // TODO use this method in all tests
  private static Table reorderColumns(ColumnarTable table, List<String> order) {
    List<List<Object>> newValues = new ArrayList<>(table.getColumns().size());
    List<Header> newHeaders = new ArrayList<>(table.getColumns().size());
    for (int i = 0; i < table.getColumns().size(); i++) {
      newValues.add(null);
      newHeaders.add(null);
    }
    for (int i = 0; i < order.size(); i++) {
      int newIndex = table.headers().stream().map(h -> h.name()).toList().indexOf(order.get(i));
      newValues.set(i, table.getColumns().get(newIndex));
      newHeaders.set(i, table.headers().get(newIndex));
    }
    return new ColumnarTable(newHeaders, table.measures(), newValues);
  }
}
