package io.squashql.table;

import io.squashql.query.*;
import io.squashql.query.dto.JoinType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

abstract class ATestMergeTables {

  abstract JoinType getJoinType();

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

    Table expectedTable = getMergeTablesWithSameColumnsButDifferentValues();
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable, getJoinType());
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));

    if (getJoinType() == JoinType.FULL) {
      // Inverse right and left tables. This is only valid for FULL OUTER JOIN
      expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "price.avg", "price.sum"));
      mergedTable = MergeTables.mergeTables(rightTable, leftTable);
      Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));
    }
  }

  abstract Table getMergeTablesWithSameColumnsButDifferentValues();

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

    Table expectedTable = getMergeTablesWithSameColumns();
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable, getJoinType());
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));

    if (getJoinType() == JoinType.FULL) {
      // Inverse right and left tables. This is only valid for FULL OUTER JOIN
      expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "price.avg", "price.sum"));
      mergedTable = MergeTables.mergeTables(rightTable, leftTable);
      Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));
    }
  }

  abstract Table getMergeTablesWithSameColumns();

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

    Table expectedTable = getMergeTablesWithDifferentColumns();
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable, getJoinType());
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));

    if (getJoinType() == JoinType.FULL) {
      // Inverse right and left tables. This is only valid for FULL OUTER JOIN
      expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "company", "price.avg", "price.sum"));
      mergedTable = MergeTables.mergeTables(rightTable, leftTable);
      Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));
    }
  }

  abstract Table getMergeTablesWithDifferentColumns();

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
    | MN       | A        | ___total___ | 6.3       |
    | MDD      | A        | AUCHAN      | 1         |
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
                    new ArrayList<>(Arrays.asList("MN", "MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "A", "A", "B")),
                    new ArrayList<>(Arrays.asList("___total___", "AUCHAN", "LECLERC", "SUPER U")),
                    new ArrayList<>(Arrays.asList(6.3, 1, 2.3, 3))));

    Table expectedTable = getMergeTablesWithDifferentColumnsAndTotalValues();
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable, getJoinType());
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));

    if (getJoinType() == JoinType.FULL) {
      // Inverse right and left tables. This is only valid for FULL OUTER JOIN
      expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "company", "price.avg", "price.sum"));
      mergedTable = MergeTables.mergeTables(rightTable, leftTable);
      Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));
    }
  }

  abstract Table getMergeTablesWithDifferentColumnsAndTotalValues();

  /**
   * Same as {@link #merge_tables_with_different_columns_and_total_values()} but columns are not in the same order in
   * the tables. Ideally, all the tests should shuffle the order of the columns.
   */
  @Test
  void merge_tables_with_different_columns_different_order_and_total_values() {
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
    | company     | category | typology | price.avg |
    |-------------|----------|----------|-----------|
    | ___total___ | A        | MN       | 6.3       |
    | AUCHAN      | A        | MDD      | 1         |
    | LECLERC     | A        | MN       | 2.3       |
    | SUPER U     | B        | MN       | 3         |
    */
    Table rightTable = new ColumnarTable(
            List.of(new Header("company", String.class, false),
                    new Header("category", String.class, false),
                    new Header("typology", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "AUCHAN", "LECLERC", "SUPER U")),
                    new ArrayList<>(Arrays.asList("A", "A", "A", "B")),
                    new ArrayList<>(Arrays.asList("MN", "MDD", "MN", "MN")),
                    new ArrayList<>(Arrays.asList(6.3, 1, 2.3, 3))));

    Table expectedTable = getMergeTablesWithDifferentColumnsAndTotalValues();
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable, getJoinType());
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));

    if (getJoinType() == JoinType.FULL) {
      // Inverse right and left tables. This is only valid for FULL OUTER JOIN
      expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("company", "category", "typology", "price.avg", "price.sum"));
      mergedTable = MergeTables.mergeTables(rightTable, leftTable);
      Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));
    }
  }

  @Test
  void merge_tables_with_both_common_and_different_columns() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MDD      | A        | 5         |
    | MN       | A        | 20        |
    | MN       | B        | 25        |
    | ZZ       | B        | 15        |
    */
    Table leftTable = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN", "ZZ")),
                    new ArrayList<>(Arrays.asList("A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList(5, 20, 25, 15))));
    /*
    | typology | company     | price.avg |
    |----------|-------------|-----------|
    | MDD      | CARREFOUR   | 6.8       |
    | MN       | ___total___ | 4         |
    | MN       | LECLERC     | 2.3       |
    | MN       | SUPER U     | 3         |
    | XX       | AUCHAN      | 42        |
    */
    Table rightTable = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MDD", "MN", "MN", "MN", "XX")),
                    new ArrayList<>(Arrays.asList("CARREFOUR", "___total___", "LECLERC", "SUPER U", "AUCHAN")),
                    new ArrayList<>(Arrays.asList(6.8, 4., 2.3, 3., 42))));

    Table expectedTable = getMergeTablesWithBothCommonAndDifferentColumns();
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable, getJoinType());
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));

    if (getJoinType() == JoinType.FULL) {
      // Inverse right and left tables. This is only valid for FULL OUTER JOIN
      expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "company", "category", "price.avg", "price.sum"));
      mergedTable = MergeTables.mergeTables(rightTable, leftTable);
      Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));
    }
  }

  abstract Table getMergeTablesWithBothCommonAndDifferentColumns();

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

    Table expectedTable = getMergeTablesWithTotals();
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable, getJoinType());
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));

    if (getJoinType() == JoinType.FULL) {
      // Inverse right and left tables. This is only valid for FULL OUTER JOIN
      expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("typology", "category", "price.avg", "price.sum"));
      mergedTable = MergeTables.mergeTables(rightTable, leftTable);
      Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));
    }
  }

  abstract Table getMergeTablesWithTotals();

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

    Table expectedTable = getMergeTablesWithoutCommonColumns();
    Table mergedTable = MergeTables.mergeTables(leftTable, rightTable, getJoinType());
    Assertions.assertThat(mergedTable.headers()).containsExactlyElementsOf(expectedTable.headers());
    Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));

    if (getJoinType() == JoinType.FULL) {
      // Inverse right and left tables. This is only valid for FULL OUTER JOIN
      expectedTable = reorderColumns((ColumnarTable) expectedTable, List.of("category", "typology", "price.avg", "price.sum"));
      mergedTable = MergeTables.mergeTables(rightTable, leftTable);
      Assertions.assertThat(orderRows(mergedTable)).containsExactlyInAnyOrderElementsOf(orderRows(expectedTable));
    }
  }

  abstract Table getMergeTablesWithoutCommonColumns();

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

  protected static Table orderRows(Table table) {
    return TableUtils.orderRows((ColumnarTable) table);
  }
}
