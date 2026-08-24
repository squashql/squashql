package io.squashql.table;

import io.squashql.query.Header;
import io.squashql.query.dto.JoinType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.squashql.table.ATestMergeTables.priceAvg;
import static io.squashql.table.ATestMergeTables.priceSum;

class TestMergeTablesEdgeCases {

  /*
  | typology | category | price.sum |
  |----------|----------|-----------|
  | MN       | A        | 20        |
  | MN       | B        | 25        |
  | MDD      | A        | 12        |
  | MDD      | C        | 5         |
  */
  private static Table table() {
    return new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
            Set.of(priceSum),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5))));
  }

  /*
  | typology | company | price.avg |
  |----------|---------|-----------|
  (no rows)
  */
  private static Table emptyTable() {
    return new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.avg", double.class, true)),
            Set.of(priceAvg),
            List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>()));
  }

  /**
   * The merged table must carry the headers of both sides even when one side has no row. Returning the non-empty
   * side as-is drops the empty side's columns and measures, and pivoting the result on one of those dropped
   * columns then fails.
   */
  @Test
  void mergeWithEmptyRightTable() {
    Table mergedTable = MergeTables.mergeTables(table(), emptyTable(), JoinType.LEFT);

    Assertions.assertThat(mergedTable.headers().stream().map(Header::name))
            .containsExactly("typology", "category", "company", "price.sum", "price.avg");
    Assertions.assertThat(mergedTable.count()).isEqualTo(4);

    PivotTable pivotTable = new PivotTable(mergedTable, List.of("typology", "category"), List.of("company"),
            List.of("price.sum", "price.avg"), List.of());
    Assertions.assertThat(pivotTable.pivotTableCells).isNotNull();
  }

  @Test
  void mergeWithEmptyLeftTable() {
    Table mergedTable = MergeTables.mergeTables(emptyTable(), table(), JoinType.FULL);

    Assertions.assertThat(mergedTable.headers().stream().map(Header::name))
            .containsExactly("typology", "company", "category", "price.avg", "price.sum");
    Assertions.assertThat(mergedTable.count()).isEqualTo(4);

    PivotTable pivotTable = new PivotTable(mergedTable, List.of("typology", "category"), List.of("company"),
            List.of("price.sum", "price.avg"), List.of());
    Assertions.assertThat(pivotTable.pivotTableCells).isNotNull();
  }

  @Test
  void mergeWithEmptySideAndRestrictiveJoinYieldsNoRow() {
    Table mergedTable = MergeTables.mergeTables(emptyTable(), table(), JoinType.LEFT);
    Assertions.assertThat(mergedTable.count()).isEqualTo(0);

    mergedTable = MergeTables.mergeTables(table(), emptyTable(), JoinType.INNER);
    Assertions.assertThat(mergedTable.count()).isEqualTo(0);
    Assertions.assertThat(mergedTable.headers().stream().map(Header::name))
            .containsExactly("typology", "category", "company", "price.sum", "price.avg");
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
            List.of(new Header("typology", String.class, false),
                    new Header("price.sum", int.class, true)),
            Set.of(priceSum),
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
            List.of(new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
            Set.of(priceSum),
            List.of(
                    new ArrayList<>(Arrays.asList("A", "B", "C")),
                    new ArrayList<>(Arrays.asList(2.3, 3, 5))));

    Assertions.assertThatThrownBy(() -> MergeTables.mergeTables(leftTable, rightTable, JoinType.LEFT))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
  }
}
