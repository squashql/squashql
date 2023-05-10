package io.squashql.table;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Table;
import io.squashql.query.dto.JoinType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

class TestMergeTablesEdgeCases {

  @Test
  void mergeWithEmptyTable() {
    /*
    | typology | category | price.sum |
    |----------|----------|-----------|
    | MN       | A        | 20        |
    | MN       | B        | 25        |
    | MDD      | A        | 12        |
    | MDD      | C        | 5         |
    */
    Table table = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(20, 25, 12, 5))));

    Table emptyTable = Mockito.mock(Table.class);
    Mockito.when(emptyTable.count()).thenReturn(0L);
    Table mergedTable = MergeTables.mergeTables(emptyTable, table, JoinType.LEFT);
    Assertions.assertThat(mergedTable).isEqualTo(table);

    mergedTable = MergeTables.mergeTables(table, emptyTable, JoinType.LEFT);
    Assertions.assertThat(mergedTable).isEqualTo(table);
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
            List.of(new Header("category", String.class, false),
                    new Header("price.sum", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum")),
            List.of(
                    new ArrayList<>(Arrays.asList("A", "B", "C")),
                    new ArrayList<>(Arrays.asList(2.3, 3, 5))));

    Assertions.assertThatThrownBy(() -> MergeTables.mergeTables(leftTable, rightTable, JoinType.LEFT))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
  }
}
