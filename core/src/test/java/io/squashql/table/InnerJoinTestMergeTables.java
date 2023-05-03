package io.squashql.table;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Table;
import io.squashql.query.dto.JoinType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

class InnerJoinTestMergeTables extends ATestMergeTables {

  @Override
  JoinType getJoinType() {
    return JoinType.INNER;
  }

  @Override
  Table getMergeTablesWithSameColumnsButDifferentValues() {
    /*
    | typology | category | price.sum | price.avg |
    |----------|----------|-----------|-----------|
    | MN       | A        | 20        | 2.3       |
    | MN       | B        | 25        | 3         |
    */
    return new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "B")),
                    new ArrayList<>(Arrays.asList(20, 25)),
                    new ArrayList<>(Arrays.asList(2.3, 3))));
  }

  @Override
  Table getMergeTablesWithSameColumns() {
    /*
    | typology | category | price.sum | price.avg |
    |----------|----------|-----------|-----------|
    | MN       | A        | 20        | 2.3       |
    | MN       | B        | 25        | 3         |
    | MDD      | A        | 12        | 6         |
    | MDD      | C        | 5         | 5         |
    */
    return new ColumnarTable(
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
  }

  @Override
  Table getMergeTablesWithDifferentColumns() {
    /*
    | typology | category | company     | price.sum | price.avg |
    |----------|----------|-------------|-----------|-----------|
    | MN       | A        | ___total___ | 20        | null      |
    | MN       | A        | LECLERC     | null      | 2.3       |
    | MN       | A        | null        | null      | 4         |
    | MN       | B        | ___total___ | 25        | null      |
    | MN       | B        | SUPER U     | null      | 3         |
    */
    return new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList("___total___", "LECLERC", null, "___total___",
                            "SUPER U")),
                    new ArrayList<>(Arrays.asList(20, null, null, 25, null)),
                    new ArrayList<>(Arrays.asList(null, 2.3, 4, null, 3))));
  }

  @Override
  Table getMergeTablesWithDifferentColumnsAndTotalValues() {
    /*
    | typology | category | company     | price.sum | price.avg |
    |----------|----------|-------------|-----------|-----------|
    | MN       | A        | ___total___ | 20        | 4         |
    | MN       | A        | LECLERC     | null      | 2.3       |
    | MN       | B        | ___total___ | 25        | null      |
    | MN       | B        | SUPER U     | null      | 3         |
    */
    return new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("company", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MN", "MN")),
                    new ArrayList<>(Arrays.asList("A", "A", "B", "B")),
                    new ArrayList<>(Arrays.asList("___total___", "LECLERC", "___total___", "SUPER U")),
                    new ArrayList<>(Arrays.asList(20, null, 25, null)),
                    new ArrayList<>(Arrays.asList(6.3, 2.3, null, 3))));
  }

  @Override
  Table getMergeTablesWithBothCommonAndDifferentColumns() {
    /*
    | typology | category    | company     | price.sum | price.avg |
    |----------|-------------|-------------|-----------|-----------|
    | MDD      | ___total___ | CARREFOUR   | null      | 6.8       |
    | MN       | ___total___ | ___total___ | null      | 4         |
    | MDD      | A           | ___total___ | 5         | null      |
    | MN       | ___total___ | LECLERC     | null      | 2.3       |
    | MN       | ___total___ | SUPER U     | null      | 3         |
    | MN       | A           | ___total___ | 20        | null      |
    | MN       | B           | ___total___ | 25        | null      |
    */
    return new ColumnarTable(
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
                    new ArrayList<>(Arrays.asList("CARREFOUR", "___total___", "___total___", "LECLERC", "SUPER U", "___total___", "___total___")),
                    new ArrayList<>(Arrays.asList(null, 5, null, null, null, 20, 25)),
                    new ArrayList<>(Arrays.asList(6.8, null, 4., 2.3, 3., null, null))));
  }

  @Override
  Table getMergeTablesWithTotals() {
    /*
    | typology    | category    | price.sum | price.avg |
    |-------------|-------------|-----------|-----------|
    | ___total___ | ___total___ | 27        | 5.3       |
    | MDD         | ___total___ | 15        | 2.3       |
    | MDD         | B           | 15        | null      |
    */
    return new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.sum", int.class, true),
                    new Header("price.avg", int.class, true)),
            Set.of(new AggregatedMeasure("price.sum", "price", "sum"),
                    new AggregatedMeasure("price.avg", "price", "avg")),
            List.of(
                    new ArrayList<>(Arrays.asList("___total___", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("___total___", "___total___", "B")),
                    new ArrayList<>(Arrays.asList(27, 15, 15)),
                    new ArrayList<>(Arrays.asList(5.3, 2.3, null))));
  }

  @Override
  Table getMergeTablesWithoutCommonColumns() {
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
    return new ColumnarTable(
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
  }
}
