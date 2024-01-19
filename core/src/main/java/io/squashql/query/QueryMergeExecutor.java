package io.squashql.query;

import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.exception.LimitExceedException;
import io.squashql.table.*;
import io.squashql.util.Queries;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryMergeExecutor {

  public static Table executeQueryMerge(QueryExecutor queryExecutor, QueryDto first, QueryDto second, JoinType joinType, SquashQLUser user) {
    Function<QueryDto, Table> executor = query -> queryExecutor.executeQuery(
            query,
            CacheStatsDto.builder(),
            user,
            false,
            limit -> {
              throw new LimitExceedException("Result of " + query + " is too big (limit=" + limit + ")");
            });
    return execute(first, second, joinType, t -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t, true), executor);
  }

  public static PivotTable executePivotQueryMerge(QueryExecutor queryExecutor, QueryDto first, QueryDto second, List<NamedField> rows, List<NamedField> columns, JoinType joinType, SquashQLUser user) {
    Function<QueryDto, Table> executor = query -> {
      Set<Field> columnsFromColumnSets = query.columnSets.values().stream().flatMap(cs -> cs.getNewColumns().stream()).collect(Collectors.toSet());
      return queryExecutor.executePivotQuery(
              new PivotTableQueryDto(query,
                      rows.stream().filter(r -> query.columns.contains(r) || columnsFromColumnSets.contains(r)).toList(),
                      columns.stream().filter(r -> query.columns.contains(r) || columnsFromColumnSets.contains(r)).toList()),
              CacheStatsDto.builder(),
              user,
              false,
              limit -> {
                throw new LimitExceedException("Result of " + query + " is too big (limit=" + limit + ")");
              })
              .table;
    };

    Function<Table, ColumnarTable> replaceTotalCellValuesFunction = t -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t,
            rows.stream().map(NamedField::name).toList(),
            columns.stream().map(NamedField::name).toList());
    ColumnarTable table = execute(first, second, joinType, replaceTotalCellValuesFunction, executor);
    List<String> values = table.headers().stream().filter(Header::isMeasure).map(Header::name).toList();
    return new PivotTable(table, rows.stream().map(NamedField::name).toList(), columns.stream().map(NamedField::name).toList(), values);
  }

  private static ColumnarTable execute(QueryDto first,
                                       QueryDto second,
                                       JoinType joinType,
                                       Function<Table, ColumnarTable> replaceTotalCellValuesFunction,
                                       Function<QueryDto, Table> executor) {
    Map<String, Comparator<?>> firstComparators = Queries.getComparators(first);
    Map<String, Comparator<?>> secondComparators = Queries.getComparators(second);
    secondComparators.putAll(firstComparators); // the comparators of the first query take precedence over the second's

    Set<ColumnSet> columnSets = Stream
            .concat(first.columnSets.values().stream(), second.columnSets.values().stream())
            .collect(Collectors.toSet());

    CompletableFuture<Table> f1 = CompletableFuture.supplyAsync(() -> executor.apply(first));
    CompletableFuture<Table> f2 = CompletableFuture.supplyAsync(() -> executor.apply(second));
    try {
      return CompletableFuture.allOf(f1, f2)
              .thenApply(__ -> {
                ColumnarTable table = (ColumnarTable) MergeTables.mergeTables(f1.join(), f2.join(), joinType);
                table = replaceTotalCellValuesFunction.apply(table);
                return (ColumnarTable) TableUtils.orderRows(table, secondComparators, columnSets);
              })
              .join();
    } catch (Exception e) {
      if (e instanceof CompletionException) {
        Throwable cause = e.getCause();
        if (cause instanceof LimitExceedException lee) {
          throw lee;
        }
      }
      throw new RuntimeException(e);
    }
  }
}
