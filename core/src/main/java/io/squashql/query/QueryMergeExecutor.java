package io.squashql.query;

import io.squashql.query.compiled.CompiledColumnSet;
import io.squashql.query.dto.*;
import io.squashql.query.exception.LimitExceedException;
import io.squashql.table.*;
import io.squashql.util.CustomExplicitOrdering;
import io.squashql.util.DependentExplicitOrdering;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryMergeExecutor {

  public static Table executeQueryMerge(QueryExecutor queryExecutor, QueryDto first, QueryDto second, JoinType joinType, SquashQLUser user) {
    Function<QueryDto, Pair<Table, QueryResolver>> executor = query -> queryExecutor.executeQueryInternal(
            query,
            CacheStatsDto.builder(),
            user,
            false,
            limit -> {
              throw new LimitExceedException("Result of " + query + " is too big (limit=" + limit + ")");
            });
    return execute(first, second, joinType, t -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t, true), executor);
  }

  public static PivotTable executePivotQueryMerge(QueryExecutor queryExecutor, QueryDto first, QueryDto second, List<Field> rows, List<Field> columns, JoinType joinType, SquashQLUser user) {
    Function<QueryDto, Pair<Table, QueryResolver>> executor = query -> {
      Set<Field> columnsFromColumnSets = query.columnSets.values().stream().flatMap(cs -> cs.getNewColumns().stream()).collect(Collectors.toSet());
        final Pair<PivotTable, QueryResolver> result = queryExecutor.executePivotQueryInternal(
              new PivotTableQueryDto(query,
                      rows.stream().filter(r -> query.columns.contains(r) || columnsFromColumnSets.contains(r)).toList(),
                      columns.stream().filter(r -> query.columns.contains(r) || columnsFromColumnSets.contains(r)).toList()),
              CacheStatsDto.builder(),
              user,
              false,
              limit -> {
                throw new LimitExceedException("Result of " + query + " is too big (limit=" + limit + ")");
              });
        return Tuples.pair(result.getOne().table, result.getTwo());
    };

    Function<Table, ColumnarTable> replaceTotalCellValuesFunction = t -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t,
            rows.stream().map(Field::name).toList(),
            columns.stream().map(Field::name).toList());
    ColumnarTable table = execute(first, second, joinType, replaceTotalCellValuesFunction, executor);
    List<String> values = table.headers().stream().filter(Header::isMeasure).map(Header::name).toList();
    return new PivotTable(table, rows.stream().map(Field::name).toList(), columns.stream().map(Field::name).toList(), values);
  }

  private static ColumnarTable execute(QueryDto first,
                                       QueryDto second,
                                       JoinType joinType,
                                       Function<Table, ColumnarTable> replaceTotalCellValuesFunction,
                                       Function<QueryDto, Pair<Table, QueryResolver>> executor) {




    CompletableFuture<Pair<Table, QueryResolver>> f1 = CompletableFuture.supplyAsync(() -> executor.apply(first));
    CompletableFuture<Pair<Table, QueryResolver>> f2 = CompletableFuture.supplyAsync(() -> executor.apply(second));
    try {
      return CompletableFuture.allOf(f1, f2)
              .thenApply(__ -> {
                final Pair<Table, QueryResolver> r1 = f1.join();
                final Pair<Table, QueryResolver> r2 = f2.join();
                ColumnarTable table = (ColumnarTable) MergeTables.mergeTables(r1.getOne(), r2.getOne(), joinType);
                table = replaceTotalCellValuesFunction.apply(table);
                final Map<String, Comparator<?>> firstComparators = r1.getTwo().squashqlComparators();
                final Map<String, Comparator<?>> secondComparators = r2.getTwo().squashqlComparators();
                secondComparators.putAll(firstComparators); // the comparators of the first query take precedence over the second's
                final Set<CompiledColumnSet> columnSets = Stream
                        .concat(r1.getTwo().getCompiledColumnSets().values().stream(), r2.getTwo().getCompiledColumnSets().values().stream())
                        .collect(Collectors.toSet());
                return (ColumnarTable) TableUtils.orderRows(table, secondComparators, columnSets, r1.getTwo().useDefaultComparator() && r2.getTwo().useDefaultComparator());
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
