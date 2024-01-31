package io.squashql.query;

import io.squashql.query.compiled.CompiledColumnSet;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.query.exception.LimitExceedException;
import io.squashql.table.*;
import io.squashql.util.Queries;
import lombok.RequiredArgsConstructor;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
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
    return execute(first, second, joinType, (t, r) -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t, true), executor).table;
  }

  public static PivotTable executePivotQueryMerge(QueryExecutor queryExecutor, QueryDto first, QueryDto second, List<Field> rows, List<Field> columns, JoinType joinType, SquashQLUser user) {
    Function<QueryDto, Pair<Table, QueryResolver>> executor = query -> {
      Set<Field> columnsFromColumnSets = query.columnSets.values().stream().flatMap(cs -> cs.getNewColumns().stream()).collect(Collectors.toSet());
      final Pair<PivotTable, QueryResolver> pivotTable = queryExecutor.executePivotQueryInternal(
              new PivotTableQueryDto(query,
                      rows.stream().filter(r -> query.columns.contains(r) || columnsFromColumnSets.contains(r)).toList(),
                      columns.stream().filter(r -> query.columns.contains(r) || columnsFromColumnSets.contains(r)).toList()),
              CacheStatsDto.builder(),
              user,
              false,
              limit -> {
                throw new LimitExceedException("Result of " + query + " is too big (limit=" + limit + ")");
              });

      return Tuples.pair(pivotTable.getOne().table, pivotTable.getTwo());
    };

    BiFunction<Table, CombinedResolver, ColumnarTable> replaceTotalCellValuesFunction = (t, r) -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t,
            rows.stream().map(r::expression).toList(),
            columns.stream().map(r::expression).toList());
    MergeResult result = execute(first, second, joinType, replaceTotalCellValuesFunction, executor);
    List<String> values = result.table.headers().stream().filter(Header::isMeasure).map(Header::name).toList();
    return new PivotTable(result.table, rows.stream().map(result.resolver::expression).toList(), columns.stream().map(result.resolver::expression).toList(), values);
  }

  private static MergeResult execute(QueryDto first,
                                       QueryDto second,
                                       JoinType joinType,
                                       BiFunction<Table, CombinedResolver, ColumnarTable> replaceTotalCellValuesFunction,
                                       Function<QueryDto, Pair<Table, QueryResolver>> executor) {
    Map<String, Comparator<?>> firstComparators = Queries.getComparators(first);
    Map<String, Comparator<?>> secondComparators = Queries.getComparators(second);
    secondComparators.putAll(firstComparators); // the comparators of the first query take precedence over the second's

    CompletableFuture<Pair<Table, QueryResolver>> f1 = CompletableFuture.supplyAsync(() -> executor.apply(first));
    CompletableFuture<Pair<Table, QueryResolver>> f2 = CompletableFuture.supplyAsync(() -> executor.apply(second));
    try {
      return CompletableFuture.allOf(f1, f2)
              .thenApply(__ -> {
                ColumnarTable table = (ColumnarTable) MergeTables.mergeTables(f1.join().getOne(), f2.join().getOne(), joinType);
                final CombinedResolver resolver = new CombinedResolver(List.of(f1.join().getTwo(), f2.join().getTwo()));
                table = replaceTotalCellValuesFunction.apply(table, resolver);
                Set<CompiledColumnSet> columnSets = Stream
                        .concat(f1.join().getTwo().getCompiledColumnSets().values().stream(),
                                f2.join().getTwo().getCompiledColumnSets().values().stream())
                        .collect(Collectors.toSet());
                return new MergeResult((ColumnarTable) TableUtils.orderRows(table, secondComparators, columnSets), resolver);
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

  @RequiredArgsConstructor
  private static class CombinedResolver {
    private final List<QueryResolver> resolvers;

    String expression(Field field) {
      for(QueryResolver resolver : resolvers) {
        try {
          return SqlUtils.squashqlExpression(resolver.resolveField(field));
        } catch (FieldNotFoundException ignored) {
          // absorb exception
        }
      }
      throw new FieldNotFoundException("Cannot find field with name " + field);
    }
  }

  @RequiredArgsConstructor
  private static class MergeResult {
    private final ColumnarTable table;
    private final CombinedResolver resolver;
  }
}
