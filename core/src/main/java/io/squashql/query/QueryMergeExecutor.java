package io.squashql.query;

import io.squashql.query.compiled.CompiledColumnSet;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.QueryMergeDto;
import io.squashql.query.exception.LimitExceedException;
import io.squashql.table.*;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryMergeExecutor {

  public static Table executeQueryMerge(QueryExecutor queryExecutor, QueryMergeDto queryMerge, SquashQLUser user) {
    Function<QueryDto, Pair<Table, QueryResolver>> executor = query -> queryExecutor.executeQueryInternal(
            query,
            CacheStatsDto.builder(),
            user,
            false,
            limit -> {
              throw new LimitExceedException("Result of " + query + " is too big (limit=" + limit + ")");
            });
    return execute(queryMerge, t -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t, true), executor);
  }

  public static PivotTable executePivotQueryMerge(QueryExecutor queryExecutor, QueryMergeDto queryMerge, List<Field> rows, List<Field> columns, SquashQLUser user) {
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
    ColumnarTable table = execute(queryMerge, replaceTotalCellValuesFunction, executor);
    List<String> values = table.headers().stream().filter(Header::isMeasure).map(Header::name).toList();
    return new PivotTable(table, rows.stream().map(Field::name).toList(), columns.stream().map(Field::name).toList(), values);
  }

  private static ColumnarTable execute(QueryMergeDto queryMerge,
                                       Function<Table, ColumnarTable> replaceTotalCellValuesFunction,
                                       Function<QueryDto, Pair<Table, QueryResolver>> executor) {




    final List<CompletableFuture<Pair<Table, QueryResolver>>> futures = new ArrayList<>();
    for (QueryDto q : queryMerge.queries) {
      futures.add(CompletableFuture.supplyAsync(() -> executor.apply(q)));
    }

    try {
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
              .thenApply(__ -> {
                final List<Pair<Table, QueryResolver>> results = futures.stream().map(CompletableFuture::join).toList();
                ColumnarTable table = (ColumnarTable) MergeTables.mergeTables(results.stream().map(Pair::getOne).toList(), queryMerge.joinTypes);
                table = replaceTotalCellValuesFunction.apply(table);
                final Map<String, Comparator<?>> comparators = new HashMap<>();
                final Set<CompiledColumnSet> columnSets = new HashSet<>();
                boolean useDefaultComparator = true;
                for (int i = results.size() - 1; i >= 0; i--) {
                  comparators.putAll(results.get(i).getTwo().squashqlComparators()); // the comparators of the first query take precedence over the second's
                  columnSets.addAll(results.get(i).getTwo().getCompiledColumnSets().values());
                  useDefaultComparator &= results.get(i).getTwo().useDefaultComparator();
                }
                return (ColumnarTable) TableUtils.orderRows(table, comparators, columnSets, useDefaultComparator);
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
