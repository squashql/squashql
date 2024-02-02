package io.squashql.query;

import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.QueryMergeDto;
import io.squashql.query.exception.LimitExceedException;
import io.squashql.table.*;
import io.squashql.util.Queries;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryMergeExecutor {

  public static Table executeQueryMerge(QueryExecutor queryExecutor, QueryMergeDto queryMerge, SquashQLUser user) {
    Function<QueryDto, Table> executor = query -> queryExecutor.executeQuery(
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
    Function<QueryDto, Table> executor = query -> {
      Set<Field> columnsFromColumnSets = query.columnSets.values().stream().flatMap(cs -> cs.getNewColumns().stream()).collect(Collectors.toSet());
      List<Field> localRows = getLocalFields(rows, query, columnsFromColumnSets);
      List<Field> localColumns = getLocalFields(columns, query, columnsFromColumnSets);

      return queryExecutor.executePivotQuery(
              new PivotTableQueryDto(query, localRows, localColumns),
              CacheStatsDto.builder(),
              user,
              false,
              limit -> {
                throw new LimitExceedException("Result of " + query + " is too big (limit=" + limit + ")");
              })
              .table;
    };

    Function<Table, ColumnarTable> replaceTotalCellValuesFunction = t -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t,
            rows.stream().map(SqlUtils::squashqlExpression).toList(),
            columns.stream().map(SqlUtils::squashqlExpression).toList());
    ColumnarTable table = execute(queryMerge, replaceTotalCellValuesFunction, executor);
    List<String> values = table.headers().stream().filter(Header::isMeasure).map(Header::name).toList();
    return new PivotTable(table, rows.stream().map(SqlUtils::squashqlExpression).toList(), columns.stream().map(SqlUtils::squashqlExpression).toList(), values);
  }

  private static List<Field> getLocalFields(List<Field> rows, QueryDto query, Set<Field> columnsFromColumnSets) {
    List<Field> localRows = new ArrayList<>();
    for (Field row : rows) {
      // FIXME replace with check on squashQLExpression
      if (row instanceof AliasedField af) {
        for (Field column : query.columns) {
          if (af.alias().equals(column.alias())) {
            localRows.add(column); // add the column from the select.
            break;
          }
        }
      } else if (query.columns.contains(row) || columnsFromColumnSets.contains(row)) {
        localRows.add(row);
      }
    }
    return localRows;
  }

  private static ColumnarTable execute(QueryMergeDto queryMerge,
                                       Function<Table, ColumnarTable> replaceTotalCellValuesFunction,
                                       Function<QueryDto, Table> executor) {
    Map<String, Comparator<?>> comparators = new HashMap<>();
    Set<ColumnSet> columnSets = new HashSet<>();
    for (int i = queryMerge.queries.size() - 1; i >= 0; i--) {
      QueryDto q = queryMerge.queries.get(i);
      comparators.putAll(Queries.getComparators(q)); // the comparators of the first query take precedence over the second's
      columnSets.addAll(q.columnSets.values());
    }

    List<CompletableFuture<Table>> futures = new ArrayList<>();
    for (QueryDto q : queryMerge.queries) {
      futures.add(CompletableFuture.supplyAsync(() -> executor.apply(q)));
    }

    try {
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
              .thenApply(__ -> {
                ColumnarTable table = (ColumnarTable) MergeTables.mergeTables(futures.stream().map(CompletableFuture::join).toList(), queryMerge.joinTypes);
                table = replaceTotalCellValuesFunction.apply(table);
                return (ColumnarTable) TableUtils.orderRows(table, comparators, columnSets);
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
