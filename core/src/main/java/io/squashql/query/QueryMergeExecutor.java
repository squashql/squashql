package io.squashql.query;

import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.query.exception.LimitExceedException;
import io.squashql.store.Datastore;
import io.squashql.table.*;
import io.squashql.util.Queries;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    return execute(queryMerge,
            t -> (ColumnarTable) TableUtils.replaceTotalCellValues((ColumnarTable) t, true),
            executor,
            queryExecutor.queryEngine.datastore());
  }

  public static PivotTable executePivotQueryMerge(QueryExecutor queryExecutor, PivotTableQueryMergeDto pivotTableQueryMergeDto, SquashQLUser user) {
    List<Field> rows = pivotTableQueryMergeDto.rows;
    List<Field> columns = pivotTableQueryMergeDto.columns;
    Function<QueryDto, Table> executor = query -> {
      Set<Field> columnsFromColumnSets = query.columnSets.values().stream().flatMap(cs -> cs.getNewColumns().stream()).collect(Collectors.toSet());
      List<Field> localRows = getLocalFields(rows, query, columnsFromColumnSets);
      List<Field> localColumns = getLocalFields(columns, query, columnsFromColumnSets);
      query.minify = false;

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
    ColumnarTable table = execute(pivotTableQueryMergeDto.query, replaceTotalCellValuesFunction, executor, queryExecutor.queryEngine.datastore());
    List<String> values = table.headers().stream().filter(Header::isMeasure).map(Header::name).toList();
    return new PivotTable(table,
            rows.stream().map(SqlUtils::squashqlExpression).toList(),
            columns.stream().map(SqlUtils::squashqlExpression).toList(),
            values,
            Collections.emptyList()); // not supported for now
  }

  private static List<Field> getLocalFields(List<Field> elements, QueryDto query, Set<Field> columnsFromColumnSets) {
    List<Field> localElements = new ArrayList<>();
    for (Field element : elements) {
      Stream.concat(query.columns.stream(), columnsFromColumnSets.stream())
              .filter(f -> SqlUtils.squashqlExpression(f).equals(SqlUtils.squashqlExpression(element)))
              .findFirst()
              .ifPresent(localElements::add); // add the column from the select for the check done later.
    }
    return localElements;
  }

  private static ColumnarTable execute(QueryMergeDto queryMerge,
                                       Function<Table, ColumnarTable> replaceTotalCellValuesFunction,
                                       Function<QueryDto, Table> executor,
                                       Datastore datastore) {
    Map<String, Comparator<?>> comparators = new HashMap<>();
    Set<ColumnSet> columnSets = new HashSet<>();
    for (int i = queryMerge.queries.size() - 1; i >= 0; i--) {
      QueryDto q = queryMerge.queries.get(i);
      QueryResolver qr = new QueryResolver(q, datastore.storesByName());
      // all = true because the order by is computed by SquashQL only (results size is inferior to limit)
      comparators.putAll(Queries.getSquashQLComparators(qr, true)); // the comparators of the first query take precedence over the second's
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
                return (ColumnarTable) TableUtils.orderRows(table, comparators, false, columnSets);
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
