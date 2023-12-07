package io.squashql.query.database;

import io.squashql.query.Header;
import io.squashql.query.QueryExecutor;
import io.squashql.query.compiled.DatabaseQuery2;
import io.squashql.store.Datastore;
import io.squashql.store.Store;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import io.squashql.util.Queries;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

@Slf4j
public abstract class AQueryEngine<T extends Datastore> implements QueryEngine<T> {

  public final T datastore;

  protected final QueryRewriter queryRewriter;

  protected AQueryEngine(T datastore, QueryRewriter queryRewriter) {
    this.datastore = datastore;
    this.queryRewriter = queryRewriter;
  }

  @Override
  public QueryRewriter queryRewriter() {
    return this.queryRewriter;
  }

  @Override
  public T datastore() {
    return this.datastore;
  }

  @Override
  public Table execute(DatabaseQuery2 query, QueryExecutor.PivotTableContext context) {
    if (query.table != null) {
      String tableName = query.table.name();
      // Can be null if sub-query
      Store store = this.datastore.storesByName().get(tableName);
      if (store == null) {
        throw new IllegalArgumentException(String.format("Cannot find table with name %s. Available tables: %s",
                tableName, this.datastore.storesByName().values().stream().map(Store::name).toList()));
      }
    }
    String sql = createSqlStatement(query, context);
    log.info(query + " translated into " + System.lineSeparator() + "sql=" + sql);
    Table aggregates = retrieveAggregates(query, sql);
    return postProcessDataset(aggregates, query);
  }

  protected String createSqlStatement(DatabaseQuery2 query, QueryExecutor.PivotTableContext context) {
    return SQLTranslator.translate(query, this.queryRewriter);
  }

  protected abstract Table retrieveAggregates(DatabaseQuery2 query, String sql);

  /**
   * Changes the content of the input table to remove columns corresponding to grouping() (columns that help to identify
   * rows containing totals) and write {@link SQLTranslator#TOTAL_CELL} in the corresponding cells. The modifications
   * happen in-place i.e. in the input table columns directly.
   * <pre>
   *   Input:
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   | scenario | category | ___grouping___scenario___ | ___grouping___category___ |    p | _contributors_count_ |  q |
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   |     base |    drink |                         0 |                         0 |  2.0 |                    1 | 10 |
   *   |     base |     food |                         0 |                         0 |  3.0 |                    1 | 20 |
   *   |     base |    cloth |                         0 |                         0 | 10.0 |                    1 |  3 |
   *   |     null |     null |                         1 |                         1 | 15.0 |                    3 | 33 |
   *   |     base |     null |                         0 |                         1 | 15.0 |                    3 | 33 |
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   Output:
   *   +-------------+-------------+------+----------------------+----+
   *   |    scenario |    category |    p | _contributors_count_ |  q |
   *   +-------------+-------------+------+----------------------+----+
   *   |        base |       drink |  2.0 |                    1 | 10 |
   *   |        base |        food |  3.0 |                    1 | 20 |
   *   |        base |       cloth | 10.0 |                    1 |  3 |
   *   | ___total___ | ___total___ | 15.0 |                    3 | 33 |
   *   |        base | ___total___ | 15.0 |                    3 | 33 |
   *   +-------------+-------------+------+----------------------+----+
   * </pre>
   */
  protected Table postProcessDataset(Table input, DatabaseQuery2 query) {
    List<TypedField> groupingSelects = Queries.generateGroupingSelect(query);
    if (this.queryRewriter.useGroupingFunction() && !groupingSelects.isEmpty()) {
      List<Header> newHeaders = new ArrayList<>();
      List<List<Object>> newValues = new ArrayList<>();
      for (int i = 0; i < input.headers().size(); i++) {
        Header header = input.headers().get(i);
        List<Object> columnValues = input.getColumn(i);
        if (i < query.select.size() || i >= query.select.size() + groupingSelects.size()) {
          newHeaders.add(header);
          newValues.add(columnValues);
        } else {
          String baseName = Objects.requireNonNull(SqlUtils.extractFieldFromGroupingAlias(header.name()));
          List<Object> baseColumnValues = input.getColumnValues(baseName);
          for (int rowIndex = 0; rowIndex < columnValues.size(); rowIndex++) {
            if (((Number) columnValues.get(rowIndex)).longValue() == 1) {
              // It is a total if == 1. It is cast as Number because the type is Byte with Spark, Long with
              // ClickHouse...
              baseColumnValues.set(rowIndex, SQLTranslator.TOTAL_CELL);
            }
          }
        }
      }

      return new ColumnarTable(
              newHeaders,
              input.measures(),
              newValues);
    } else {
      return input;
    }
  }

  public <Column, Record> Pair<List<Header>, List<List<Object>>> transformToColumnFormat(
          DatabaseQuery2 query,
          List<Column> columns,
          BiFunction<Column, String, Class<?>> columnTypeProvider,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue) {
    List<Header> headers = new ArrayList<>();
    Function<TypedField, String> typedFieldStringFunction = f -> {
      if (f instanceof TableTypedField ttf) {
        return SqlUtils.getFieldFullName(ttf);
      } else if (f instanceof FunctionTypedField ftf) {
        return SqlUtils.singleOperandFunctionName(ftf.function(), SqlUtils.getFieldFullName(ftf.field()));
      } else {
        throw new IllegalArgumentException(f.getClass().getName());
      }
    };
    List<String> fieldNames = new ArrayList<>(query.select.stream().map(typedFieldStringFunction).toList());
    List<TypedField> groupingSelects = Queries.generateGroupingSelect(query);
    if (this.queryRewriter.useGroupingFunction()) {
      groupingSelects.forEach(r -> fieldNames.add(SqlUtils.groupingAlias(typedFieldStringFunction.apply(r))));
    }
    query.measures.forEach(m -> fieldNames.add(m.alias()));
    List<List<Object>> values = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      headers.add(new Header(
              fieldNames.get(i),
              columnTypeProvider.apply(columns.get(i), fieldNames.get(i)),
              i >= query.select.size() + (this.queryRewriter.useGroupingFunction() ? groupingSelects.size() : 0)));
      values.add(new ArrayList<>());
    }
    recordIterator.forEachRemaining(r -> {
      for (int i = 0; i < headers.size(); i++) {
        values.get(i).add(recordToFieldValue.apply(i, r));
      }
    });
    return Tuples.pair(headers, values);
  }

  public static <Column, Record> Pair<List<Header>, List<List<Object>>> transformToRowFormat(
          List<Column> columns,
          Function<Column, String> columnNameProvider,
          Function<Column, Class<?>> columnTypeProvider,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue) {
    List<Header> headers = columns.stream().map(column -> new Header(columnNameProvider.apply(column), columnTypeProvider.apply(column), false)).toList();
    List<List<Object>> rows = new ArrayList<>();
    recordIterator.forEachRemaining(r -> rows.add(
            IntStream.range(0, headers.size()).mapToObj(i -> recordToFieldValue.apply(i, r)).toList()));
    return Tuples.pair(headers, rows);
  }

}
