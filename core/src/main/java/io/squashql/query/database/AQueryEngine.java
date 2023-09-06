package io.squashql.query.database;

import io.squashql.query.CountMeasure;
import io.squashql.query.Header;
import io.squashql.query.QueryExecutor;
import io.squashql.query.date.DateFunctions;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.store.Datastore;
import io.squashql.store.Store;
import io.squashql.store.TypedField;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.util.Queries;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

@Slf4j
public abstract class AQueryEngine<T extends Datastore> implements QueryEngine<T> {

  public final T datastore;

  protected final Supplier<Function<String, TypedField>> fieldSupplier;

  protected final QueryRewriter queryRewriter;

  protected AQueryEngine(T datastore, QueryRewriter queryRewriter) {
    this.datastore = datastore;
    this.fieldSupplier = () -> createFieldSupplier(this.datastore.storesByName());
    this.queryRewriter = queryRewriter;
  }

  @Override
  public QueryRewriter queryRewriter() {
    return this.queryRewriter;
  }

  public static Function<String, TypedField> createFieldSupplier(Map<String, Store> storesByName) {
    return fieldName -> {
      final String cleansedFieldName = DateFunctions.extractFieldFromDateFunctionOrReturn(fieldName);
      String[] split = cleansedFieldName.split("\\.");
      if (split.length > 1) {
        String tableName = split[0];
        String fieldNameInTable = split[1];
        Store store = storesByName.get(tableName);
        if (store != null) {
          for (TypedField field : store.fields()) {
            if (field.name().equals(fieldNameInTable)) {
              return cleansedFieldName.equals(fieldName) ? field : new TypedField(tableName, fieldName, field.type());
            }
          }
        }
      } else {
        for (Store store : storesByName.values()) {
          for (TypedField field : store.fields()) {
            if (field.name().equals(cleansedFieldName)) {
              // We omit on purpose the store name. It will be determined by the underlying SQL engine of the DB.
              // if any ambiguity, the DB will raise an exception.
              return new TypedField(null, fieldName, field.type());
            }
          }
        }
      }

      if (fieldName.equals(CountMeasure.INSTANCE.alias())) {
        return new TypedField(null, CountMeasure.INSTANCE.alias(), long.class);
      }
      throw new FieldNotFoundException("Cannot find field with name " + fieldName);
    };
  }

  @Override
  public Function<String, TypedField> getFieldSupplier() {
    return this.fieldSupplier.get();
  }

  @Override
  public T datastore() {
    return this.datastore;
  }

  protected abstract Table retrieveAggregates(DatabaseQuery query, String sql);

  @Override
  public Table execute(DatabaseQuery query, QueryExecutor.PivotTableContext context) {
    if (query.table != null) {
      String tableName = query.table.name;
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

  protected String createSqlStatement(DatabaseQuery query, QueryExecutor.PivotTableContext context) {
    return SQLTranslator.translate(query,
            QueryExecutor.createQueryFieldSupplier(this, query.virtualTableDto),
            this.queryRewriter);
  }

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
  protected Table postProcessDataset(Table input, DatabaseQuery query) {
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

  public static <Column, Record> Pair<List<Header>, List<List<Object>>> transformToColumnFormat(
          DatabaseQuery query,
          List<Column> columns,
          BiFunction<Column, String, String> columnNameProvider,
          BiFunction<Column, String, Class<?>> columnTypeProvider,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue,
          QueryRewriter queryRewriter) {
    List<Header> headers = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>(query.select.stream().map(f -> {
      final String cleansedFieldName = DateFunctions.extractFieldFromDateFunctionOrReturn(f.name());
      return cleansedFieldName.equals(f.name()) ? SqlUtils.getFieldFullName(f) : f.name();
    }).toList());
    List<TypedField> groupingSelects = Queries.generateGroupingSelect(query);
    if (queryRewriter.useGroupingFunction()) {
      groupingSelects.forEach(r -> fieldNames.add(SqlUtils.groupingAlias(SqlUtils.getFieldFullName(r))));
    }
    query.measures.forEach(m -> fieldNames.add(m.alias()));
    List<List<Object>> values = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      headers.add(new Header(
              columnNameProvider.apply(columns.get(i), fieldNames.get(i)),
              columnTypeProvider.apply(columns.get(i), fieldNames.get(i)),
              i >= query.select.size() + (queryRewriter.useGroupingFunction() ? groupingSelects.size() : 0)));
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
