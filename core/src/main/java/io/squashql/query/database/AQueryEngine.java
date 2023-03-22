package io.squashql.query.database;

import io.squashql.query.*;
import io.squashql.store.Datastore;
import io.squashql.store.Field;
import io.squashql.store.Store;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

@Slf4j
public abstract class AQueryEngine<T extends Datastore> implements QueryEngine<T> {

  public final T datastore;

  public final Function<String, Field> fieldSupplier;

  protected final QueryRewriter queryRewriter;

  protected AQueryEngine(T datastore, QueryRewriter queryRewriter) {
    this.datastore = datastore;
    this.fieldSupplier = createFieldSupplier(this.datastore.storesByName());
    this.queryRewriter = queryRewriter;
  }

  @Override
  public QueryRewriter queryRewriter() {
    return this.queryRewriter;
  }

  public static Function<String, Field> createFieldSupplier(Map<String, Store> storesByName) {
    return fieldName -> {
      String[] split = fieldName.split("\\.");
      if (split.length > 1) {
        String tableName = split[0];
        String fieldNameInTable = split[1];
        Store store = storesByName.get(tableName);
        if (store != null) {
          for (Field field : store.fields()) {
            if (field.name().equals(fieldNameInTable)) {
              return field;
            }
          }
        }
      } else {
        for (Store store : storesByName.values()) {
          for (Field field : store.fields()) {
            if (field.name().equals(fieldName)) {
              // We omit on purpose the store name. It will be determined by the underlying SQL engine of the DB.
              // if any ambiguity, the DB will raise an exception.
              return new Field(null, field.name(), field.type());
            }
          }
        }
      }

      if (fieldName.equals(CountMeasure.INSTANCE.alias())) {
        return new Field(null, CountMeasure.INSTANCE.alias(), long.class);
      }
      throw new IllegalArgumentException("Cannot find field with name " + fieldName);
    };
  }

  @Override
  public Function<String, Field> getFieldSupplier() {
    return this.fieldSupplier;
  }

  @Override
  public T datastore() {
    return this.datastore;
  }

  protected abstract Table retrieveAggregates(DatabaseQuery query, String sql);

  @Override
  public Table execute(DatabaseQuery query) {
    if (query.table != null) {
      String tableName = query.table.name;
      // Can be null if sub-query
      Store store = this.datastore.storesByName().get(tableName);
      if (store == null) {
        throw new IllegalArgumentException(String.format("Cannot find table with name %s. Available tables: %s",
                tableName, this.datastore.storesByName().values().stream().map(Store::name).toList()));
      }
    }
    String sql = createSqlStatement(query);
    log.info(query + " translated into sql='" + sql + "'");
    Table aggregates = retrieveAggregates(query, sql);
    return postProcessDataset(aggregates, query);
  }

  protected String createSqlStatement(DatabaseQuery query) {
    return SQLTranslator.translate(query,
            QueryExecutor.withFallback(this.fieldSupplier, String.class),
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
    if (!query.rollup.isEmpty()) {
      List<Header> newHeaders = new ArrayList<>();
      List<List<Object>> newValues = new ArrayList<>();
      for (int i = 0; i < input.headers().size(); i++) {
        Header header = input.headers().get(i);
        List<Object> columnValues = input.getColumn(i);
        if (i < query.select.size() || i >= query.select.size() + query.rollup.size()) {
          newHeaders.add(header);
          newValues.add(columnValues);
        } else {
          String baseName = Objects.requireNonNull(SqlUtils.extractFieldFromGroupingAlias(header.field().name()));
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
          BiFunction<Column, String, Field> columnToField,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue,
          QueryRewriter queryRewriter) {
    List<Header> headers = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>(query.select.stream().map(SqlUtils::getFieldFullName).toList());
    if (queryRewriter.useGroupingFunction()) {
      query.rollup.forEach(r -> fieldNames.add(SqlUtils.groupingAlias(SqlUtils.getFieldFullName(r))));
    }
    query.measures.forEach(m -> fieldNames.add(m.alias()));
    for (int i = 0; i < columns.size(); i++) {
      headers.add(new Header(
              columnToField.apply(columns.get(i), fieldNames.get(i)),
              i >= query.select.size() + (queryRewriter.useGroupingFunction() ? query.rollup.size() : 0)));
    }
    List<List<Object>> values = new ArrayList<>();
    headers.forEach(f -> values.add(new ArrayList<>()));
    recordIterator.forEachRemaining(r -> {
      for (int i = 0; i < headers.size(); i++) {
        values.get(i).add(recordToFieldValue.apply(i, r));
      }
    });
    return Tuples.pair(headers, values);
  }

  public static <Column, Record> Pair<List<Header>, List<List<Object>>> transformToRowFormat(
          List<Column> columns,
          Function<Column, Field> columnToField,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue) {
    List<Header> headers = columns.stream().map(column -> new Header(columnToField.apply(column), false)).toList();
    List<List<Object>> rows = new ArrayList<>();
    recordIterator.forEachRemaining(r -> rows.add(
            IntStream.range(0, headers.size()).mapToObj(i -> recordToFieldValue.apply(i, r)).toList()));
    return Tuples.pair(headers, rows);
  }
}
