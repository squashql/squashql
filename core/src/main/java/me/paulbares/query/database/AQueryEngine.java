package me.paulbares.query.database;

import me.paulbares.query.ColumnarTable;
import me.paulbares.query.CountMeasure;
import me.paulbares.query.Table;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static me.paulbares.query.database.SQLTranslator.groupingAlias;

public abstract class AQueryEngine<T extends Datastore> implements QueryEngine<T> {

  public final T datastore;

  public final Function<String, Field> fieldSupplier;

  protected AQueryEngine(T datastore) {
    this.datastore = datastore;
    this.fieldSupplier = createFieldSupplier();
  }

  protected Function<String, Field> createFieldSupplier() {
    return fieldName -> {
      for (Store store : this.datastore.storesByName().values()) {
        for (Field field : store.fields()) {
          if (field.name().equals(fieldName)) {
            return field;
          }
        }
      }

      if (fieldName.equals(CountMeasure.INSTANCE.alias())) {
        return new Field(CountMeasure.INSTANCE.alias(), long.class);
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

  protected abstract Table retrieveAggregates(DatabaseQuery query);

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
    Table aggregates = retrieveAggregates(query);
    return postProcessDataset(aggregates, query);
  }

  /**
   * Changes the content of the input table to remove columns corresponding to grouping() (columns that help to identify
   * rows containing totals) and write {@link SQLTranslator#TOTAL_CELL} in the corresponding cells. The modifications
   * happen in-place i.e in the input table columns directly.
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
      List<Field> newFields = new ArrayList<>();
      List<List<Object>> newValues = new ArrayList<>();
      for (int i = 0; i < input.headers().size(); i++) {
        Field header = input.headers().get(i);
        List<Object> columnValues = input.getColumn(i);
        if (i < query.select.size() || i >= query.select.size() + query.rollup.size()) {
          newFields.add(header);
          newValues.add(columnValues);
        } else {
          String baseName = Objects.requireNonNull(SqlUtils.extractFieldFromGroupingAlias(header.name()));
          List<Object> baseColumnValues = input.getColumnValues(baseName);
          for (int rowIndex = 0; rowIndex < columnValues.size(); rowIndex++) {
            if (((Number) columnValues.get(rowIndex)).longValue() == 1) {
              // It is a total if == 1. It is cast as Number because the type is Byte with Spark, Long with ClickHouse...
              baseColumnValues.set(rowIndex, SQLTranslator.TOTAL_CELL);
            }
          }
        }
      }

      return new ColumnarTable(
              newFields,
              input.measures(),
              IntStream.range(query.select.size(), query.select.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.select.size()).toArray(),
              newValues);
    } else {
      return input;
    }
  }

  public static <Column, Record> Pair<List<Field>, List<List<Object>>> transform(
          DatabaseQuery query,
          List<Column> columns,
          BiFunction<Column, String, Field> columnToField,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue) {
    List<String> fieldNames = new ArrayList<>();
    query.select.forEach(fieldNames::add);
    query.rollup.forEach(r -> fieldNames.add(groupingAlias(r)));
    query.measures.forEach(m -> fieldNames.add(m.alias()));

    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      fields.add(columnToField.apply(columns.get(i), fieldNames.get(i)));
    }
    List<List<Object>> values = new ArrayList<>();
    fields.forEach(f -> values.add(new ArrayList<>()));
    recordIterator.forEachRemaining(r -> {
      for (int i = 0; i < fields.size(); i++) {
        values.get(i).add(recordToFieldValue.apply(i, r));
      }
    });
    return Tuples.pair(fields, values);
  }
}
