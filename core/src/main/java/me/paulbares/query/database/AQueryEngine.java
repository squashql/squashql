package me.paulbares.query.database;

import me.paulbares.query.ColumnarTable;
import me.paulbares.query.CountMeasure;
import me.paulbares.query.Table;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

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

  protected Table postProcessDataset(Table initialTable, DatabaseQuery query) {
    if (!query.rollUp.isEmpty()) {
      List<Field> newFields = new ArrayList<>();
      List<List<Object>> newValues = new ArrayList<>();
      for (int i = 0; i < initialTable.headers().size(); i++) {
        Field header = initialTable.headers().get(i);
        List<Object> columnValues = initialTable.getColumn(i);
        if (i < query.select.size() || i >= query.select.size() + query.rollUp.size()) {
          newFields.add(header);
          newValues.add(columnValues);
        } else {
          String baseName = Objects.requireNonNull(SqlUtils.extractGroupingField(header.name()));
          List<Object> baseColumnValues = initialTable.getColumnValues(baseName);
          for (int rowIndex = 0; rowIndex < columnValues.size(); rowIndex++) {
            if (((Number) columnValues.get(rowIndex)).longValue() == 1) {  // It is a total
              baseColumnValues.set(rowIndex, SQLTranslator.TOTAL_CELL);
            }
          }
        }
      }

      return new ColumnarTable(
              newFields,
              initialTable.measures(),
              IntStream.range(query.select.size(), query.select.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.select.size()).toArray(),
              newValues);
    } else {
      return initialTable;
    }
  }

  public static <Column, Record> Pair<List<Field>, List<List<Object>>> transform(
          List<Column> columns,
          Function<Column, Field> columnToField,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue) {
    List<Field> fields = columns.stream().map(columnToField::apply).toList();
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
