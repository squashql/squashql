package me.paulbares.query.database;

import me.paulbares.query.Table;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class AQueryEngine<T extends Datastore> implements QueryEngine<T> {

  public final T datastore;

  public final Function<String, Field> fieldSupplier;

  protected AQueryEngine(T datastore) {
    this.datastore = datastore;
    this.fieldSupplier = fieldName -> {
      for (Store store : this.datastore.storesByName().values()) {
        for (Field field : store.fields()) {
          if (field.name().equals(fieldName)) {
            return field;
          }
        }
      }
      throw new IllegalArgumentException("Cannot find field with name " + fieldName);
    };
  }

  @Override
  public T datastore() {
    return this.datastore;
  }

  protected abstract Table retrieveAggregates(DatabaseQuery query);

  @Override
  public Table execute(DatabaseQuery query) {
    Store store = this.datastore.storesByName().get(query.table.name);
    if (store == null) {
      throw new IllegalArgumentException(String.format("Cannot find table with name %s. Available tables: %s",
              query.table.name, this.datastore.storesByName().values().stream().map(Store::name).toList()));
    }
    Table aggregates = retrieveAggregates(query);
    return postProcessDataset(aggregates, query);
  }

  protected Table postProcessDataset(Table initialTable, DatabaseQuery query) {
    return initialTable;
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
