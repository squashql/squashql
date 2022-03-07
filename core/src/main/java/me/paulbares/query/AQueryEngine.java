package me.paulbares.query;

import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public abstract class AQueryEngine implements QueryEngine {

  public final Datastore datastore;

  public final Function<String, Field> fieldSupplier;

  protected AQueryEngine(Datastore datastore) {
    this.datastore = datastore;
    this.fieldSupplier = fieldName -> {
      for (Store store : this.datastore.stores()) {
        for (Field field : store.getFields()) {
          if (field.name().equals(fieldName)) {
            return field;
          }
        }
      }
      throw new IllegalArgumentException("Cannot find field with name " + fieldName);
    };
  }

  protected abstract Table retrieveAggregates(QueryDto query);

  @Override
  public Table execute(QueryDto query) {
    Table aggregates = retrieveAggregates(query);
    return postProcessDataset(aggregates, query);
  }

  protected Table postProcessDataset(Table initialTable, QueryDto query) {
    if (!query.context.containsKey(Totals.KEY)) {
      return initialTable;
    }

    return editTotalsAndSubtotals(initialTable, query);
  }

  protected Table editTotalsAndSubtotals(Table dataset, QueryDto query) {
    Iterator<List<Object>> rowIterator = dataset.iterator();

    List<List<Object>> newRows = new ArrayList<>((int) dataset.count());
    List<String> headers = new ArrayList<>(query.coordinates.keySet());
    while (rowIterator.hasNext()) {
      List<Object> objects = new ArrayList<>(rowIterator.next());

      Object[] newHeaders = new String[headers.size()];
      for (int i = 0; i < headers.size(); i++) {
        Object current = objects.get(i);
        if (i == 0 && isTotal(current)) {
          // GT
          newHeaders[i] = GRAND_TOTAL;
        } else if (i >= 1 && !isTotal(objects.get(i - 1)) && isTotal(current)) {
          // Total
          newHeaders[i] = TOTAL;
        } else {
          newHeaders[i] = current; // nothing to change
        }
      }

      for (int i = 0; i < newHeaders.length; i++) {
        objects.set(i, newHeaders[i]);
      }
      newRows.add(objects);
    }

    return new ArrayTable(dataset.headers(), newRows);
  }

  protected boolean isTotal(Object current) {
    return current == null || (current instanceof String s && s.isEmpty());
  }
}
