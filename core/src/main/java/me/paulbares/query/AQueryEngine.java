package me.paulbares.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.Repository;
import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  protected abstract Table retrieveAggregates(QueryDto query);

  @Override
  public Table execute(QueryDto query) {
    Store store = this.datastore.storesByName().get(query.table.name);
    if (store == null) {
      throw new IllegalArgumentException(String.format("Cannot find table with name %s. Available tables: %s",
              query.table.name, this.datastore.storesByName().values().stream().map(Store::name).toList()));
    }
    resolveMeasures(query);
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
      boolean isTotal = false;
      for (int i = 0; i < headers.size(); i++) {
        Object current = objects.get(i);
        if (i == 0 && isTotal(current)) {
          // GT
          newHeaders[i] = GRAND_TOTAL;
          isTotal = true;
        } else if (i >= 1 && !isTotal(objects.get(i - 1)) && isTotal(current)) {
          // Total
          newHeaders[i] = TOTAL;
          isTotal = true;
        } else {
          newHeaders[i] = isTotal ? null : current; // if total on row, replace element with null to handle "" and other default values
        }
      }

      for (int i = 0; i < newHeaders.length; i++) {
        objects.set(i, newHeaders[i]);
      }
      newRows.add(objects);
    }

    return new ArrayTable(dataset.headers(), dataset.measures(), dataset.measureIndices(), newRows);
  }

  protected boolean isTotal(Object current) {
    // See why the second condition in SQLTranslator: case when %s is null or %s = ''...
    return current == null || (current instanceof String s && s.isEmpty());
  }

  protected void resolveMeasures(QueryDto queryDto) {
    ContextValue repo = queryDto.context.get(Repository.KEY);

    Supplier<Map<String, ExpressionMeasure>> supplier = Suppliers.memoize(() -> ExpressionResolver.get(((Repository) repo).url));

    List<Measure> newMeasures = new ArrayList<>();
    for (Measure measure : queryDto.measures) {
      if (measure instanceof UnresolvedExpressionMeasure) {
        if (repo == null) {
          throw new IllegalStateException(Repository.class.getSimpleName() + " context is missing in the query");
        }
        String alias = ((UnresolvedExpressionMeasure) measure).alias;
        ExpressionMeasure expressionMeasure = supplier.get().get(alias);
        if (expressionMeasure == null) {
          throw new IllegalArgumentException("Cannot find expression with alias " + alias);
        }
        newMeasures.add(expressionMeasure);
      } else {
        newMeasures.add(measure);
      }
    }
    queryDto.measures = newMeasures;
  }
}
