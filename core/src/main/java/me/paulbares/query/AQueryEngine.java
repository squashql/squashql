package me.paulbares.query;

import me.paulbares.dto.QueryDto;
import me.paulbares.query.context.Totals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AQueryEngine implements QueryEngine {

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
        if (i == 0 && current == null) {
          // GT
          newHeaders[i] = GRAND_TOTAL;
        } else if (i >= 1 && objects.get(i - 1) != null && current == null) {
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

    return new ArrayTable(dataset.fields(), newRows);
  }
}
