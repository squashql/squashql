package me.paulbares.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.Repository;
import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.ConditionType;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.SingleValueConditionDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.util.*;
import java.util.function.Function;

import static me.paulbares.query.QueryBuilder.eq;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

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
    addScenarioConditionIfNecessary(query);
    replaceScenarioFieldName(query);
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

    return new ArrayTable(dataset.headers(), newRows);
  }

  protected boolean isTotal(Object current) {
    // See why the second condition in SQLTranslator: case when %s is null or %s = ''...
    return current == null || (current instanceof String s && s.isEmpty());
  }

  /**
   * Adds a condition on {@link Datastore#SCENARIO_FIELD_NAME} if not in the query.
   * Default {@link Datastore#SCENARIO_FIELD_NAME} is {@link Datastore#MAIN_SCENARIO_NAME}.
   */
  protected void addScenarioConditionIfNecessary(QueryDto query) {
    if (!query.coordinates.containsKey(SCENARIO_FIELD_NAME)) {
      ConditionDto c = query.conditions.get(SCENARIO_FIELD_NAME);
      if (c == null) {
        // If no condition, default to base by adding a condition and let Spark handle it :)
        query.condition(SCENARIO_FIELD_NAME, eq(Datastore.MAIN_SCENARIO_NAME));
      } else {
        // Only support single value condition. Otherwise, it does not make sense.
        if (!(c instanceof SingleValueConditionDto s
                && (s.type == ConditionType.EQ || (s.type == ConditionType.IN && ((Set<Object>) s.value).size() == 1)))) {
          String format = String.format("""
                  Query %s is not correct. Field s% should be in the coordinates or if not in a
                  single value condition.
                  """, query, SCENARIO_FIELD_NAME);
          throw new IllegalArgumentException(format);
        }
      }
    }
  }

  private void replaceScenarioFieldName(QueryDto query) {
    String scenarioFieldName = this.datastore.storesByName().get(query.table.name).scenarioFieldName();
    ConditionDto cond = query.conditions.remove(SCENARIO_FIELD_NAME);
    if (cond != null) {
      query.conditions.put(scenarioFieldName, cond);
    }

    // Order here is important.
    if (query.coordinates.containsKey(SCENARIO_FIELD_NAME)) {
      List<String> coord = query.coordinates.get(SCENARIO_FIELD_NAME);
      Map<String, List<String>> newCoords = new LinkedHashMap<>();
      for (Map.Entry<String, List<String>> entry : query.coordinates.entrySet()) {
        if (entry.getKey().equals(SCENARIO_FIELD_NAME)) {
          newCoords.put(scenarioFieldName, coord);
        } else {
          newCoords.put(entry.getKey(), entry.getValue());
        }
      }
      query.coordinates = newCoords;
    }
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
