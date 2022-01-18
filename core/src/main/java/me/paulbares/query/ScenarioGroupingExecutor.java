package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ScenarioGroupingExecutor {

  protected final QueryEngine queryEngine;

  public ScenarioGroupingExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table execute(ScenarioGroupingQuery query) {
    ComparisonMethod comparisonMethod = query.comparisonMethod;
    Map<String, List<String>> groups = query.groups;

    Query simpleQuery = new Query().addWildcardCoordinate("scenario");
    simpleQuery.measures.addAll(query.measures);

    Table table = this.queryEngine.execute(simpleQuery);

    Map<String, List<Object>> rowByScenario = new HashMap<>();
    // Index row per scenario
    Iterator<List<Object>> rowIterator = table.iterator();
    while (rowIterator.hasNext()) {
      List<Object> row = rowIterator.next();
      rowByScenario.put((String) row.get(0), row);
    }

    List<Field> newFields = new ArrayList<>();
    newFields.add(new Field("group", String.class));
    List<Field> rawFields = table.fields();
    for (int i = 0; i < rawFields.size(); i++) {
      Field rawField = rawFields.get(i);
      if (i == 0) {
        newFields.add(rawField);
      } else {
        String newName = comparisonMethod.name().substring(0, 3).toLowerCase() + ". diff. " + rawField.name();
        newFields.add(new Field(newName, rawField.type()));
      }
    }

    List<List<Object>> newRows = new ArrayList<>();
    List<Object>[] previous = new List[1];

    groups.forEach((group, scenarios) -> {
      List<Object> base = rowByScenario.get("base");
      previous[0] = null;
      scenarios.forEach(scenario -> {
        List<Object> row = rowByScenario.get(scenario);
        if (row == null) {
          return;
        }

        if (previous[0] == null) {
          previous[0] = base;
        }

        List<Object> elements = new ArrayList<>();
        elements.add(group);
        elements.add(row.get(0));

        for (int i = 1; i < row.size(); i++) {
          Class<?> rawField = rawFields.get(i).type();
          Object newValue = switch (comparisonMethod) {
            case ABSOLUTE -> computeAbsoluteDiff(row.get(i), previous[0].get(i), rawField);
            case RELATIVE -> computeRelativeDiff(row.get(i), previous[0].get(i), rawField);
          };
          elements.add(newValue);
        }
        newRows.add(elements);

        previous[0] = rowByScenario.get(scenario);
      });
    });

    return new ArrayTable(newFields, newRows);
  }

  private Object computeRelativeDiff(Object current, Object previous, Class<?> dataType) {
    if (dataType.equals(Double.class) || dataType.equals(double.class)) {
      return (((double) current) - ((double) previous)) / ((double) previous);
    } else if (dataType.equals(Float.class) || dataType.equals(float.class)) {
      return (((float) current) - ((float) previous)) / ((float) previous);
    } else if (dataType.equals(Integer.class) || dataType.equals(int.class)) {
      return (((int) current) - ((int) previous)) / ((int) previous);
    } else if (dataType.equals(Long.class) || dataType.equals(long.class)) {
      return (((long) current) - ((long) previous)) / ((long) previous);
    } else {
      throw new RuntimeException("Unsupported type " + dataType);
    }
  }

  private Object computeAbsoluteDiff(Object current, Object previous, Class<?> dataType) {
    if (dataType.equals(Double.class) || dataType.equals(double.class)) {
      return ((double) current) - ((double) previous);
    } else if (dataType.equals(Float.class) || dataType.equals(float.class)) {
      return ((float) current) - ((float) previous);
    } else if (dataType.equals(Integer.class) || dataType.equals(int.class)) {
      return ((int) current) - ((int) previous);
    } else if (dataType.equals(Long.class) || dataType.equals(long.class)) {
      return ((long) current) - ((long) previous);
    } else {
      throw new RuntimeException("Unsupported type " + dataType);
    }
  }
}
