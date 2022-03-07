package me.paulbares.query;

import me.paulbares.query.dto.ScenarioComparisonDto;
import me.paulbares.query.dto.ScenarioGroupingQueryDto;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScenarioGroupingExecutor {

  public static final String GROUP_NAME = "group";
  public static final String COMPARISON_METHOD_ABS_DIFF = "absolute_difference";
  public static final String COMPARISON_METHOD_REL_DIFF = "relative_difference";
  public static final String REF_POS_PREVIOUS = "previous";
  public static final String REF_POS_FIRST = "first";

  public final QueryEngine queryEngine;
  public final ScenarioGroupingCache queryCache;

  public ScenarioGroupingExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
    this.queryCache = new ScenarioGroupingCache(queryEngine);
  }

  public Table execute(ScenarioGroupingQueryDto query) {
    Table table = this.queryCache.get(query);
    Map<String, List<Object>> valuesByScenario = new HashMap<>();
    for (List<Object> row : table) {
      valuesByScenario.put((String) row.get(0), row.subList(1, row.size()));
    }

    List<Field> tableFields = createTableFields(query, table.headers());

    long count = query.groups.entrySet().stream().flatMap(e -> e.getValue().stream()).count();
    List<List<Object>> rows = FastList.newList((int) count);
    query.groups.forEach((group, scenarios) -> {
      for (String scenario : scenarios) {
        List<Object> row = FastList.newListWith(group, scenario);

        for (int i = 0; i < query.comparisons.size(); i++) {
          ScenarioComparisonDto comp = query.comparisons.get(i);

          // Gets the value that current value is being compared to
          Object referenceValue = getReferenceValue(i, comp.referencePosition(), scenario, scenarios, valuesByScenario);
          Object currentValue = valuesByScenario.get(scenario).get(i);

          Object newValue = switch (comp.method()) {
            case COMPARISON_METHOD_ABS_DIFF -> computeAbsoluteDiff(currentValue, referenceValue,
                    table.headers().get(i + 1).type());
            case COMPARISON_METHOD_REL_DIFF -> computeRelativeDiff(currentValue, referenceValue,
                    table.headers().get(i + 1).type());
            default -> throw new IllegalArgumentException(String.format("Not supported comparison %s", comp.method()));
          };

          row.add(newValue);
          if (comp.showValue()) {
            row.add(currentValue); // the original value
          }
        }
        rows.add(row);
      }
    });

    return new ArrayTable(tableFields, rows);
  }

  private Object getReferenceValue(
          int measureIndex,
          String referencePosition,
          String scenario,
          List<String> scenarios,
          Map<String, List<Object>> valuesByScenario) {
    return switch (referencePosition) {
      case REF_POS_PREVIOUS -> {
        int index = scenarios.indexOf(scenario); // will never be negative by design
        String previousScenario = scenarios.get(Math.max(index - 1, 0));
        yield valuesByScenario.get(previousScenario).get(measureIndex);
      }
      case REF_POS_FIRST -> {
        String firstScenario = scenarios.get(0);
        yield valuesByScenario.get(firstScenario).get(measureIndex);
      }
      default -> throw new IllegalArgumentException(String.format("Not supported reference position %s",
              referencePosition));
    };
  }

  private List<Field> createTableFields(ScenarioGroupingQueryDto query, List<Field> rawFields) {
    List<Field> fields = FastList.newListWith(new Field(GROUP_NAME, String.class));
    for (int i = 0; i < rawFields.size(); i++) {
      Field rawField = rawFields.get(i);
      if (i == 0) {
        fields.add(rawField); // first is scenario field
      } else {
        ScenarioComparisonDto comparison = query.comparisons.get(i - 1);
        String newName = String.format("%s(%s, %s)",
                comparison.method(),
                comparison.measure().alias(),
                comparison.referencePosition());
        fields.add(new Field(newName, rawField.type()));
        if (comparison.showValue()) {
          fields.add(rawField);
        }
      }
    }
    return fields;
  }

  private double computeRelativeDiff(Object current, Object previous, Class<?> dataType) {
    if (dataType.equals(Double.class) || dataType.equals(double.class)) {
      return (((double) current) - ((double) previous)) / ((double) previous);
    } else if (dataType.equals(Float.class) || dataType.equals(float.class)) {
      return (((float) current) - ((float) previous)) / ((float) previous);
    } else if (dataType.equals(Integer.class) || dataType.equals(int.class)) {
      return (double) (((int) current) - ((int) previous)) / ((long) previous);
    } else if (dataType.equals(Long.class) || dataType.equals(long.class)) {
      return (double) (((long) current) - ((long) previous)) / ((long) previous);
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
