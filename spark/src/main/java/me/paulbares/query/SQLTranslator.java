package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.Totals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static me.paulbares.query.SqlUtils.escape;

public class SQLTranslator {

  public static String translate(Query query) {
    List<String> selects = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    List<String> conditions = new ArrayList<>();

    List<String> aggregates = new ArrayList<>();
    query.coordinates.forEach((field, values) -> {
      groupBy.add(escape(field));
      if (values == null) {
        // wildcard
      } else if (values.size() == 1) {
        conditions.add(escape(field) + " = '" + values.get(0) + "'");
      } else {
        conditions.add(escape(field) + " in (" + values.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", ")) + ")");
      }
    });
    query.measures.forEach(m -> aggregates.add(m.sqlExpression()));


    groupBy.forEach(selects::add); // coord first, then aggregates
    aggregates.forEach(selects::add);

    StringBuilder statement = new StringBuilder();
    statement.append("select ");
    statement.append(selects.stream().collect(Collectors.joining(", ")));
    statement.append(" from ");
    statement.append(SparkDatastore.BASE_STORE_NAME);
    if (!conditions.isEmpty()) {
      statement.append(" where ").append(conditions.stream().collect(Collectors.joining(" and ")));
    }
    if (!groupBy.isEmpty()) {
      statement.append(" group by ");

      ContextValue totals = query.context.get(Totals.KEY);
      if (totals != null) {
        statement.append("rollup(");
      }
      String groupByStatement = groupBy.stream().collect(Collectors.joining(", "));
      statement.append(groupByStatement);
      if (totals != null) {
        Totals cv = (Totals) totals;
        statement.append(") order by ");
        String order = " asc"; // default for now
        // https://stackoverflow.com/a/7862601
        // to move totals and subtotals at the top or at the bottom and keep normal order for other rows.
        String position = cv.position == null ? Totals.POSITION_TOP : cv.position; // default top
        String orderBy = "case when %s is null then %d else %d end, %s %s";
        int first = position.equals(Totals.POSITION_TOP) ? 0 : 1;
        int second = first ^ 1;
        String orderByStatement = groupBy.stream().map(g -> orderBy.formatted(g, first, second, g, order)).collect(Collectors.joining(", "));
        statement.append(orderByStatement);
      }
    }
    return statement.toString();
  }
}