package io.squashql.query.database;

import com.google.common.collect.Ordering;
import io.squashql.list.Lists;
import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.compiled.CompiledOrderBy;
import io.squashql.query.compiled.CteRecordTable;
import io.squashql.store.UnknownType;
import io.squashql.type.TypedField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SQLTranslator {

  public static final String TOTAL_CELL = "___total___";

  public static String translate(DatabaseQuery query) {
    return translate(query, new DefaultQueryRewriter(query));
  }

  public static String translate(DatabaseQuery dq, QueryRewriter queryRewriter) {
    QueryScope query = dq.scope();
    List<String> selects = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    List<String> aggregates = new ArrayList<>();

    query.columns().forEach(f -> {
      selects.add(queryRewriter.select(f));
      groupBy.add(queryRewriter.groupBy(f));
    });
    dq.measures().forEach(m -> aggregates.add(m.sqlExpression(queryRewriter, true))); // Alias is needed when using sub-queries

    selects.addAll(aggregates);

    StringBuilder statement = new StringBuilder();
    addCtes(query.cteRecordTables(), statement, queryRewriter);
    statement.append("select ");
    statement.append(String.join(", ", selects));
    statement.append(" from ");
    statement.append(query.table().sqlExpression(queryRewriter));
    addWhereConditions(statement, dq, queryRewriter);
    if (!query.groupingSets().isEmpty()) {
      addGroupingSets(query.groupingSets().stream().map(g -> g.stream().map(queryRewriter::rollup).toList()).toList(), statement);
    } else {
      addGroupByAndRollup(groupBy, query.rollup().stream().map(queryRewriter::rollup).toList(), queryRewriter.usePartialRollupSyntax(), statement);
    }
    addHavingConditions(statement, query.havingCriteria(), queryRewriter);
    addOrderBy(statement, query.orderBy(), queryRewriter);
    addLimit(query.limit(), statement);
    return statement.toString();
  }

  private static void addCtes(List<CteRecordTable> cteRecordTables, StringBuilder statement, QueryRewriter qr) {
    if (cteRecordTables == null || cteRecordTables.isEmpty()) {
      return;
    }
    statement.append("with ");
    statement.append(String.join(", ", cteRecordTables.stream().map(t -> t.sqlExpression(qr)).toList())).append(" ");
  }

  private static void addOrderBy(StringBuilder statement, List<CompiledOrderBy> orderBy, QueryRewriter qr) {
    if (!orderBy.isEmpty()) {
      statement.append(" order by ");
      statement.append(String.join(", ", orderBy.stream().map(t -> t.sqlExpression(qr)).toList())).append(" ");
    }
  }

  public static void addLimit(int limit, StringBuilder statement) {
    if (limit > 0) {
      statement.append(" limit ").append(limit);
    }
  }

  private static void addGroupByAndRollup(List<String> groupBy, List<String> rollup, boolean supportPartialRollup, StringBuilder statement) {
    if (groupBy.isEmpty()) {
      return;
    }
    checkRollupIsValid(groupBy, rollup);

    statement.append(" group by ");

    boolean isPartialRollup = !Set.copyOf(groupBy).equals(Set.copyOf(rollup));
    boolean hasRollup = !rollup.isEmpty();

    List<String> groupByOnly = new ArrayList<>();
    List<String> rollupOnly = new ArrayList<>();

    for (String s : groupBy) {
      if (hasRollup && rollup.contains(s)) {
        rollupOnly.add(s);
      } else {
        groupByOnly.add(s);
      }
    }

    // Order in the rollup is important.
    Ordering<String> explicit = Ordering.explicit(rollup);
    rollupOnly.sort(explicit);

    if (hasRollup && isPartialRollup && !supportPartialRollup) {
      List<String> groupingSets = new ArrayList<>();
      groupingSets.add(groupBy.stream().collect(Collectors.joining(", ", "(", ")")));
      List<String> toRemove = new ArrayList<>();
      Collections.reverse(rollupOnly);
      // The equivalent of group by scenario, rollup(category, subcategory) is:
      // (scenario, category, subcategory), (scenario, category), (scenario)
      for (String r : rollupOnly) {
        toRemove.add(r);
        List<String> copy = new ArrayList<>(groupBy);
        copy.removeAll(toRemove);
        groupingSets.add(copy.stream().collect(Collectors.joining(", ", "(", ")")));
      }

      statement
              .append("grouping sets ")
              .append(groupingSets.stream().collect(Collectors.joining(", ", "(", ")")));
    } else {
      statement.append(String.join(", ", groupByOnly));

      if (hasRollup) {
        if (!groupByOnly.isEmpty()) {
          statement.append(", ");
        }
        statement.append(rollupOnly.stream().collect(Collectors.joining(", ", "rollup(", ")")));
      }
    }
  }

  private static void addGroupingSets(List<List<String>> groupingSets, StringBuilder statement) {
    statement.append(" group by grouping sets(");

    for (int i = 0; i < groupingSets.size(); i++) {
      statement.append('(');
      statement.append(String.join(",", groupingSets.get(i)));
      statement.append(')');
      if (i < groupingSets.size() - 1) {
        statement.append(", ");
      }
    }

    statement.append(")");
  }

  protected static void addWhereConditions(StringBuilder statement, DatabaseQuery query, QueryRewriter queryRewriter) {
    if (query.scope().whereCriteria() != null) {
      String whereClause = query.scope().whereCriteria().sqlExpression(queryRewriter);
      if (whereClause != null) {
        statement
                .append(" where ")
                .append(whereClause);
      }
    }
  }

  public static Function<Object, String> getQuoteFn(TypedField field, QueryRewriter queryRewriter) {
    if (Number.class.isAssignableFrom(field.type())
            || field.type().equals(double.class)
            || field.type().equals(int.class)
            || field.type().equals(long.class)
            || field.type().equals(float.class)
            || field.type().equals(boolean.class)
            || field.type().equals(Boolean.class)
            || field.type().equals(UnknownType.class)
            || field.type().equals(Lists.LongList.class)) {
      // no quote
      return String::valueOf;
    } else if (field.type().equals(String.class) || field.type().equals(Lists.StringList.class)) {
      // quote
      return s -> "'" + queryRewriter.escapeSingleQuote(String.valueOf(s)) + "'";
    } else {
      throw new RuntimeException("Not supported " + field.type());
    }
  }

  protected static void addHavingConditions(StringBuilder statement, CompiledCriteria havingCriteria, QueryRewriter queryRewriter) {
    if (havingCriteria != null) {
      final String havingClause = havingCriteria.sqlExpression(queryRewriter);
      if (havingClause != null) {
        statement
                .append(" having ")
                .append(havingClause);
      }
    }
  }

  public static void checkRollupIsValid(List<String> select, List<String> rollup) {
    if (!rollup.isEmpty() && Collections.disjoint(select, rollup)) {
      throw new RuntimeException(String.format("The columns contain in rollup %s must be a subset of the columns contain in the select %s", rollup, select));
    }
  }
}
