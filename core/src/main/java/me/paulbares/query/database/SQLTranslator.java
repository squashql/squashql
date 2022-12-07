package me.paulbares.query.database;

import me.paulbares.query.dto.*;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static me.paulbares.query.database.SqlUtils.escape;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

public class SQLTranslator {

  public static final String TOTAL_CELL = "___total___";

  private static final DefaultQueryRewriter DEFAULT_QUERY_REWRITER = new DefaultQueryRewriter();

  public static String translate(DatabaseQuery query, Function<String, Field> fieldProvider) {
    return translate(query, fieldProvider, DEFAULT_QUERY_REWRITER, (qr, name) -> qr.tableName(name));
  }

  public static String translate(DatabaseQuery query,
                                 Function<String, Field> fieldProvider,
                                 QueryRewriter queryRewriter,
                                 BiFunction<QueryRewriter, String, String> tableTransformer) {
    List<String> selects = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    List<String> aggregates = new ArrayList<>();

    query.select.forEach(field -> groupBy.add(escape(field)));
    query.measures.forEach(m -> aggregates.add(m.sqlExpression(fieldProvider, queryRewriter, true)));

    groupBy.forEach(selects::add); // coord first, then aggregates
    query.rollup.forEach(field -> selects.add(String.format("grouping(%s) as %s", escape(field), groupingAlias(field)))); // use grouping to identify totals
    aggregates.forEach(selects::add);

    StringBuilder statement = new StringBuilder();
    statement.append("select ");
    statement.append(selects.stream().collect(Collectors.joining(", ")));
    statement.append(" from ");
    if (query.subQuery != null) {
      statement.append("(");
      statement.append(translate(query.subQuery, fieldProvider, queryRewriter, tableTransformer));
      statement.append(")");
    } else {
      statement.append(tableTransformer.apply(queryRewriter, query.table.name));
      addJoins(statement, query.table, queryRewriter);
    }
    addConditions(statement, query, fieldProvider);
    addGroupByAndRollup(groupBy, query.rollup.stream().map(SqlUtils::escape).toList(), queryRewriter.doesSupportPartialRollup(), statement);
    return statement.toString();
  }

  private static void addGroupByAndRollup(List<String> groupBy, List<String> rollup, boolean supportPartialRollup, StringBuilder statement) {
    if (groupBy.isEmpty()) {
      return;
    }

    statement.append(" group by ");

    boolean isPartialRollup = !Set.copyOf(groupBy).equals(Set.copyOf(rollup));
    boolean hasRollup = rollup != null && !rollup.isEmpty();
    List<String> groupByOnly = new ArrayList<>();
    List<String> rollupOnly = new ArrayList<>();

    for (String s : groupBy) {
      if (hasRollup && rollup.contains(s)) {
        rollupOnly.add(s);
      } else {
        groupByOnly.add(s);
      }
    }

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
      statement.append(groupByOnly.stream().collect(Collectors.joining(", ")));

      if (hasRollup) {
        if (!groupByOnly.isEmpty()) {
          statement.append(", ");
        }
        statement.append(rollupOnly.stream().collect(Collectors.joining(", ", "rollup(", ")")));
      }
    }
  }

  protected static void addConditions(StringBuilder statement, DatabaseQuery query, Function<String, Field> fieldProvider) {
    if (query.criteriaDto != null) {
      String whereClause = toSql(fieldProvider, query.criteriaDto);
      if (whereClause != null) {
        statement
                .append(" where ")
                .append(whereClause);
      }
    }
  }

  private static void addJoins(StringBuilder statement, TableDto tableQuery, QueryRewriter queryRewriter) {
    for (JoinDto join : tableQuery.joins) {
      statement
              .append(" ")
              .append(join.type.name().toLowerCase())
              .append(" join ")
              .append(queryRewriter.tableName(join.table.name))
              .append(" on ");
      for (int i = 0; i < join.mappings.size(); i++) {
        JoinMappingDto mapping = join.mappings.get(i);
        statement
                .append(queryRewriter.tableName(mapping.fromTable)).append('.').append(mapping.from)
                .append(" = ")
                .append(queryRewriter.tableName(mapping.toTable)).append('.').append(mapping.to);
        if (i < join.mappings.size() - 1) {
          statement.append(" and ");
        }
      }

      if (!join.table.joins.isEmpty()) {
        addJoins(statement, join.table, queryRewriter);
      }
    }
  }

  public static String toSql(Field field, ConditionDto dto) {
    if (dto instanceof SingleValueConditionDto || dto instanceof InConditionDto) {
      Function<Object, String> sqlMapper;
      if (Number.class.isAssignableFrom(field.type())
              || field.type().equals(double.class)
              || field.type().equals(int.class)
              || field.type().equals(long.class)
              || field.type().equals(float.class)
              || field.type().equals(boolean.class)) {
        // no quote
        sqlMapper = o -> String.valueOf(o);
      } else if (field.type().equals(String.class)) {
        // quote
        sqlMapper = s -> "'" + s + "'";
      } else {
        throw new RuntimeException("Not supported " + field.type());
      }

      String escape = escape(field.name());
      return switch (dto.type()) {
        case IN -> escape + " in (" +
                ((InConditionDto) dto).values
                        .stream()
                        .map(sqlMapper)
                        .collect(Collectors.joining(", ")) + ")";
        case EQ -> escape + " = " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case NEQ -> escape + " <> " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case LT -> escape + " < " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case LE -> escape + " <= " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case GT -> escape + " > " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case GE -> escape + " >= " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case LIKE -> escape + " like " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        default -> throw new IllegalStateException("Unexpected value: " + dto.type());
      };
    } else if (dto instanceof LogicalConditionDto logical) {
      String first = toSql(field, logical.one);
      String second = toSql(field, logical.two);
      String typeString = switch (dto.type()) {
        case AND -> " and "; // TODO unnest nested and (and (and (and...))) = (and and and)
        case OR -> " or "; // TODO unnest nested or
        default -> throw new IllegalStateException("Incorrect type " + logical.type);
      };
      return first + typeString + second;
    } else if (dto instanceof ConstantConditionDto cc) {
      String escape = escape(field.name());
      return switch (cc.type()) {
        case NULL -> escape + " is null";
        case NOT_NULL -> escape + " is not null";
        default -> throw new IllegalStateException("Unexpected value: " + dto.type());
      };
    } else {
      throw new RuntimeException("Not supported condition " + dto);
    }
  }

  public static String toSql(Function<String, Field> fieldProvider, CriteriaDto criteriaDto) {
    if (criteriaDto.isCriterion()) {
      return toSql(fieldProvider.apply(criteriaDto.field), criteriaDto.condition);
    } else if (!criteriaDto.children.isEmpty()) {
      String sep = switch (criteriaDto.conditionType) {
        case AND -> " and ";
        case OR -> " or ";
        default -> throw new IllegalStateException("Unexpected value: " + criteriaDto.conditionType);
      };
      Iterator<CriteriaDto> iterator = criteriaDto.children.iterator();
      List<String> conditions = new ArrayList<>();
      while (iterator.hasNext()) {
        String c = toSql(fieldProvider, iterator.next());
        if (c != null) {
          conditions.add(c);
        }
      }
      return conditions.isEmpty() ? null : ("(" + String.join(sep, conditions) + ")");
    } else {
      return null;
    }
  }

  public static String virtualTableStatementWhereNotIn(String baseTableName, List<String> scenarios, List<String> columnKeys, QueryRewriter qr) {
    List<String> vtScenarios = new ArrayList<>(scenarios.size());
    for (String scenarioName : scenarios) {
      String scenarioStoreName = TransactionManager.scenarioStoreName(baseTableName, scenarioName);
      String keys = String.join(",", columnKeys);
      String sql = "SELECT *, '" + scenarioName + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + "\n" +
              "FROM " + qr.tableName(baseTableName) + " WHERE (" + keys + ") NOT IN ( SELECT " + keys + " FROM " + qr.tableName(scenarioStoreName) + " )\n" +
              "UNION ALL\n" +
              "SELECT *, '" + scenarioName + "' FROM " + qr.tableName(scenarioStoreName) + "";
      vtScenarios.add(sql);
    }
    String sqlBase = "SELECT *, '" + MAIN_SCENARIO_NAME + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + " FROM " + qr.tableName(baseTableName);

    String virtualTable = sqlBase;
    for (String vtScenario : vtScenarios) {
      virtualTable += "\n" + "UNION ALL\n" + vtScenario;
    }
    return virtualTable;
  }

  public static String virtualTableStatementWhereNotExists(String baseTableName, List<String> scenarios, List<String> columnKeys, QueryRewriter qr) {
    List<String> vtScenarios = new ArrayList<>(scenarios.size());
    for (String scenarioName : scenarios) {
      String scenarioStoreName = TransactionManager.scenarioStoreName(baseTableName, scenarioName);
      StringBuilder condition = new StringBuilder();
      for (int i = 0; i < columnKeys.size(); i++) {
        String key = columnKeys.get(i);
        condition.append(baseTableName).append('.').append(key)
                .append(" = ")
                .append(scenarioStoreName).append('.').append(key);
        if (i < columnKeys.size() - 1) {
          condition.append(" AND ");
        }
      }
      String sql = "SELECT *, '" + scenarioName + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + "\n" +
              "FROM " + qr.tableName(baseTableName) + " WHERE NOT EXISTS ( SELECT 1 FROM " + qr.tableName(scenarioStoreName) + " WHERE " + condition + " )\n" +
              "UNION ALL\n" +
              "SELECT *, '" + scenarioName + "' FROM " + qr.tableName(scenarioStoreName) + "";
      vtScenarios.add(sql);
    }
    String sqlBase = "SELECT *, '" + MAIN_SCENARIO_NAME + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + " FROM " + qr.tableName(baseTableName);

    String virtualTable = sqlBase;
    for (String vtScenario : vtScenarios) {
      virtualTable += "\nUNION ALL\n" + vtScenario;
    }
    return virtualTable;
  }

  /**
   * Returns the name of the column used for grouping(). If it is modified, please modify also
   * {@link SqlUtils#GROUPING_PATTERN}.
   */
  public static String groupingAlias(String field) {
    return String.format(escape("___grouping___%s___"), field);
  }
}
