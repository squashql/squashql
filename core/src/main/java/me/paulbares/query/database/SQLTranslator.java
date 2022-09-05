package me.paulbares.query.database;

import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.*;
import me.paulbares.store.Field;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static me.paulbares.query.database.SqlUtils.escape;

public class SQLTranslator {

  private static final DefaultQueryRewriter DEFAULT_QUERY_REWRITER = new DefaultQueryRewriter();

  public static String translate(DatabaseQuery query, Function<String, Field> fieldProvider) {
    return translate(query, null, fieldProvider, DEFAULT_QUERY_REWRITER);
  }

  public static String translate(DatabaseQuery query, Totals totals, Function<String, Field> fieldProvider) {
    return translate(query, totals, fieldProvider, DEFAULT_QUERY_REWRITER);
  }

  public static String translate(DatabaseQuery query,
                                 Totals totals,
                                 Function<String, Field> fieldProvider,
                                 QueryRewriter queryRewriter) {
    List<String> selects = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    List<String> aggregates = new ArrayList<>();

    query.coordinates.forEach((field, values) -> groupBy.add(escape(field)));
    query.measures.forEach(m -> aggregates.add(m.sqlExpression(fieldProvider, queryRewriter, true)));

    groupBy.forEach(selects::add); // coord first, then aggregates
    aggregates.forEach(selects::add);

    StringBuilder statement = new StringBuilder();
    statement.append("select ");
    statement.append(selects.stream().collect(Collectors.joining(", ")));
    statement.append(" from ");
    if (query.subQuery != null) {
      statement.append("(");
      statement.append(translate(query.subQuery, totals, fieldProvider, queryRewriter));
      statement.append(")");
    } else {
      statement.append(queryRewriter.tableName(query.table.name));

      addJoins(statement, query.table, queryRewriter);
      addConditions(statement, query, fieldProvider);

      if (!groupBy.isEmpty()) {
        statement.append(" group by ");
        if (totals != null) {
          statement.append("rollup(");
        }
        statement.append(groupBy.stream().collect(Collectors.joining(", ")));

        if (totals != null) {
          statement.append(") order by ");
          String order = " asc"; // default for now
          // https://stackoverflow.com/a/7862601
          // to move totals and subtotals at the top or at the bottom and keep normal order for other rows.
          String position = totals.position == null ? Totals.POSITION_TOP : totals.position; // default top
          // Note: with Spark, values of totals are set to null but for Clickhouse, they are set to '' for string type,
          // 0 for integer... this is why there is the following case condition (for clickhouse, only string type is
          // handled
          // for the moment).
          String orderBy = "case when %s is null or %s = '' then %d else %d end, %s %s";
          int first = position.equals(Totals.POSITION_TOP) ? 0 : 1;
          int second = first ^ 1;
          String orderByStatement = groupBy.stream()
                  .map(g -> orderBy.formatted(g, g, first, second, g, order))
                  .collect(Collectors.joining(", "));
          statement.append(orderByStatement);
        }
      }
    }
    return statement.toString();
  }

  private static Map<String, ConditionDto> extractConditions(DatabaseQuery query) {
    Map<String, ConditionDto> conditionByField = new HashMap<>();
    query.coordinates.forEach((field, values) -> {
      if (values != null && values.size() == 1) {
        conditionByField.put(field, new SingleValueConditionDto(ConditionType.EQ, values.get(0)));
      } else if (values != null && values.size() > 1) {
        conditionByField.put(field, new InConditionDto(values));
      }
    });

    query.conditions.forEach((field, condition) -> {
      ConditionDto old = conditionByField.get(field);
      if (old != null) {
        throw new IllegalArgumentException(String.format("A condition for field %s already exists %s", field, old));
      }
      conditionByField.put(field, condition);
    });

    return conditionByField;
  }

  protected static void addConditions(StringBuilder statement, DatabaseQuery query, Function<String, Field> fieldProvider) {
    Map<String, ConditionDto> conditionByField = extractConditions(query);

    if (!conditionByField.isEmpty()) {
      String andConditions = conditionByField.entrySet()
              .stream()
              .map(e -> toSql(fieldProvider.apply(e.getKey()), e.getValue()))
              .collect(Collectors.joining(" and "));
      statement
              .append(" where ")
              .append(andConditions);
    }
  }

  private static void addJoins(StringBuilder statement, TableDto tableQuery, QueryRewriter queryRewriter) {
    for (JoinDto join : tableQuery.joins) {
      statement
              .append(" ")
              .append(join.type)
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
              || field.type().equals(float.class)) {
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
        default -> throw new IllegalStateException("Unexpected value: " + dto.type());
      };
    } else if (dto instanceof LogicalConditionDto logical) {
      String first = toSql(field, logical.one);
      String second = toSql(field, logical.two);
      String typeString = switch (dto.type()) {
        case AND -> " and "; // TODO unloop consecutive and
        case OR -> " or "; // TODO unloop consecutive and
        default -> throw new IllegalStateException("Incorrect type " + logical.type);
      };
      return first + typeString + second;
    } else {
      throw new RuntimeException("Not supported condition " + dto);
    }
  }
}
