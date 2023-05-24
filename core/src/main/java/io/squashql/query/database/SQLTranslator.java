package io.squashql.query.database;

import com.google.common.collect.Ordering;
import io.squashql.query.dto.*;
import io.squashql.store.Field;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SQLTranslator {

  public static final String TOTAL_CELL = "___total___";

  public static String translate(DatabaseQuery query, Function<String, Field> fieldProvider) {
    return translate(query, fieldProvider, DefaultQueryRewriter.INSTANCE);
  }

  public static String translate(DatabaseQuery query,
                                 Function<String, Field> fieldProvider,
                                 QueryRewriter queryRewriter) {
    QueryAwareQueryRewriter qr = new QueryAwareQueryRewriter(queryRewriter, query);
    return translate(query, fieldProvider, __ -> qr);
  }

  /**
   * Be careful when using this method directly. You may have to leverage {@link QueryAwareQueryRewriter} somehow.
   */
  public static String translate(DatabaseQuery query,
                                 Function<String, Field> fieldProvider,
                                 Function<DatabaseQuery, QueryRewriter> queryRewriterSupplier) {
    QueryRewriter queryRewriter = queryRewriterSupplier.apply(query);
    List<String> selects = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    List<String> aggregates = new ArrayList<>();

    query.select.forEach(f -> groupBy.add(queryRewriter.select(f)));
    query.measures.forEach(m -> aggregates.add(m.sqlExpression(fieldProvider, queryRewriter, true))); // Alias is needed when using sub-queries

    selects.addAll(groupBy); // coord first, then aggregates
    if (queryRewriter.useGroupingFunction()) {
      query.rollup.forEach(f -> selects.add(String.format("grouping(%s)", queryRewriter.select(f)))); // use grouping to identify totals
    }
    selects.addAll(aggregates);

    StringBuilder statement = new StringBuilder();
    addCte(query.virtualTableDto, statement, queryRewriter);
    statement.append("select ");
    statement.append(String.join(", ", selects));
    statement.append(" from ");
    if (query.subQuery != null) {
      statement.append("(");
      statement.append(translate(query.subQuery, fieldProvider, queryRewriterSupplier));
      statement.append(")");
    } else {
      statement.append(queryRewriter.tableName(query.table.name));
      addJoins(statement, query.table, query.virtualTableDto, fieldProvider, queryRewriter);
    }
    addWhereConditions(statement, query, fieldProvider, queryRewriter);
    addGroupByAndRollup(groupBy, query.rollup.stream().map(queryRewriter::rollup).toList(), queryRewriter.usePartialRollupSyntax(), statement);
    addHavingConditions(statement, query.havingCriteriaDto, queryRewriter);
    addLimit(query.limit, statement);
    return statement.toString();
  }

  private static void addCte(VirtualTableDto virtualTableDto, StringBuilder statement, QueryRewriter qr) {
    if (virtualTableDto == null) {
      return;
    }

    StringBuilder sb = new StringBuilder();
    Iterator<List<Object>> it = virtualTableDto.records.iterator();
    while (it.hasNext()) {
      sb.append("select ");
      List<Object> row = it.next();
      for (int i = 0; i < row.size(); i++) {
        Object obj = row.get(i);
        sb.append(obj instanceof String ? "'" : "");
        sb.append(obj);
        sb.append(obj instanceof String ? "'" : "");
        sb.append(" as ").append(qr.fieldName(virtualTableDto.fields.get(i)));
        if (i < row.size() - 1) {
          sb.append(", ");
        }
      }
      if (it.hasNext()) {
        sb.append(" union all ");
      }
    }

    statement
            .append("with ").append(qr.cteName(virtualTableDto.name))
            .append(" as (").append(sb).append(") ");
  }

  private static void addLimit(int limit, StringBuilder statement) {
    if (limit > 0) {
      statement.append(" limit " + limit);
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

  protected static void addWhereConditions(StringBuilder statement, DatabaseQuery query, Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
    if (query.whereCriteriaDto != null) {
      String whereClause = toSql(fieldProvider, query.whereCriteriaDto, queryRewriter);
      if (whereClause != null) {
        statement
                .append(" where ")
                .append(whereClause);
      }
    }
  }

  private static void addJoins(StringBuilder statement, TableDto tableQuery, VirtualTableDto virtualTableDto, Function<String, Field> fieldProvider, QueryRewriter qr) {
    Function<String, String> tableNameFunc = tableName -> virtualTableDto != null && virtualTableDto.name.equals(tableName) ? qr.cteName(tableName) : qr.tableName(tableName);
    for (JoinDto join : tableQuery.joins) {
      statement
              .append(" ")
              .append(join.type.name().toLowerCase())
              .append(" join ")
              .append(tableNameFunc.apply(join.table.name))
              .append(" on ");
      for (int i = 0; i < join.mappings.size(); i++) {
        JoinMappingDto mapping = join.mappings.get(i);
        var op = switch (mapping.conditionType) {
          case EQ -> " = ";
          case NEQ -> " <> ";
          case LT -> " < ";
          case LE -> " <= ";
          case GT -> " > ";
          case GE -> " >= ";
          default -> throw new IllegalStateException("Unexpected value: " + mapping.conditionType);
        };
        Field from = fieldProvider.apply(mapping.from);
        Field to = fieldProvider.apply(mapping.to);
        statement
                .append(qr.getFieldFullName(from))
                .append(op)
                .append(qr.getFieldFullName(to));
        if (i < join.mappings.size() - 1) {
          statement.append(" and ");
        }
      }

      if (!join.table.joins.isEmpty()) {
        addJoins(statement, join.table, virtualTableDto, fieldProvider, qr);
      }
    }
  }

  public static String toSql(Field field, ConditionDto dto, QueryRewriter queryRewriter) {
    if (dto instanceof SingleValueConditionDto || dto instanceof InConditionDto) {
      Function<Object, String> sqlMapper = getQuoteFn(field);
      String formattedFieldName = queryRewriter.getFieldFullName(field);
      return switch (dto.type()) {
        case IN -> formattedFieldName + " in (" +
                ((InConditionDto) dto).values
                        .stream()
                        .map(sqlMapper)
                        .collect(Collectors.joining(", ")) + ")";
        case EQ -> formattedFieldName + " = " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case NEQ -> formattedFieldName + " <> " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case LT -> formattedFieldName + " < " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case LE -> formattedFieldName + " <= " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case GT -> formattedFieldName + " > " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case GE -> formattedFieldName + " >= " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        case LIKE -> formattedFieldName + " like " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        default -> throw new IllegalStateException("Unexpected value: " + dto.type());
      };
    } else if (dto instanceof LogicalConditionDto logical) {
      String first = toSql(field, logical.one, queryRewriter);
      String second = toSql(field, logical.two, queryRewriter);
      String typeString = switch (dto.type()) {
        case AND -> " and ";
        case OR -> " or ";
        default -> throw new IllegalStateException("Incorrect type " + logical.type);
      };
      return "(" + first + typeString + second + ")";
    } else if (dto instanceof ConstantConditionDto cc) {
      String formattedFieldName = queryRewriter.fieldName(field.name());
      return switch (cc.type()) {
        case NULL -> formattedFieldName + " is null";
        case NOT_NULL -> formattedFieldName + " is not null";
        default -> throw new IllegalStateException("Unexpected value: " + dto.type());
      };
    } else {
      throw new RuntimeException("Not supported condition " + dto);
    }
  }

  public static String toSql(Function<String, Field> fieldProvider, CriteriaDto criteriaDto, QueryRewriter queryRewriter) {
    if (criteriaDto.isWhereCriterion()) {
      return toSql(fieldProvider.apply(criteriaDto.field), criteriaDto.condition, queryRewriter);
    } else if (criteriaDto.isHavingCriterion()) {
      return toSql(fieldProvider.apply(criteriaDto.measure.alias()), criteriaDto.condition, queryRewriter);
    } else if (!criteriaDto.children.isEmpty()) {
      String sep = switch (criteriaDto.conditionType) {
        case AND -> " and ";
        case OR -> " or ";
        default -> throw new IllegalStateException("Unexpected value: " + criteriaDto.conditionType);
      };
      Iterator<CriteriaDto> iterator = criteriaDto.children.iterator();
      List<String> conditions = new ArrayList<>();
      while (iterator.hasNext()) {
        String c = toSql(fieldProvider, iterator.next(), queryRewriter);
        if (c != null) {
          conditions.add(c);
        }
      }
      return conditions.isEmpty() ? null : ("(" + String.join(sep, conditions) + ")");
    } else {
      return null;
    }
  }

  public static Function<Object, String> getQuoteFn(Field field) {
    if (Number.class.isAssignableFrom(field.type())
            || field.type().equals(double.class)
            || field.type().equals(int.class)
            || field.type().equals(long.class)
            || field.type().equals(float.class)
            || field.type().equals(boolean.class)
            || field.type().equals(Boolean.class)) {
      // no quote
      return String::valueOf;
    } else if (field.type().equals(String.class)) {
      // quote
      return s -> "'" + s + "'";
    } else {
      throw new RuntimeException("Not supported " + field.type());
    }
  }

  protected static void addHavingConditions(StringBuilder statement, CriteriaDto havingCriteriaDto, QueryRewriter queryRewriter) {
    if (havingCriteriaDto != null) {
      String havingClause = toSql(name -> new Field(null, name, double.class), havingCriteriaDto, queryRewriter);
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
