package io.squashql.query.join;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.*;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.database.*;
import io.squashql.query.dto.*;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.api.tuple.Triple;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.stream.Collectors;

import static io.squashql.query.QueryExecutor.LIMIT_DEFAULT_VALUE;
import static io.squashql.query.database.AQueryEngine.transformToColumnFormat;
import static io.squashql.query.database.SQLTranslator.addLimit;

@Slf4j
@AllArgsConstructor
public class ExperimentalQueryMergeExecutor {

  private final QueryEngine<?> queryEngine;

  class Holder {
    final QueryDto query;
    final QueryResolver queryResolver;
    final DatabaseQuery dbQuery;
    final QueryRewriter queryRewriter;
    final String originalTableName; // Can be null if using a sub-query
    final String cteTableName;
    final String sql;

    Holder(String cteTableName, QueryDto query) {
      this.query = query;
      this.cteTableName = cteTableName;
      this.queryResolver = new QueryResolver(query, ExperimentalQueryMergeExecutor.this.queryEngine.datastore().storesByName());
      this.dbQuery = this.queryResolver.toDatabaseQuery(this.queryResolver.getScope(), -1);
      this.queryResolver.getMeasures().values().forEach(this.dbQuery::withMeasure);
      this.queryRewriter = ExperimentalQueryMergeExecutor.this.queryEngine.queryRewriter(this.dbQuery);
      this.originalTableName = query.table != null ? query.table.name : null;
      this.sql = SQLTranslator.translate(this.dbQuery, this.queryRewriter);
    }
  }

  public Triple<String, List<TypedField>, List<CompiledMeasure>> generateSql(QueryJoinDto queryJoin) {
    int queryLimit = queryJoin.limit <= 0 ? LIMIT_DEFAULT_VALUE : queryJoin.limit;

    List<Holder> holders = new ArrayList<>(queryJoin.queries.size());
    for (int i = 0; i < queryJoin.queries.size(); i++) {
      if (i == 0) {
        holders.add(new Holder(queryJoin.table.name, queryJoin.queries.get(i)));
      } else {
        holders.add(new Holder(queryJoin.table.joins.get(i - 1).table.name, queryJoin.queries.get(i)));
      }
    }

    // Start by setting all CTEs
    StringBuilder sb = new StringBuilder("with ");
    List<List<Twin<String>>> columnNamesAndNamesOrAliases = new ArrayList<>();
    Map<String, TypedField> typedFieldByFullName = new HashMap<>();
    for (int i = 0; i < holders.size(); i++) {
      Holder holder = holders.get(i);
      sb.append(holder.queryRewriter.cteName(holder.cteTableName)).append(" as (").append(holder.sql);
      sb.append(i < holders.size() - 1 ? "), " : ") ");

      columnNamesAndNamesOrAliases.add(
              holder.queryResolver.getColumns().stream().map(f -> {
                String fieldFullName = SqlUtils.getFieldFullName(holder.queryRewriter.cteName(holder.cteTableName), holder.queryRewriter.fieldName(getFieldName(f)));
                typedFieldByFullName.put(fieldFullName, f);
                return Tuples.twin(fieldFullName, f.alias() != null ? holder.queryRewriter.escapeAlias(f.alias()) : fieldFullName);
              }).toList());
    }

    // Order is important here (hence LinkedHashMap). The columns from first queries will take precedence over the last queries
    Map<String, String> fullNameByAlias = new LinkedHashMap<>();
    for (List<Twin<String>> columnNamesOrAliases : columnNamesAndNamesOrAliases) {
      for (Twin<String> nameOrAlias : columnNamesOrAliases) {
        fullNameByAlias.putIfAbsent(nameOrAlias.getTwo(), nameOrAlias.getOne());
      }
    }

    List<JoinDto> newJoins = new ArrayList<>();
    // Iterate over the joins to rewrite the condition when necessary (to use aliases)
    Map<Integer, Map<String, Field>> fieldBySquashQLExpression = new TreeMap<>();
    for (Field field : queryJoin.queries.get(0).columns) {
      TypedField typedField = holders.get(0).queryResolver.resolveField(field);
      fieldBySquashQLExpression.computeIfAbsent(0, k -> new HashMap<>()).put(SqlUtils.squashqlExpression(typedField), field);
    }

    int index = 1;
    for (JoinDto join : queryJoin.table.joins) {
      CriteriaDto joinConditionCopy = JacksonUtil.deserialize(JacksonUtil.serialize(join.joinCriteria), CriteriaDto.class);
      if (joinConditionCopy == null) {
        // Guess the condition
        for (Field field : holders.get(index).query.columns) {
          TypedField typedField = holders.get(index).queryResolver.resolveField(field);
          fieldBySquashQLExpression.computeIfAbsent(index, k -> new HashMap<>()).put(SqlUtils.squashqlExpression(typedField), field);
        }

        List<Pair<Pair<Field, Integer>, Pair<Field, Integer>>> common = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, Field>> entry : fieldBySquashQLExpression.entrySet()) {
          if (entry.getKey() == index) {
            break;
          }

          for (Map.Entry<String, Field> fieldEntry : entry.getValue().entrySet()) {
            if (fieldBySquashQLExpression.get(index).containsKey(fieldEntry.getKey())) {
              common.add(Tuples.pair(Tuples.pair(fieldEntry.getValue(), entry.getKey()), Tuples.pair(fieldBySquashQLExpression.get(index).get(fieldEntry.getKey()), index)));
            }
          }
        }

        if (!common.isEmpty()) {
          List<CriteriaDto> children = new ArrayList<>(common.size());
          for (Pair<Pair<Field, Integer>, Pair<Field, Integer>> c : common) {
            Field l = new TableField(holders.get(c.getOne().getTwo()).cteTableName, getFieldName(c.getOne().getOne()));
            Field r = new TableField(holders.get(c.getTwo().getTwo()).cteTableName, getFieldName(c.getTwo().getOne()));
            children.add(new CriteriaDto(l, r, null, null, ConditionType.EQ, Collections.emptyList()));
          }
          CriteriaDto criteriaDto = children.size() > 1
                  ? new CriteriaDto(null, null, null, null, ConditionType.AND, children)
                  : children.get(0);
          newJoins.add(new JoinDto(join.table, join.type, criteriaDto));
        } else {
          newJoins.add(new JoinDto(join.table, JoinType.CROSS, null));
        }
      } else {
        CriteriaDto rewrittenJoinCondition = rewriteJoinCondition(joinConditionCopy, holders);
        newJoins.add(new JoinDto(join.table, join.type, rewrittenJoinCondition));
      }
      index++;
    }

    QueryRewriter queryRewriter = holders.get(0).queryRewriter;
    StringBuilder joinSb = new StringBuilder();
    joinSb.append(" from ").append(queryRewriter.cteName(holders.get(0).cteTableName));
    Set<String> toRemoveFromSelectSet = new HashSet<>();
    for (int i = 0; i < newJoins.size(); i++) {
      Holder holder = holders.get(i + 1);
      JoinDto jc = newJoins.get(i);
      joinSb
              .append(" ")
              .append(jc.type.name().toLowerCase())
              .append(" join ")
              .append(queryRewriter.cteName(holder.cteTableName));
      if (jc.joinCriteria != null) {
        joinSb
                .append(" on ")
                .append(sqlExpression(jc.joinCriteria, queryRewriter, holders, toRemoveFromSelectSet));
      }
    }

    sb.append("select ");

    List<TypedField> selectedColumns = new ArrayList<>();
    List<String> selectSt = new ArrayList<>();
    for (Map.Entry<String, String> e : fullNameByAlias.entrySet()) {
      String alias = e.getKey();
      String fullName = e.getValue();
      if (!toRemoveFromSelectSet.contains(fullName)) {
        if (alias.equals(fullName)) { // no alias
          selectSt.add(alias);
        } else {
          selectSt.add(fullName + " as " + alias); // with alias
        }
        selectedColumns.add(typedFieldByFullName.get(fullName));
      }
    }

    // The measures
    for (int i = 0; i < holders.size(); i++) {
      Holder holder = holders.get(i);
      holder.query.measures.forEach(m -> selectSt.add(holder.queryRewriter.escapeAlias(m.alias())));
    }

    sb.append(String.join(", ", selectSt));

    sb.append(joinSb);

    addOrderBy(queryJoin.orders, sb, queryRewriter, selectedColumns, holders);
    addLimit(queryLimit, sb);

    List<CompiledMeasure> measures = new ArrayList<>();
    for (int i = 0; i < holders.size(); i++) {
      Holder holder = holders.get(i);
      holder.query.measures.forEach(measure -> measures.add(holder.queryResolver.getMeasures().get(measure)));
    }

    return Tuples.triple(sb.toString(), selectedColumns, measures);
  }

  public Table execute(QueryJoinDto queryJoin) {
    Triple<String, List<TypedField>, List<CompiledMeasure>> sqlGenerationResult = generateSql(queryJoin);
    log.info("sql=" + sqlGenerationResult.getOne());
    Table result = this.queryEngine.executeRawSql(sqlGenerationResult.getOne());

    List<? extends Class<?>> columnTypes = result.headers().stream().map(Header::type).toList();
    Pair<List<Header>, List<List<Object>>> transform = transformToColumnFormat(
            sqlGenerationResult.getTwo(),
            sqlGenerationResult.getThree(),
            columnTypes,
            (columnType, name) -> columnType,
            result.iterator(),
            (i, row) -> row.get(i));
    return new ColumnarTable(
            transform.getOne(),
            new HashSet<>(sqlGenerationResult.getThree()),
            transform.getTwo());
  }

  /**
   * Generates the sql expression corresponding to the {@link CriteriaDto}. In addition, the set is here to collect the
   * columns from the {@link CriteriaDto} that should be removed from the select: e.g A.id = B.id, the set should contain
   * B.id (or __cte1__.id) assuming A comes before B in the {@link Holder} list.
   */
  public String sqlExpression(CriteriaDto jc, QueryRewriter queryRewriter, List<Holder> holders, Set<String> toRemoveFromSelectSet) {
    if (jc.field != null && jc.fieldOther != null && jc.conditionType != null) {
      Holder leftHolder = getHolderOrigin(holders, jc.field);
      Holder rightHolder = getHolderOrigin(holders, jc.fieldOther);

      String left;
      String leftFieldName = getFieldName(jc.field);
      if (jc.field instanceof TableField tf) {
        left = SqlUtils.getFieldFullName(queryRewriter.cteName(tf.tableName), queryRewriter.fieldName(leftFieldName));
      } else {
        // could be an aliased field? Need find where it comes from
        left = SqlUtils.getFieldFullName(leftHolder == null ? null : queryRewriter.cteName(leftHolder.cteTableName), queryRewriter.fieldName(leftFieldName));
      }

      String right;
      String rightFieldName = getFieldName(jc.fieldOther);
      if (jc.fieldOther instanceof TableField tf) {
        right = SqlUtils.getFieldFullName(queryRewriter.cteName(tf.tableName), queryRewriter.fieldName(rightFieldName));
      } else {
        // could be an aliased field? Need find where it comes from
        right = SqlUtils.getFieldFullName(rightHolder == null ? null : queryRewriter.cteName(rightHolder.cteTableName), queryRewriter.fieldName(rightFieldName));
      }

      String toRemoveFromSelect = null;
      for (Holder holder : holders) {
        if (holder.equals(leftHolder)) {
          toRemoveFromSelect = left; // Same logic than the select i.e __cte0__.fieldName
        }
        if (holder.equals(rightHolder)) {
          toRemoveFromSelect = right; // Same logic than the select i.e __cte0__.fieldName
        }
      }

      if (toRemoveFromSelect != null) {
        toRemoveFromSelectSet.add(toRemoveFromSelect);
      }

      return String.join(" ", left, jc.conditionType.sqlInfix, right);
    } else if (!jc.children.isEmpty()) {
      String sep = switch (jc.conditionType) {
        case AND -> " and ";
        default -> throw new IllegalStateException("Unexpected value: " + jc.conditionType);
      };
      Iterator<CriteriaDto> iterator = jc.children.iterator();
      List<String> conditions = new ArrayList<>();
      while (iterator.hasNext()) {
        String c = sqlExpression(iterator.next(), queryRewriter, holders, toRemoveFromSelectSet);
        if (c != null) {
          conditions.add(c);
        }
      }
      return conditions.isEmpty() ? null : ("(" + String.join(sep, conditions) + ")");
    } else {
      return null;
    }
  }

  public static void addOrderBy(Map<Field, OrderDto> orders, StringBuilder sb, QueryRewriter queryRewriter, List<TypedField> selectedColumns, List<Holder> holders) {
    if (orders != null && !orders.isEmpty()) {
      sb.append(" order by ");
      List<String> orderList = new ArrayList<>();
      for (Map.Entry<Field, OrderDto> e : orders.entrySet()) {
        Field key = e.getKey();
        TypedField typedField = null;
        Holder h = null;
        if (key.alias() != null) {
          // Rely on the alias
          for (TypedField selectedColumn : selectedColumns) {
            if (key.alias().equals(selectedColumn.alias())) {
              typedField = selectedColumn;
              break;
            }
          }
        } else {
          // We assume it is a TableField, otherwise it is not supported
          String tableName = ((TableField) key).tableName;
          if (tableName != null) {
            for (Holder holder : holders) {
              if (holder.originalTableName.equals(tableName)) {
                typedField = holder.queryResolver.resolveField(key);
                h = holder;
                break;
              }
            }
          } else {
            // Take the first one that matches
            for (Holder holder : holders) {
              if (holder.query.columns.stream().map(ExperimentalQueryMergeExecutor::getFieldName).collect(Collectors.toSet()).contains(getFieldName(key))) {
                typedField = holder.queryResolver.resolveField(key);
                h = holder;
                break;
              }
            }
          }
        }

        if (typedField == null) {
          throw new RuntimeException("Cannot resolve " + e.getKey());
        }


        String orderByField = h == null ? queryRewriter.aliasOrFullExpression(typedField)
                : SqlUtils.getFieldFullName(h.queryRewriter.cteName(h.cteTableName), h.queryRewriter.fieldName(getFieldName(typedField)));
        orderList.add(orderByField + " nulls last");
      }
      sb.append(String.join(", ", orderList));
    }
  }

  static CriteriaDto rewriteJoinCondition(CriteriaDto joinCondition, List<Holder> holders) {
    Map<String, String> cteByOriginalTableName = new HashMap<>();
    for (Holder holder : holders) {
      cteByOriginalTableName.put(holder.originalTableName, holder.cteTableName);
    }

    if (joinCondition != null) {
      List<CriteriaDto> children = joinCondition.children;
      if (children != null && !children.isEmpty()) {
        for (CriteriaDto child : children) {
          rewriteJoinCondition(child, holders);
        }
      } else {
        String alias = joinCondition.field.alias();
        if (alias != null) {
          joinCondition.field = new AliasedField(alias); // replace with aliased field
        } else if (joinCondition.field instanceof TableField tf) {
          joinCondition.field = new TableField(cteByOriginalTableName.get(tf.tableName), tf.fieldName);
        }

        String otherAlias = joinCondition.fieldOther.alias();
        if (otherAlias != null) {
          joinCondition.fieldOther = new AliasedField(otherAlias); // replace with aliased field
        } else if (joinCondition.fieldOther instanceof TableField tf) {
          joinCondition.fieldOther = new TableField(cteByOriginalTableName.get(tf.tableName), tf.fieldName);
        }
      }
      return joinCondition;
    } else {
      return null;
    }
  }

  private static String replaceTableNameByCteNameIfNotNull(Holder holder, String s) {
    if (holder.originalTableName != null) {
      s = s.replace(holder.queryRewriter.tableName(holder.originalTableName), holder.queryRewriter.cteName(holder.cteTableName));
    }
    return s;
  }

  private static Holder getHolderOrigin(List<Holder> holders, Field field) {
    for (Holder holder : holders) {
      if (field instanceof TableField tf) {
        if (holder.cteTableName.equals(tf.tableName)) {
          return holder;
        }
      } else if (field instanceof AliasedField af) {
        for (Field column : holder.query.columns) {
          if (af.alias().equals(column.alias())) {
            return holder;
          }
        }
      }
    }
    return null;
  }

  static String getFieldName(Field field) {
    String alias = field.alias();
    if (alias != null) {
      return alias;
    } else if (field instanceof TableField tf) {
      return tf.fieldName;
    } else {
      throw new IllegalArgumentException("The field " + field + " need to have an alias or of type " + TableField.class);
    }
  }

  static String getFieldName(TypedField field) {
    String alias = field.alias();
    if (alias != null) {
      return alias;
    } else if (field instanceof TableTypedField tf) {
      return tf.name();
    } else {
      throw new IllegalArgumentException("The field " + field + " need to have an alias or of type " + TableTypedField.class);
    }
  }
}
