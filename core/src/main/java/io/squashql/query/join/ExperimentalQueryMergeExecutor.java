package io.squashql.query.join;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.*;
import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.compiled.CompiledJoin;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.compiled.CompiledTable;
import io.squashql.query.database.*;
import io.squashql.query.dto.*;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;

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

  public Table execute(JoinStatement statement,
                       Map<Field, OrderDto> orders,
                       int limit) {
    int queryLimit = limit <= 0 ? LIMIT_DEFAULT_VALUE : limit;

    List<Holder> holders = new ArrayList<>(statement.queries.size());
    for (int i = 0; i < statement.queries.size(); i++) {
      if (i == 0) {
        holders.add(new Holder(statement.tableDto.name, statement.queries.get(i)));
      } else {
        holders.add(new Holder(statement.tableDto.joins.get(i - 1).table.name, statement.queries.get(i)));
      }
    }

    // Start by setting all CTEs
    StringBuilder sb = new StringBuilder("with ");
    for (int i = 0; i < holders.size(); i++) {
      Holder holder = holders.get(i);
      sb.append(holder.queryRewriter.cteName(holder.cteTableName)).append(" as (").append(holder.sql);
      sb.append(i < holders.size() - 1 ? "), " : ") ");
    }

    CompiledTable table = getCompiledTable(statement, holders);
    String tableExpression = table.sqlExpression(ExperimentalQueryMergeExecutor.this.queryEngine.queryRewriter(null));
    for (int i = 0; i < holders.size(); i++) {
      tableExpression = replaceTableNameByCteNameIfNotNull(holders.get(i), tableExpression);
    }

    List<List<TypedField>> selectColumns = getSelectElements(table, holders);
    List<String> selectSt = new ArrayList<>();
    // The columns
    for (int i = 0; i < selectColumns.size(); i++) {
      Holder holder = holders.get(i);
      selectColumns.get(i).forEach(typedField -> selectSt.add(replaceTableNameByCteNameIfNotNull(holder, holder.queryRewriter.select(typedField))));
    }
    // The measures
    for (int i = 0; i < holders.size(); i++) {
      Holder holder = holders.get(i);
      holder.query.measures.forEach(m -> selectSt.add(holder.queryRewriter.escapeAlias(m.alias())));
    }

    sb
            .append("select ")
            .append(String.join(", ", selectSt))
            .append(" from ")
            .append(tableExpression);


    addOrderBy(orders, sb, holders);
    addLimit(queryLimit, sb);

    String sql = sb.toString();
    log.info("sql=" + sql);
    Table result = this.queryEngine.executeRawSql(sql);

    List<? extends Class<?>> columnTypes = result.headers().stream().map(Header::type).toList();
    List<CompiledMeasure> measures = new ArrayList<>();
    for (int i = 0; i < holders.size(); i++) {
      Holder holder = holders.get(i);
      holder.query.measures.forEach(m -> measures.add(holder.queryResolver.getMeasures().get(m)));
    }

    Pair<List<Header>, List<List<Object>>> transform = transformToColumnFormat(
            selectColumns.stream().flatMap(Collection::stream).toList(),
            measures,
            columnTypes,
            (columnType, name) -> columnType,
            result.iterator(),
            (i, row) -> row.get(i));
    return new ColumnarTable(
            transform.getOne(),
            new HashSet<>(measures),
            transform.getTwo());
  }

  private CompiledTable getCompiledTable(JoinStatement statement, List<Holder> holders) {
    TableDto tableDto = new TableDto(statement.tableDto.name);
    List<JoinDto> newJoins = new ArrayList<>();
    // Iterate over the joins to rewrite the condition when necessary (to use aliases)
    Map<Integer, Map<String, Field>> fieldBySquashQLExpression = new TreeMap<>();
    for (Field field : statement.queries.get(0).columns) {
      TypedField typedField = holders.get(0).queryResolver.resolveField(field);
      fieldBySquashQLExpression.computeIfAbsent(0, k -> new HashMap<>()).put(SqlUtils.squashqlExpression(typedField), field);
    }

    int index = 1;
    for (JoinDto join : statement.tableDto.joins) {
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
          newJoins.add(new JoinDto(join.table, JoinType.LEFT, criteriaDto));
        } else {
          newJoins.add(new JoinDto(join.table, JoinType.CROSS, null));
        }
      } else {
        CriteriaDto rewrittenJoinCondition = rewriteJoinCondition(joinConditionCopy);
        newJoins.add(new JoinDto(join.table, join.type, rewrittenJoinCondition));
      }
      index++;
    }
    tableDto.joins = newJoins;
    tableDto.isCte = true;

    List<VirtualTableDto> vts = new ArrayList<>(holders.size());
    for (Holder h : holders) {
      vts.add(new VirtualTableDto(
              h.cteTableName,
              h.query.columns.stream().map(ExperimentalQueryMergeExecutor::getFieldName).toList(),
              List.of(h.query.columns.stream().map(__ -> new Object()).toList()))); // we don't care about the records
    }

    QueryDto queryDto = new QueryDto().table(tableDto);
    queryDto.virtualTableDtos = vts;
    QueryResolver queryResolver = new QueryResolver(queryDto, ExperimentalQueryMergeExecutor.this.queryEngine.datastore().storesByName());
    return queryResolver.getScope().table();
  }

  static String getFieldName(Field field) {
    String alias = field.alias();
    if (alias != null) {
      return alias;
    } else if (field instanceof TableField tf) {
      return tf.fieldName;
    } else {
      throw new IllegalArgumentException("The field " + field + " need to have an alias");
    }
  }

  static String getFieldName(TypedField field) {
    String alias = field.alias();
    if (alias != null) {
      return alias;
    } else if (field instanceof TableTypedField tf) {
      return tf.name();
    } else {
      throw new IllegalArgumentException("The field " + field + " need to have an alias");
    }
  }

  static CriteriaDto rewriteJoinCondition(CriteriaDto joinCondition) {
    if (joinCondition != null) {
      List<CriteriaDto> children = joinCondition.children;
      if (children != null && !children.isEmpty()) {
        for (CriteriaDto child : children) {
          rewriteJoinCondition(child);
        }
      } else {
        String alias = joinCondition.field.alias();
        if (alias != null) {
          joinCondition.field = new AliasedField(alias); // replace with aliased field
        }

        String otherAlias = joinCondition.fieldOther.alias();
        if (otherAlias != null) {
          joinCondition.fieldOther = new AliasedField(otherAlias); // replace with aliased field
        }
      }
      return joinCondition;
    } else {
      return null;
    }
  }

  public static void addOrderBy(Map<Field, OrderDto> orders, StringBuilder sb, List<Holder> holders) {
    if (orders != null && !orders.isEmpty()) {
      sb.append(" order by ");
      List<String> orderList = new ArrayList<>();
      for (Map.Entry<Field, OrderDto> e : orders.entrySet()) {
        Field key = e.getKey();


        TypedField typedField = null;
        Holder holder = null;
        for (Holder h : holders) {
          // Where does it come from ?
          if ((typedField = h.queryResolver.getTypedFieldOrNull(key)) != null) {
            holder = h;
            break;
          }

        }

        if (typedField == null) {
          throw new RuntimeException("Cannot resolve " + e.getKey());
        }
        String orderByField = holder.queryRewriter.aliasOrFullExpression(typedField);
        orderByField = replaceTableNameByCteNameIfNotNull(holder, orderByField);
        orderList.add(orderByField + " nulls last");
      }
      sb.append(String.join(", ", orderList));
    }
  }

  /**
   * Returns a list of elements that will end up in the select statement based on the given join table, left holder,
   * and right holder. Columns first then measures.
   */
  private static List<List<TypedField>> getSelectElements(CompiledTable joinTable, List<Holder> holders) {
    List<List<TypedField>> allColumns = new ArrayList<>();
    Set<TypedField> joinFields = new HashSet<>();
    for (CompiledJoin join : joinTable.joins()) {
      CompiledCriteria jc = join.joinCriteria();
      joinFields.addAll(collectJoinFields(jc));
    }

    Map<String, TypedField> added = new HashMap<>();
    List<String> joinFieldsSt = joinFields.stream().map(SqlUtils::squashqlExpression).toList();

    for (int i = 0; i < holders.size(); i++) {
      Holder h = holders.get(i);
      List<TypedField> columns = new ArrayList<>();
      for (Field field : h.query.columns) {
        TypedField typedField = h.queryResolver.resolveField(field);
        String name = getFieldName(typedField);
        String fullName = SqlUtils.getFieldFullName(h.cteTableName, name);
        if (i == 0 || !joinFieldsSt.contains(fullName)) {
//          added.put(fullName, typedField);
          columns.add(typedField);
        }
      }
      allColumns.add(columns); // we have to use the aliased field in the select
    }
    return allColumns;
  }

  private static Set<TypedField> collectJoinFields(CompiledCriteria joinCriteria) {
    Set<TypedField> collected = new HashSet<>();
    List<CompiledCriteria> children = joinCriteria.children();
    if (children != null && !children.isEmpty()) {
      for (CompiledCriteria child : children) {
        collected.addAll(collectJoinFields(child));
      }
    } else {
      collected.add(joinCriteria.field());
      collected.add(joinCriteria.fieldOther());
    }
    return collected;
  }

  private static String replaceTableNameByCteNameIfNotNull(Holder holder, String s) {
    if (holder.originalTableName != null) {
      s = s.replace(holder.queryRewriter.tableName(holder.originalTableName), holder.queryRewriter.cteName(holder.cteTableName));
    }
    return s;
  }
}
