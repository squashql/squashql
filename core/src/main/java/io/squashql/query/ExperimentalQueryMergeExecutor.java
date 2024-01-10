package io.squashql.query;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.compiled.CompiledTable;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.*;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.AliasedTypedField;
import io.squashql.type.TypedField;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.list.mutable.MutableListFactoryImpl;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;

import static io.squashql.query.QueryExecutor.LIMIT_DEFAULT_VALUE;
import static io.squashql.query.database.AQueryEngine.transformToColumnFormat;
import static io.squashql.query.database.SQLTranslator.addLimit;

@Slf4j
public class ExperimentalQueryMergeExecutor {

  private final QueryEngine<?> queryEngine;

  class Holder {
    final QueryDto query;
    final QueryResolver queryResolver;
    final DatabaseQuery dbQuery;
    final QueryRewriter queryRewriter;
    final String originalTableName;
    final String cteTableName;
    final String sql;

    Holder(String cteTableName, QueryDto query) {
      this.query = query;
      this.cteTableName = cteTableName;
      this.queryResolver = new QueryResolver(query, new HashMap<>(ExperimentalQueryMergeExecutor.this.queryEngine.datastore().storesByName()));
      this.dbQuery = this.queryResolver.toDatabaseQuery(this.queryResolver.getScope(), -1);
      this.queryResolver.getMeasures().values().forEach(this.dbQuery::withMeasure);
      this.queryRewriter = ExperimentalQueryMergeExecutor.this.queryEngine.queryRewriter(this.dbQuery);
      this.originalTableName = this.queryRewriter.tableName(query.table.name);
      this.sql = SQLTranslator.translate(this.dbQuery, this.queryRewriter);
    }
  }

  public ExperimentalQueryMergeExecutor(QueryEngine<?> queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table execute(QueryDto first,
                       QueryDto second,
                       JoinType joinType,
                       CriteriaDto joinCondition,
                       Map<Field, OrderDto> orders,
                       int limit) {
    int queryLimit = limit < 0 ? LIMIT_DEFAULT_VALUE : limit;

    Holder left = new Holder("__cteL__", first);
    Holder right = new Holder("__cteR__", second);

    StringBuilder sb = new StringBuilder("with ");
    sb.append(left.cteTableName).append(" as (").append(left.sql).append("), ");
    sb.append(right.cteTableName).append(" as (").append(right.sql).append(") ");

    QueryDto firstCopy = JacksonUtil.deserialize(JacksonUtil.serialize(first), QueryDto.class);
    CriteriaDto joinConditionCopy = JacksonUtil.deserialize(JacksonUtil.serialize(joinCondition), CriteriaDto.class);
    CriteriaDto rewrittenJoinCondition = rewriteJoinCondition(joinConditionCopy);
    firstCopy.table.join(new TableDto(second.table.name), joinType, rewrittenJoinCondition);
    QueryResolver queryResolver = new QueryResolver(firstCopy, new HashMap<>(this.queryEngine.datastore().storesByName()));
    CompiledTable joinTable = queryResolver.getScope().table();

    Twin<List<TypedField>> selectColumns = getSelectElements(joinTable, left, right);
    List<String> select = new ArrayList<>();
    selectColumns.getOne().forEach(typedField -> select.add(left.queryRewriter.select(typedField).replace(left.originalTableName, left.cteTableName)));
    selectColumns.getTwo().forEach(typedField -> select.add(right.queryRewriter.select(typedField).replace(right.originalTableName, right.cteTableName)));
    left.query.measures.forEach(m -> select.add(left.queryRewriter.escapeAlias(m.alias())));
    right.query.measures.forEach(m -> select.add(right.queryRewriter.escapeAlias(m.alias())));
    sb
            .append("select ")
            .append(String.join(", ", select))
            .append(" from ");

    String tableExpression = joinTable.sqlExpression(this.queryEngine.queryRewriter(null));
    tableExpression = tableExpression.replace(left.originalTableName, left.cteTableName);
    tableExpression = tableExpression.replace(right.originalTableName, right.cteTableName);
    sb.append(tableExpression);

    addOrderBy(orders, sb, left, right);
    addLimit(queryLimit, sb);

    String sql = sb.toString();
    log.info("sql=" + sql);
    Table table = this.queryEngine.executeRawSql(sql);

    List<? extends Class<?>> columnTypes = table.headers().stream().map(Header::type).toList();
    List<CompiledMeasure> measures = new ArrayList<>();
    left.query.measures.forEach(m -> measures.add(left.queryResolver.getMeasures().get(m)));
    right.query.measures.forEach(m -> measures.add(right.queryResolver.getMeasures().get(m)));

    Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
            MutableListFactoryImpl.INSTANCE.withAll(selectColumns.getOne()).withAll(selectColumns.getTwo()),
            measures,
            columnTypes,
            (columnType, name) -> columnType,
            table.iterator(),
            (i, row) -> row.get(i));
    return new ColumnarTable(
            result.getOne(),
            new HashSet<>(measures),
            result.getTwo());
  }

  private static CriteriaDto rewriteJoinCondition(CriteriaDto joinCondition) {
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
  }

  private static void addOrderBy(Map<Field, OrderDto> orders, StringBuilder sb, Holder left, Holder right) {
    if (!orders.isEmpty()) {
      sb.append(" order by ");
      List<String> orderList = new ArrayList<>();
      for (Map.Entry<Field, OrderDto> e : orders.entrySet()) {
        Field key = e.getKey();
        Holder holder = left;
        // Where does it come from ? Left or right?
        TypedField typedField = left.queryResolver.getTypedFieldOrNull(key);
        if (typedField == null) {
          typedField = (holder = right).queryResolver.getTypedFieldOrNull(key);
        }
        if (typedField == null) {
          throw new RuntimeException("Cannot resolve " + e.getKey());
        }
        String orderByField = holder.queryRewriter.aliasOrFullExpression(typedField).replace(holder.originalTableName, holder.cteTableName);
        orderList.add(orderByField + " nulls last");
      }
      sb.append(String.join(", ", orderList));
    }
  }

  /**
   * Returns a list of elements that will end up in the select statement based on the given join table, left holder,
   * and right holder. Columns first then measures.
   *
   * @param joinTable the compiled table containing join information
   * @param left      the left holder
   * @param right     the right holder
   * @return a list of select elements
   */
  private static Twin<List<TypedField>> getSelectElements(CompiledTable joinTable, Holder left, Holder right) {
    // Try to guess from the conditions which field to keep.
    CompiledTable.CompiledJoin join = joinTable.joins().get(0);
    List<TypedField> leftColumns = new ArrayList<>();
    List<TypedField> rightColumns = new ArrayList<>();
    for (Field field : left.query.columns) {
      TypedField typedField = left.queryResolver.resolveField(field);
      leftColumns.add(typedField.alias() != null ? new AliasedTypedField(typedField.alias()) : typedField); // we have to use the aliased field in the select
    }

    Set<TypedField> joinFields = collectJoinFields(join.joinCriteria());
    for (Field field : right.query.columns) {
      TypedField typedField = right.queryResolver.resolveField(field);
      TypedField tf = typedField.alias() != null ? new AliasedTypedField(typedField.alias()) : typedField;
      // Do not add if it is in the join. We keep only the other field from the left query.
      if (!joinFields.contains(tf)) {
        rightColumns.add(tf); // we have to use the aliased field in the select
      }
    }
    return Tuples.twin(leftColumns, rightColumns);
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
}
