package io.squashql.query;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.compiled.*;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.*;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.AliasedTypedField;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
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

  record CteTable(String name, List<CompiledJoin> joins) implements CompiledTable, NamedTable {

    @Override
    public String sqlExpressionTableName(QueryRewriter queryRewriter) {
      return queryRewriter.cteName(this.name);
    }

    @Override
    public String sqlExpression(QueryRewriter queryRewriter) {
      return CompiledTable.sqlExpression(queryRewriter, this);
    }
  }

  public Table execute(QueryDto first,
                       QueryDto second,
                       JoinType joinType,
                       CriteriaDto joinCondition,
                       Map<Field, OrderDto> orders,
                       int limit) {
    int queryLimit = limit <= 0 ? LIMIT_DEFAULT_VALUE : limit;

    Holder left = new Holder("__cteL__", first);
    Holder right = new Holder("__cteR__", second);

    StringBuilder sb = new StringBuilder("with ");
    sb.append(left.queryRewriter.cteName(left.cteTableName)).append(" as (").append(left.sql).append("), ");
    sb.append(right.queryRewriter.cteName(right.cteTableName)).append(" as (").append(right.sql).append(") ");

    CompiledCriteria compiledCriteria = compiledCriteria(joinType, joinCondition, left, right);
    CompiledJoin compiledJoin = new CompiledJoin(new CteTable(right.cteTableName, List.of()), joinType, compiledCriteria);
    CteTable cteLeftTable = new CteTable(left.cteTableName, List.of(compiledJoin));

    Twin<List<TypedField>> selectColumns = getSelectElements(cteLeftTable, left, right);
    List<String> selectSt = new ArrayList<>();
    selectColumns.getOne().forEach(typedField -> selectSt.add(replaceTableNameByCteNameIfNotNull(left, left.queryRewriter.select(typedField))));
    selectColumns.getTwo().forEach(typedField -> selectSt.add(replaceTableNameByCteNameIfNotNull(right, right.queryRewriter.select(typedField))));
    left.query.measures.forEach(m -> selectSt.add(left.queryRewriter.escapeAlias(m.alias())));
    right.query.measures.forEach(m -> selectSt.add(right.queryRewriter.escapeAlias(m.alias())));
    sb
            .append("select ")
            .append(String.join(", ", selectSt))
            .append(" from ");

    String tableExpression = cteLeftTable.sqlExpression(left.queryRewriter);
    tableExpression = replaceTableNameByCteNameIfNotNull(left, tableExpression);
    tableExpression = replaceTableNameByCteNameIfNotNull(right, tableExpression);
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

  private CompiledCriteria compiledCriteria(JoinType joinType, CriteriaDto joinCondition, Holder left, Holder right) {
    QueryDto leftCteQuery = new QueryDto().table(left.cteTableName);
    QueryDto rightCteQuery = new QueryDto().table(right.cteTableName);
    CriteriaDto joinConditionCopy = JacksonUtil.deserialize(JacksonUtil.serialize(joinCondition), CriteriaDto.class);
    CriteriaDto rewrittenJoinCondition = rewriteJoinCondition(joinConditionCopy);
    leftCteQuery.table.join(new TableDto(rightCteQuery.table.name), joinType, rewrittenJoinCondition);
    QueryResolver queryResolver = new QueryResolver(leftCteQuery, this.queryEngine.datastore().storesByName());
    return queryResolver.compileCriteria(rewrittenJoinCondition);
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
    if (orders != null && !orders.isEmpty()) {
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
   *
   * @param joinTable the compiled table containing join information
   * @param left      the left holder
   * @param right     the right holder
   * @return a list of select elements
   */
  private static Twin<List<TypedField>> getSelectElements(CompiledTable joinTable, Holder left, Holder right) {
    // Try to guess from the conditions which field to keep.
    CompiledJoin join = joinTable.joins().get(0);
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

  private static String replaceTableNameByCteNameIfNotNull(Holder holder, String s) {
    if (holder.originalTableName != null) {
      s = s.replace(holder.queryRewriter.tableName(holder.originalTableName), holder.queryRewriter.cteName(holder.cteTableName));
    }
    return s;
  }
}
