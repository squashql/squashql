package io.squashql.query;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.*;
import io.squashql.table.Table;
import io.squashql.type.TypedField;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.squashql.query.QueryExecutor.LIMIT_DEFAULT_VALUE;

@Slf4j
public class ExperimentalQueryMergeExecutor {

  private final QueryEngine<?> queryEngine;

  public ExperimentalQueryMergeExecutor(QueryEngine<?> queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table execute(QueryDto first,
                       QueryDto second,
                       JoinType joinType,
                       CriteriaDto joinCondition,
                       Map<Field, OrderDto> orders,
                       int limit,
                       SquashQLUser user) {
    int queryLimit = limit < 0 ? LIMIT_DEFAULT_VALUE : limit;

    // TODO check that limit and order are not set. They will be ignored.
    List<String> originalTableNames = new ArrayList<>();
    List<QueryResolver> queryResolvers = new ArrayList<>();
    List<QueryRewriter> queryRewriters = new ArrayList<>();
    Function<QueryDto, String> f = query -> {
      QueryResolver queryResolver = new QueryResolver(query, new HashMap<>(this.queryEngine.datastore().storesByName()));
      queryResolvers.add(queryResolver);
      DatabaseQuery dbQuery = queryResolver.toDatabaseQuery(queryResolver.getScope(), -1);
      queryResolver.getMeasures().values().forEach(dbQuery::withMeasure);
      QueryRewriter qr = this.queryEngine.queryRewriter(dbQuery);
      queryRewriters.add(qr);
      originalTableNames.add(qr.tableName(query.table.name));
      return SQLTranslator.translate(dbQuery, qr);
    };
    String firstSql = f.apply(first);
    String secondSql = f.apply(second);

    List<String> cteTableNames = List.of("__cteL__", "__cteR__");
    StringBuilder sb = new StringBuilder("with ");
    sb.append(cteTableNames.get(0)).append(" as (").append(firstSql).append("), ");
    sb.append(cteTableNames.get(1)).append(" as (").append(secondSql).append(") ");

    QueryDto firstCopy = JacksonUtil.deserialize(JacksonUtil.serialize(first), QueryDto.class);
    firstCopy.table.join(new TableDto(second.table.name), joinType, joinCondition);
    QueryResolver queryResolver = new QueryResolver(firstCopy, new HashMap<>(this.queryEngine.datastore().storesByName()));
    String tableExpression = queryResolver.getScope().table().sqlExpression(this.queryEngine.queryRewriter(null));
    tableExpression = tableExpression.replace(originalTableNames.get(0), cteTableNames.get(0));
    tableExpression = tableExpression.replace(originalTableNames.get(1), cteTableNames.get(1));

    sb.append("select * from ").append(tableExpression);

    if (!orders.isEmpty()) {
      sb.append(" order by ");
      List<String> orderList = new ArrayList<>();
      for (Map.Entry<Field, OrderDto> e : orders.entrySet()) {
        Field key = e.getKey();
        int index = 0;
        // Where does it come from ? Left or right?
        TypedField typedField = queryResolvers.get(index).getTypedFieldOrNull(key);
        if (typedField == null) {
          typedField = queryResolvers.get(index = 1).getTypedFieldOrNull(key);
        }
        if (typedField == null) {
          throw new RuntimeException("Cannot resolve " + e.getKey());
        }
        orderList.add(queryRewriters.get(index).select(typedField).replace(originalTableNames.get(index), cteTableNames.get(index)));
      }
      sb.append(String.join(", ", orderList));
    }

    SQLTranslator.addLimit(queryLimit, sb);
    String sql = sb.toString();
    log.info("sql=" + sql);
    return this.queryEngine.executeRawSql(sql);
  }
}
