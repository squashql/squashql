package io.squashql.query;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.*;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.TypedField;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                       int limit) {
    int queryLimit = limit < 0 ? LIMIT_DEFAULT_VALUE : limit;

    List<String> originalTableNames = new ArrayList<>();
    List<QueryResolver> queryResolvers = new ArrayList<>();
    List<QueryRewriter> queryRewriters = new ArrayList<>();
    List<DatabaseQuery> databaseQueries = new ArrayList<>();
    Function<QueryDto, String> f = query -> {
      QueryResolver queryResolver = new QueryResolver(query, new HashMap<>(this.queryEngine.datastore().storesByName()));
      queryResolvers.add(queryResolver);
      DatabaseQuery dbQuery = queryResolver.toDatabaseQuery(queryResolver.getScope(), -1);
      queryResolver.getMeasures().values().forEach(dbQuery::withMeasure);
      QueryRewriter qr = this.queryEngine.queryRewriter(dbQuery);
      queryRewriters.add(qr);
      databaseQueries.add(dbQuery);
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
        String orderByField = queryRewriters.get(index).aliasOrFullExpression(typedField).replace(originalTableNames.get(index), cteTableNames.get(index));
        orderList.add(orderByField + " nulls last");
      }
      sb.append(String.join(", ", orderList));
    }

    SQLTranslator.addLimit(queryLimit, sb);
    String sql = sb.toString();
    log.info("sql=" + sql);
    Table table = this.queryEngine.executeRawSql(sql);

    return selectAndOrderColumns(databaseQueries, table);
  }

  private static ColumnarTable selectAndOrderColumns(List<DatabaseQuery> databaseQueries, Table table) {
    // From the queries, reconstitute the correct header list. Columns first then measures. Columns from the left result
    // that have the same name as columns in right result will take precedence.
    List<Header> columnHeaders = new ArrayList<>();
    List<Header> measureHeaders = new ArrayList<>();
    Set<CompiledMeasure> measures = Stream.concat(databaseQueries.get(0).measures.stream(), databaseQueries.get(1).measures.stream()).collect(Collectors.toSet());
    Set<String> measureNames = measures.stream().map(CompiledMeasure::alias).collect(Collectors.toSet());
    MutableObjectIntMap<String> nameToIndex = new ObjectIntHashMap<>();
    int originalTableSize = table.headers().size();
    for (int i = 0; i < originalTableSize; i++) {
      Header h = table.headers().get(i);
      String headerName = h.name();
      if (!nameToIndex.containsKey(headerName)) {
        boolean isMeasure = measureNames.contains(headerName);
        (isMeasure ? measureHeaders : columnHeaders).add(new Header(headerName, h.type(), isMeasure));
        nameToIndex.put(headerName, i);
      }
    }

    List<Header> newHeaders = Stream.concat(columnHeaders.stream(), measureHeaders.stream()).toList();
    int[] mapping = new int[newHeaders.size()];
    for (int i = 0; i < columnHeaders.size(); i++) {
      mapping[i] = nameToIndex.getOrThrow(columnHeaders.get(i).name());
    }
    for (int i = 0; i < measureHeaders.size(); i++) {
      mapping[i + columnHeaders.size()] = nameToIndex.getOrThrow(measureHeaders.get(i).name());
    }

    // Reverse mapping
    int[] reverseMapping = new int[originalTableSize];
    Arrays.fill(reverseMapping, -1);
    for (int i = 0; i < mapping.length; i++) {
      reverseMapping[mapping[i]] = i;
    }

    List<List<Object>> values = new ArrayList<>(newHeaders.size());
    for (int i = 0; i < newHeaders.size(); i++) {
      values.add(new ArrayList<>());
    }
    table.forEach(row -> {
      for (int c = 0; c < row.size(); c++) {
        int index = reverseMapping[c];
        if (index >= 0) {
          values.get(index).add(row.get(c));
        }
      }
    });
    return new ColumnarTable(newHeaders, measures, values);
  }
}
