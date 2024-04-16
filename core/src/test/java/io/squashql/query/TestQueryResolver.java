package io.squashql.query;

import io.squashql.query.builder.Query;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Store;
import io.squashql.type.TableTypedField;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestQueryResolver {

  private DatabaseQuery compileQuery(QueryDto query, Map<String, Store> stores) {
    return new QueryResolver(query, stores) {
      DatabaseQuery toDatabaseQuery() {
        return new DatabaseQuery(getScope().copyWithNewLimit(query.limit), new ArrayList<>(getMeasures().values()));
      }
    }.toDatabaseQuery();
  }

  private Map<String, Store> stores(Map<String, List<Pair<String, Class<?>>>> fieldsByStore) {
    Map<String, Store> stores = new HashMap<>();
    for (Map.Entry<String, List<Pair<String, Class<?>>>> entry : fieldsByStore.entrySet()) {
      stores.put(entry.getKey(), new Store(entry.getKey(), entry.getValue().stream().map(s -> new TableTypedField(entry.getKey(), s.getOne(), s.getTwo())).toList()));
    }
    return stores;
  }

  @Test
  void test() {
    Map<String, Store> stores = stores(Map.of("A", List.of(Tuples.pair("a", int.class), Tuples.pair("b", String.class))));

    QueryDto sq = Query.from("A")
            .select(List.of(new TableField("A.a").as("alias_a")), List.of())
            .build();

    QueryDto q = Query.from(sq)
            .select(List.of(new AliasedField("alias_a")), List.of())
            .build();

    QueryResolver queryResolver = new QueryResolver(q, stores);
    System.out.println(queryResolver.getColumns().get(0).type());
  }
}
