package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Datastore;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.squashql.query.Functions.*;

@TestClass(ignore = {TestClass.Type.BIGQUERY, TestClass.Type.SNOWFLAKE, TestClass.Type.CLICKHOUSE, TestClass.Type.SPARK})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryExecutionLocality extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField qty = new TableTypedField(this.storeName, "quantity", int.class);

    return Map.of(this.storeName, List.of(ean, category, price, qty));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  @Test
  void testBinaryMeasureExecutionIsPushedToDB() {
    QueryEngineInterceptor interceptor = new QueryEngineInterceptor(this.queryEngine);
    this.executor = new QueryExecutor(interceptor);
    Measure divide = divide("d", multiply("m", sum("ps", "price"), sum("qs", "quantity")), Functions.integer(2));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("ean"), List.of(divide))
            .build();
    Table table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("bottle", 10d),
            List.of("cookie", 30d),
            List.of("shirt", 15d));
    Assertions.assertThat(interceptor.lastExecutedDatabaseQuery.measures).contains(divide);
  }

  private static class QueryEngineInterceptor<T extends Datastore> implements QueryEngine<T> {

    private final QueryEngine<T> underlying;

    protected DatabaseQuery lastExecutedDatabaseQuery;

    private QueryEngineInterceptor(QueryEngine<T> underlying) {
      this.underlying = underlying;
    }

    @Override
    public Table execute(DatabaseQuery query, QueryExecutor.PivotTableContext context) {
      this.lastExecutedDatabaseQuery = query;
      return this.underlying.execute(query, context);
    }

    @Override
    public Table executeRawSql(String sql) {
      return this.underlying.executeRawSql(sql);
    }

    @Override
    public T datastore() {
      return this.underlying.datastore();
    }

    @Override
    public Function<String, TypedField> getFieldSupplier() {
      return this.underlying.getFieldSupplier();
    }

    @Override
    public List<String> supportedAggregationFunctions() {
      return this.underlying.supportedAggregationFunctions();
    }

    @Override
    public QueryRewriter queryRewriter() {
      return this.underlying.queryRewriter();
    }
  }
}
