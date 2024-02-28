package io.squashql;

import io.squashql.query.QueryExecutor;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.transaction.DuckDBDataLoader;
import io.squashql.type.TableTypedField;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TestOrderByFromOrderTable {
  protected String storeName = "myStore";
  protected String orderTable = "orderTable";
  protected DuckDBDatastore datastore;
  protected DuckDBQueryEngine queryEngine;
  protected DuckDBDataLoader dl;
  protected QueryExecutor executor;

  void setup(Map<String, List<TableTypedField>> fieldsByStore, Runnable dataLoading) {
    this.datastore = new DuckDBDatastore();
    this.dl = new DuckDBDataLoader(this.datastore);
    fieldsByStore.forEach(this.dl::createOrReplaceTable);
    this.queryEngine = new DuckDBQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    dataLoading.run();
  }

  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField portfolio = new TableTypedField(this.storeName, "portfolio", String.class);
    TableTypedField ticker = new TableTypedField(this.storeName, "ticker", String.class);

    TableTypedField orderId = new TableTypedField(this.orderTable, "orderId", int.class);
    TableTypedField portfolioOrder = new TableTypedField(this.orderTable, "portfolio", String.class);
    TableTypedField tickerOrder = new TableTypedField(this.orderTable, "ticker", String.class);
    return Map.of(this.storeName, List.of(portfolio, ticker), this.orderTable, List.of(orderId, portfolioOrder, tickerOrder));
  }

  protected void loadData() {
    // Shuffle
    List<Object[]> tuples = Arrays.asList(
            new Object[]{"A", "AAPL"},
            new Object[]{"A", "NVDA"},
            new Object[]{"A", "TSLA"},
            new Object[]{"B", "MSFT"},
            new Object[]{"B", "AAPL"},
            new Object[]{"B", "META"}
    );
    Collections.shuffle(tuples, new Random(1234));
    this.dl.load(this.storeName, tuples);

    // This table store the order in which we want the ticker to appear under each portfolio
    tuples = Arrays.asList(
            new Object[]{0, "A", "AAPL"},
            new Object[]{1, "A", "NVDA"},
            new Object[]{2, "A", "TSLA"},
            new Object[]{0, "B", "MSFT"},
            new Object[]{1, "B", "AAPL"},
            new Object[]{2, "B", "META"}
    );
    Collections.shuffle(tuples, new Random(1234));
    this.dl.load(this.orderTable, tuples);
  }

  @Test
  void testOrderByFromOrderTable() {
    setup(getFieldsByStore(), this::loadData);

    this.executor.executeRaw("select * from "  + this.storeName).show();
    this.executor.executeRaw("select * from "  + this.orderTable).show();

    String order = "select myStore.portfolio, myStore.ticker from " + this.storeName + " inner join " + this.orderTable + " on myStore.portfolio=orderTable.portfolio and myStore.ticker=orderTable.ticker" +
            " order by" +
            " myStore.portfolio, orderTable.orderId";
    System.out.println(order);
    this.executor.executeRaw(order).show();
  }
}
