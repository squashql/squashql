package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.OrderKeywordDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.*;

import static io.squashql.query.Functions.all;
import static io.squashql.query.Functions.criterion;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestOrderByFromOrderTable extends ABaseTestQuery {

  protected final String storeName = "store" + getClass().getSimpleName().toLowerCase();
  private final String orderTable = "orderTable";
  private final TableField portfolio = new TableField(this.storeName, "portfolio");
  private final TableField ticker = new TableField(this.storeName, "ticker");
  private final TableField orderId = new TableField(this.orderTable, "orderId");
  private final TableField portfolioOrder = new TableField(this.orderTable, "portfolio");
  private final TableField tickerOrder = new TableField(this.orderTable, "ticker");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField portfolio = new TableTypedField(this.storeName, "portfolio", String.class);
    TableTypedField ticker = new TableTypedField(this.storeName, "ticker", String.class);
    TableTypedField year = new TableTypedField(this.storeName, "year", int.class);

    TableTypedField orderId = new TableTypedField(this.orderTable, "orderId", int.class);
    TableTypedField portfolioOrder = new TableTypedField(this.orderTable, "portfolio", String.class);
    TableTypedField tickerOrder = new TableTypedField(this.orderTable, "ticker", String.class);
    return Map.of(this.storeName, List.of(portfolio, ticker, year), this.orderTable, List.of(orderId, portfolioOrder, tickerOrder));
  }

  @Override
  protected void loadData() {
    // Shuffle
    List<Object[]> tuples = Arrays.asList(
            new Object[]{"A", "AAPL", 2024},
            new Object[]{"A", "AAPL", 2023},
            new Object[]{"A", "NVDA", 2024},
            new Object[]{"A", "NVDA", 2023},
            new Object[]{"A", "TSLA", 2024},
            new Object[]{"A", "TSLA", 2023},
            new Object[]{"B", "MSFT", 2024},
            new Object[]{"B", "MSFT", 2023},
            new Object[]{"B", "AAPL", 2024},
            new Object[]{"B", "AAPL", 2023},
            new Object[]{"B", "META", 2024},
            new Object[]{"B", "META", 2023}
    );
    Collections.shuffle(tuples, new Random(1234));
    this.tm.load(this.storeName, tuples);

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
    this.tm.load(this.orderTable, tuples);
  }

  @Test
  void testOrderByFromOrderTable() {
    QueryDto query = Query.from(this.storeName)
            .join(this.orderTable, JoinType.INNER)
            .on(all(
                    criterion(this.portfolio, this.portfolioOrder, ConditionType.EQ),
                    criterion(this.ticker, this.tickerOrder, ConditionType.EQ)))
            .select(List.of(this.portfolio, this.ticker), List.of())
            .orderBy(this.portfolio, OrderKeywordDto.ASC)
            .orderBy(this.orderId, OrderKeywordDto.ASC)
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of("A", "AAPL"),
            List.of("A", "NVDA"),
            List.of("A", "TSLA"),
            List.of("B", "MSFT"),
            List.of("B", "AAPL"),
            List.of("B", "META"));
  }
}
