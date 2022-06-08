package me.paulbares.query;

import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.List;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPeriodBucketing {

  protected Datastore datastore;

  protected QueryEngine queryEngine;

  protected TransactionManager tm;

  protected String storeName = "myAwesomeStore";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();
  protected abstract TransactionManager createTransactionManager();

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field sales = new Field("sales", double.class);
    Field qty = new Field("quantity", long.class);
    Field year = new Field("year_sales", int.class);
    Field month = new Field("month_sales", int.class);
    Field date = new Field("date_sales", LocalDate.class);

    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.tm = createTransactionManager();

    beforeLoading(List.of(ean, category, sales, qty, year, month, date));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 20d, 10, 2022, 1, LocalDate.of(2022, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 4, LocalDate.of(2022, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10, 2022, 8, LocalDate.of(2022, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 12, LocalDate.of(2022, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20, 2022, 2, LocalDate.of(2022, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10, 2022, 5, LocalDate.of(2022, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2022, 9, LocalDate.of(2022, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2022, 11, LocalDate.of(2022, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2, 2022, 3, LocalDate.of(2022, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4, 2022, 6, LocalDate.of(2022, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5, 2022, 7, LocalDate.of(2022, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1, 2022, 10, LocalDate.of(2022, 10, 1)}
    ));

//    this.tm.load("s1", this.storeName, List.of(
//            new Object[]{"bottle", "drink", 4d, 10},
//            new Object[]{"cookie", "food", 3d, 20},
//            new Object[]{"shirt", "cloth", 10d, 3}
//    ));
//
//    this.tm.load("s2", this.storeName, List.of(
//            new Object[]{"bottle", "drink", 1.5d, 10},
//            new Object[]{"cookie", "food", 3d, 20},
//            new Object[]{"shirt", "cloth", 10d, 3}
//    ));
  }

  protected void beforeLoading(List<Field> fields) {
  }

  @Test
  void test() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .wildcardCoordinate("date_sales")
            .aggregatedMeasure("sales", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table result = this.queryEngine.execute(query);
    result.show();
//    Assertions.assertThat(result).containsExactlyInAnyOrder(
//            List.of("base", 15.0d, 33l),
//            List.of("s1", 17.0d, 33l),
//            List.of("s2", 14.5d, 33l));
  }

}
