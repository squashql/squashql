package me.paulbares.query;

import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQuery {

  protected TestConfigurer configurer;
  protected QueryExecutor executor;

//  public ATestQuery(TestConfigurer configurer) {
//    this.configurer = configurer;
//  }

  @BeforeAll
  void setup() {
    this.executor = new QueryExecutor(this.configurer.createQueryEngine());
    TransactionManager tm = this.configurer.createTransactionManager();
    this.configurer.createTables(tm, getFieldsByStore());
    loadData(tm);
  }

  /**
   * Defines the fields indexed by store name that will be used in the tests.
   */
  protected abstract Map<String, List<Field>> getFieldsByStore();

  /**
   * Loads data into the tables created above.
   */
  protected abstract void loadData(TransactionManager transactionManager);
}
