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
  protected Map<String, List<Field>> fieldsByStore;

  @BeforeAll
  void setup() {
    this.executor = new QueryExecutor(this.configurer.createQueryEngine());
    TransactionManager tm = this.configurer.createTransactionManager();
    this.fieldsByStore = getFieldsByStore();

    createTables(tm);
    loadData(tm);
  }

  protected abstract Map<String, List<Field>> getFieldsByStore();

  protected abstract void createTables(TransactionManager transactionManager);

  protected abstract void loadData(TransactionManager transactionManager);
}
