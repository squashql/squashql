package me.paulbares.query;

import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;

import java.util.List;
import java.util.Map;

public interface TestConfigurer {

  void beforeSetup();

  QueryEngine createQueryEngine();

  TransactionManager createTransactionManager();

  void createTables(TransactionManager transactionManager, Map<String, List<Field>> fieldsByStore);
}
