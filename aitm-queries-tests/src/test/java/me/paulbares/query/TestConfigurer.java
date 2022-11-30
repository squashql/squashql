package me.paulbares.query;

import me.paulbares.query.database.QueryEngine;
import me.paulbares.transaction.TransactionManager;

public interface TestConfigurer {

  QueryEngine createQueryEngine();

  TransactionManager createTransactionManager();

  void createTables(TransactionManager transactionManager);

  void loadData(TransactionManager transactionManager);
}
