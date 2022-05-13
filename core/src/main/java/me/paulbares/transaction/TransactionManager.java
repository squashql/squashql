package me.paulbares.transaction;

import java.util.List;

public interface TransactionManager {

  void load(String scenario, String store, List<Object[]> tuples);

  void loadCsv(String scenario, String store, String path, String delimiter, boolean header);
}
