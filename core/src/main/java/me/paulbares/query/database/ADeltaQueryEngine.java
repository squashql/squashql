package me.paulbares.query.database;

import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class ADeltaQueryEngine<T extends Datastore> extends AQueryEngine<T> {

  public ADeltaQueryEngine(T datastore) {
    super(datastore);
  }

  @Override
  protected Function<String, Field> createFieldSupplier() {
    Function<String, Field> sup = super.createFieldSupplier();
    // Since there is no scenario column, we have to modify the supplier.
    return f -> {
      if (f.equals(TransactionManager.SCENARIO_FIELD_NAME)) {
        return new Field(TransactionManager.SCENARIO_FIELD_NAME, String.class);
      } else {
        return sup.apply(f);
      }
    };
  }

  protected static List<String> getListOfScenarios(Datastore datastore, String baseStoreName) {
    return datastore.storesByName()
            .keySet()
            .stream()
            .map(s -> TransactionManager.extractScenarioFromStoreName(baseStoreName, s))
            .filter(Objects::nonNull)
            .toList();
  }

  abstract class TableTransformer implements BiFunction<QueryRewriter, String, String> {

    final List<String> keys;
    final Datastore datastore;

    TableTransformer(Datastore datastore, List<String> keys) {
      this.datastore = datastore;
      this.keys = keys;
    }

    @Override
    public String apply(QueryRewriter qr, String tableName) {
      List<String> listOfScenarios = getListOfScenarios(this.datastore, tableName);// does it have scenario?
      String newTableName = qr.tableName(tableName);
      if (listOfScenarios.isEmpty()) {
        return newTableName;
      } else {
        String virtualTable = virtualTableStatement(tableName, listOfScenarios, this.keys, qr);
        return "(" + virtualTable + ") as vt_" + newTableName;
      }
    }

    protected abstract String virtualTableStatement(String baseTableName, List<String> scenarios, List<String> columnKeys, QueryRewriter qr);
  }
}
