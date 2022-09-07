package me.paulbares.query.database;

import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;

import java.util.List;
import java.util.Objects;
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

  protected List<String> getListOfScenarios(String baseStoreName) {
    return this.datastore.storesByName()
            .keySet()
            .stream()
            .map(s -> TransactionManager.extractScenarioFromStoreName(baseStoreName, s))
            .filter(Objects::nonNull)
            .toList();
  }
}
