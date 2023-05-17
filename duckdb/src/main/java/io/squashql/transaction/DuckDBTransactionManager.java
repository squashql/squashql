package io.squashql.transaction;

import io.squashql.DuckDBDatastore;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class DuckDBTransactionManager implements TransactionManager {

  private final DuckDBDatastore datastore;

  public DuckDBTransactionManager(DuckDBDatastore datastore) {
    this.datastore = datastore;
  }

  public void createOrReplaceTable(String table, List<Field> fields) {
    createOrReplaceTable(this.datastore, table, fields, true);
  }

  public static void createOrReplaceTable(DuckDBDatastore datastore, String table, List<Field> fields,
                                          boolean cjMode) {
    List<Field> list = cjMode ? ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new Field(table, SCENARIO_FIELD_NAME, String.class))
            .castToList() : fields;

    try (Connection conn = datastore.getConnection();
         Statement stmt = conn.createStatement()) {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      int size = list.size();
      for (int i = 0; i < size; i++) {
        Field field = list.get(i);
        sb.append("\"").append(field.name()).append("\" ").append(JdbcUtil.classToSqlType(field.type()));
        if (i < size - 1) {
          sb.append(", ");
        }
      }
      sb.append(")");
      stmt.execute("create or replace table \"" + table + "\"" + sb + ";"); // REMOVE or replace
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    // Check the table contains a column scenario.
    ensureScenarioColumnIsPresent(store);
    String sql = "insert into \"" + store + "\" values ";
    try (Connection conn = this.datastore.getConnection();
         Statement stmt = conn.createStatement()) {
      for (Object[] tuple : tuples) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (int i = 0; i < tuple.length; i++) {
          Object o = tuple[i];
          if (o != null && (o.getClass().equals(LocalDate.class) || o.getClass().equals(LocalDateTime.class))) {
            o = o.toString();
          }

          if (o instanceof String) {
            sb.append('\'').append(o).append('\'');
          } else {
            sb.append(o);
          }
          sb.append(",");
        }
        sb.append('\'').append(scenario).append('\'').append("),");
        sql += sb.toString();
      }
      // addBatch is Not supported.
      stmt.execute(sql.substring(0, sql.length() - 1));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void ensureScenarioColumnIsPresent(String store) {
    List<Field> fields = this.datastore.storesByName().get(store).fields();
    boolean found = fields.stream().anyMatch(f -> f.name().equals(SCENARIO_FIELD_NAME));
    if (!found) {
      throw new RuntimeException(String.format("%s field not found", SCENARIO_FIELD_NAME));
    }
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    throw new RuntimeException("Not implemented");
  }
}
