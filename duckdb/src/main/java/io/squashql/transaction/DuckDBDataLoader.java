package io.squashql.transaction;

import io.squashql.DuckDBDatastore;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DuckDBDataLoader implements DataLoader {

  private final DuckDBDatastore datastore;

  public DuckDBDataLoader(DuckDBDatastore datastore) {
    this.datastore = datastore;
  }

  public void createOrReplaceTable(String tableName, Table table) {
    List<TableTypedField> fields = table.headers().stream().map(h -> new TableTypedField(tableName, h.field(), h.type())).toList();
    createOrReplaceTable(this.datastore, tableName, fields, false);
    loadWithOrWithoutScenario(null, tableName, table.iterator());
  }

  public void createOrReplaceTable(String tableName, List<TableTypedField> fields) {
    createOrReplaceTable(tableName, fields, true);
  }

  public void createOrReplaceTable(String tableName, List<TableTypedField> fields, boolean cjMode) {
    createOrReplaceTable(this.datastore, tableName, fields, cjMode);
  }

  public static void createOrReplaceTable(DuckDBDatastore datastore, String tableName, List<TableTypedField> fields,
                                          boolean cjMode) {
    List<TableTypedField> list = cjMode ? ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new TableTypedField(tableName, SCENARIO_FIELD_NAME, String.class))
            .castToList() : fields;

    try (Connection conn = datastore.getConnection();
         Statement stmt = conn.createStatement()) {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      int size = list.size();
      for (int i = 0; i < size; i++) {
        TableTypedField field = list.get(i);
        sb.append("\"").append(field.name()).append("\" ").append(JdbcUtil.classToSqlType(field.type()));
        if (i < size - 1) {
          sb.append(", ");
        }
      }
      sb.append(")");
      stmt.execute("create or replace table \"" + tableName + "\"" + sb + ";"); // REMOVE or replace
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String scenario, String table, List<Object[]> tuples) {
    loadWithOrWithoutScenario(scenario, table, tuples.stream().map(t -> Arrays.asList(t)).toList().iterator());
  }

  private void loadWithOrWithoutScenario(String scenario, String table, Iterator<List<Object>> tuplesIterator) {
    // Check the table contains a column scenario.
    if (scenario != null && !scenario.equals(MAIN_SCENARIO_NAME)) {
      ensureScenarioColumnIsPresent(table);
    }

    boolean addScenario = scenarioColumnIsPresent(table);
    String sql = "insert into \"" + table + "\" values ";
    try (Connection conn = this.datastore.getConnection();
         Statement stmt = conn.createStatement()) {
      while (tuplesIterator.hasNext()) {
        List<Object> tuple = tuplesIterator.next();
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (int i = 0; i < tuple.size(); i++) {
          Object o = tuple.get(i);
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

        if (addScenario) {
          sb.append('\'').append(scenario).append('\'').append("),");
          sql += sb.toString();
        } else {
          String s = sb.toString();
          sql += s.substring(0, s.length() - 1) + "),";
        }
      }
      // addBatch is Not supported.
      stmt.execute(sql.substring(0, sql.length() - 1));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void ensureScenarioColumnIsPresent(String store) {
    List<TableTypedField> fields = this.datastore.storesByName().get(store).fields();
    boolean found = fields.stream().anyMatch(f -> f.name().equals(SCENARIO_FIELD_NAME));
    if (!found) {
      throw new RuntimeException(String.format("%s field not found", SCENARIO_FIELD_NAME));
    }
  }

  private boolean scenarioColumnIsPresent(String store) {
    List<TableTypedField> fields = this.datastore.storesByName().get(store).fields();
    return fields.stream().anyMatch(f -> f.name().equals(SCENARIO_FIELD_NAME));
  }

  @Override
  public void loadCsv(String scenario, String table, String path, String delimiter, boolean header) {
    throw new RuntimeException("Not implemented");
  }
}
