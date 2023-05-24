package io.squashql.transaction;

import io.squashql.SnowflakeDatastore;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.IntStream;

public class SnowflakeDataLoader implements DataLoader {

  private final SnowflakeDatastore snowflakeDatastore;

  public SnowflakeDataLoader(SnowflakeDatastore snowflakeDatastore) {
    this.snowflakeDatastore = snowflakeDatastore;
  }

  public void dropTable(String table) {
    try (Statement statement = snowflakeDatastore.getConnection().createStatement()) {
      statement.execute("drop table \"" + table + "\";");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void createOrReplaceTable(String table, List<Field> fields) {
    createOrReplaceTable(this.snowflakeDatastore, table, fields, true);
  }

  public static void createOrReplaceTable(SnowflakeDatastore snowflakeDatastore, String table, List<Field> fields,
                                          boolean cjMode) {
    List<Field> list = cjMode ? ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new Field(table, SCENARIO_FIELD_NAME, String.class))
            .castToList() : fields;

    try (Connection conn = snowflakeDatastore.getConnection();
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
      stmt.execute("create or replace table \"" + table + "\"" + sb + ";");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String scenario, String table, List<Object[]> tuples) {
    // Check the table contains a column scenario.
    ensureScenarioColumnIsPresent(table);
    String join = String.join(",", IntStream.range(0, tuples.get(0).length + 1).mapToObj(i -> "?").toList());
    String pattern = "insert into \"" + table + "\" values(" + join + ")";
    try (Connection conn = this.snowflakeDatastore.getConnection();
         PreparedStatement stmt = conn.prepareStatement(pattern)) {
      for (Object[] tuple : tuples) {
        for (int i = 0; i < tuple.length; i++) {
          Object o = tuple[i];
          if (o != null && (o.getClass().equals(LocalDate.class) || o.getClass().equals(LocalDateTime.class))) {
            o = o.toString();
          }
          stmt.setObject(i + 1, o);
        }
        stmt.setObject(tuple.length + 1, scenario);
        stmt.addBatch();
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void ensureScenarioColumnIsPresent(String store) {
    List<Field> fields = this.snowflakeDatastore.storesByName().get(store).fields();
    boolean found = fields.stream().anyMatch(f -> f.name().equals(SCENARIO_FIELD_NAME));
    if (!found) {
      throw new RuntimeException(String.format("%s field not found", SCENARIO_FIELD_NAME));
    }
  }

  @Override
  public void loadCsv(String scenario, String table, String path, String delimiter, boolean header) {
    // Not implemented
  }
}
