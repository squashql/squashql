package io.squashql.transaction;

import io.squashql.SnowflakeDatastore;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.type.TableTypedField;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.IntStream;

public class SnowflakeDataLoader implements DataLoader {

  private final SnowflakeDatastore snowflakeDatastore;

  public SnowflakeDataLoader(SnowflakeDatastore snowflakeDatastore) {
    this.snowflakeDatastore = snowflakeDatastore;
  }

  public void dropTable(String table) {
    try (Statement statement = this.snowflakeDatastore.getConnection().createStatement()) {
      statement.execute("drop table \"" + table + "\";");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void createOrReplaceTable(String table, List<TableTypedField> fields) {
    createOrReplaceTable(this.snowflakeDatastore, table, fields);
  }

  public static void createOrReplaceTable(SnowflakeDatastore snowflakeDatastore, String table, List<TableTypedField> fields) {
    try (Connection conn = snowflakeDatastore.getConnection();
         Statement stmt = conn.createStatement()) {
      StringJoiner sb = new StringJoiner(", ", "(", ")");
      for (TableTypedField field : fields) {
        sb.add("\"" + field.name() + "\" " + JdbcUtil.classToSqlType(field.type()));
      }
      stmt.execute("create or replace table \"" + table + "\"" + sb + ";");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String table, List<Object[]> tuples) {
    String join = String.join(",", IntStream.range(0, tuples.get(0).length).mapToObj(i -> "?").toList());
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
        stmt.addBatch();
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void loadCsv(String table, String path, String delimiter, boolean header) {
    throw new RuntimeException("not implemented");
  }
}
