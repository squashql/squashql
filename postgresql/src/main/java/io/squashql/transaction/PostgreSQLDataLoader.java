package io.squashql.transaction;

import io.squashql.PostgreSQLDatastore;
import io.squashql.PostgreSQLUtil;
import io.squashql.type.TableTypedField;
import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.IntStream;

@AllArgsConstructor
public class PostgreSQLDataLoader implements DataLoader {

  protected final PostgreSQLDatastore datastore;

  public void dropAndCreateInMemoryTable(String table, List<TableTypedField> fields) {
    dropAndCreateInMemoryTable(this.datastore, table, fields);
  }

  public static void dropAndCreateInMemoryTable(PostgreSQLDatastore datastore,
                                                String table,
                                                List<TableTypedField> fields) {
    try (Connection conn = datastore.getConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("drop table if exists " + table);
      StringJoiner joiner = new StringJoiner(",", "(", ")");
      for (TableTypedField field : fields) {
        joiner.add("\"" + field.name() + "\" " + PostgreSQLUtil.classToPostgreType(field.type()));
      }
      stmt.execute("create table " + table + joiner);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String table, List<Object[]> tuples) {
    String join = String.join(",", IntStream.range(0, tuples.get(0).length).mapToObj(i -> "?").toList());
    String pattern = "insert into " + table + " values(" + join + ")";
    try (Connection conn = this.datastore.getConnection();
         PreparedStatement stmt = conn.prepareStatement(pattern)) {

      for (Object[] tuple : tuples) {
        for (int i = 0; i < tuple.length; i++) {
          stmt.setObject(i + 1, tuple[i]);
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

  public void dropTables(Collection<String> tables) {
    try (Connection conn = this.datastore.getConnection();
         Statement stmt = conn.createStatement()) {
      for (String table : tables) {
        stmt.execute("drop table if exists " + table);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
