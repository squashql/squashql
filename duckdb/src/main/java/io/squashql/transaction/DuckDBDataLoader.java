package io.squashql.transaction;

import io.squashql.DuckDBDatastore;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.query.database.SqlUtils;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class DuckDBDataLoader implements DataLoader {

  private final DuckDBDatastore datastore;

  public DuckDBDataLoader(DuckDBDatastore datastore) {
    this.datastore = datastore;
  }

  public void createOrReplaceTable(String tableName, Table table) {
    List<TableTypedField> fields = table.headers().stream().map(h -> new TableTypedField(tableName, h.name(), h.type())).toList();
    createOrReplaceTable(this.datastore, tableName, fields);
    List<Object[]> tuples = new ArrayList<>();
    for (List<Object> row : table) {
      tuples.add(row.toArray());
    }
    load(tableName, tuples);
  }

  public void createOrReplaceTable(String tableName, List<TableTypedField> fields) {
    createOrReplaceTable(this.datastore, tableName, fields);
  }

  public static void createOrReplaceTable(DuckDBDatastore datastore, String tableName, List<TableTypedField> fields) {
    try (Connection conn = datastore.getConnection();
         Statement stmt = conn.createStatement()) {
      StringJoiner sb = new StringJoiner(", ", "(", ")");
      for (TableTypedField field : fields) {
        sb.add("\"" + field.name() + "\" " + JdbcUtil.classToSqlType(field.type()));
      }
      stmt.execute("create or replace table \"" + tableName + "\"" + sb + ";"); // REMOVE or replace
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String table, List<Object[]> tuples) {
    StringJoiner sql = new StringJoiner(",", "insert into \"" + table + "\" values ", "");
    try (Connection conn = this.datastore.getConnection();
         Statement stmt = conn.createStatement()) {
      for (Object[] tuple : tuples) {
        StringJoiner sb = new StringJoiner(",", "(", ")");
        for (Object o : tuple) {
          if (o != null && (o.getClass().equals(LocalDate.class) || o.getClass().equals(LocalDateTime.class))) {
            o = o.toString();
          }

          if (o instanceof String) {
            sb.add('\'' + SqlUtils.escapeSingleQuote((String) o, "''") + '\'');
          } else {
            sb.add(String.valueOf(o));
          }
        }
        sql.add(sb.toString());
      }
      // addBatch is Not supported.
      stmt.execute(sql.toString());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void loadCsv(String table, String path, String delimiter, boolean header) {
    throw new RuntimeException("Not implemented");
  }
}
