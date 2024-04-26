package io.squashql.transaction;

import io.squashql.DuckDBDatastore;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;

import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.IntStream;

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
      stmt.execute("create or replace table \"" + tableName + "\"" + sb + ";");
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
          Object o = tuple[i];
          if (o instanceof LocalDate ld) {
            o = new Date(ld.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
          } else if (o != null && o.getClass().isArray()) {
            o = Arrays.toString((Object[]) o);
          }
          stmt.setObject(i + 1, o);
        }
        stmt.addBatch();
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
//    StringJoiner sql = new StringJoiner(",", "insert into \"" + table + "\" values ", "");
//    try (Connection conn = this.datastore.getConnection();
//         Statement stmt = conn.createStatement()) {
//      for (Object[] tuple : tuples) {
//        StringJoiner sb = new StringJoiner(",", "(", ")");
//        for (Object o : tuple) {
//          if (o != null && (o.getClass().equals(LocalDate.class) || o.getClass().equals(LocalDateTime.class))) {
//            o = o.toString();
//          }
//
//          if (o instanceof String) {
//            sb.add('\'' + SqlUtils.escapeSingleQuote((String) o, "''") + '\'');
//          } else {
//            sb.add(String.valueOf(o));
//          }
//        }
//        sql.add(sb.toString());
//      }
//      // addBatch is Not supported.
//      stmt.execute(sql.toString());
//    } catch (SQLException e) {
//      throw new RuntimeException(e);
//    }
  }

  @Override
  public void loadCsv(String table, String path, String delimiter, boolean header) {
    throw new RuntimeException("Not implemented");
  }
}
