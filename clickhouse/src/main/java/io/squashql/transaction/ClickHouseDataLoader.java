package io.squashql.transaction;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.TableTypedField;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.IntStream;

import static io.squashql.ClickHouseUtil.classToClickHouseType;

public class ClickHouseDataLoader implements DataLoader {

  protected final ClickHouseDataSource clickHouseDataSource;

  public ClickHouseDataLoader(ClickHouseDataSource clickHouseDataSource) {
    this.clickHouseDataSource = clickHouseDataSource;
  }

  public void dropAndCreateInMemoryTable(String table, List<TableTypedField> fields) {
    dropAndCreateInMemoryTable(this.clickHouseDataSource, table, fields);
  }

  public static void dropAndCreateInMemoryTable(ClickHouseDataSource clickHouseDataSource,
                                                String table,
                                                List<TableTypedField> fields) {
    try (ClickHouseConnection conn = clickHouseDataSource.getConnection();
         ClickHouseStatement stmt = conn.createStatement()) {
      stmt.execute("drop table if exists " + table);
      StringJoiner joiner = new StringJoiner(",", "(", ")");
      for (TableTypedField field : fields) {
        String format = !List.class.isAssignableFrom(field.type()) ? "Nullable(%s)" : "%s";
        joiner.add(SqlUtils.backtickEscape(field.name()) + " " + String.format(format, classToClickHouseType(field.type())));
      }
      stmt.execute("create table " + table + joiner + "engine=Memory");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String table, List<Object[]> tuples) {
    String join = String.join(",", IntStream.range(0, tuples.get(0).length).mapToObj(i -> "?").toList());
    String pattern = "insert into " + table + " values(" + join + ")";
    try (ClickHouseConnection conn = this.clickHouseDataSource.getConnection();
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
    try (ClickHouseConnection conn = this.clickHouseDataSource.getConnection();
         ClickHouseStatement stmt = conn.createStatement()) {
      for (String table : tables) {
        stmt.execute("drop table if exists " + table);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
