package me.paulbares.transaction;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import me.paulbares.ClickHouseStore;
import me.paulbares.store.Field;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.IntStream;

import static me.paulbares.ClickHouseStore.classToClickHouseType;

public class ClickHouseTransactionManager implements TransactionManager {

  protected final ClickHouseDataSource clickHouseDataSource;

  public ClickHouseTransactionManager(ClickHouseDataSource clickHouseDataSource) {
    this.clickHouseDataSource = clickHouseDataSource;
  }

  public ClickHouseStore createTable(String table, List<Field> fields) {
    try (ClickHouseConnection conn = this.clickHouseDataSource.getConnection();
         ClickHouseStatement stmt = conn.createStatement()) {
      stmt.execute("drop table if exists " + table);
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        sb.append(field.name()).append(' ').append(classToClickHouseType(field.type()));
        if (i < fields.size() - 1) {
          sb.append(", ");
        }
      }
      sb.append(")");
      stmt.execute("create table " + table + sb + "engine=Memory");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return new ClickHouseStore(table, fields);
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    String join = String.join(",", IntStream.range(0, tuples.get(0).length + 1).mapToObj(i -> "?").toList());
    String pattern = "insert into " + store + " values(" + join + ")";
    try (ClickHouseConnection conn = this.clickHouseDataSource.getConnection();
         PreparedStatement stmt = conn.prepareStatement(pattern)) {

      for (Object[] tuple : tuples) {
        for (int i = 0; i < tuple.length; i++) {
          stmt.setObject(i + 1, tuple[i]);
        }
        stmt.setObject(tuple.length + 1, scenario);
        stmt.addBatch();
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    // TODO see ClickHouseClient.load(...)
    throw new RuntimeException("not yet implemented");
  }
}
