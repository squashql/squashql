package io.squashql.transaction;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import io.squashql.ClickHouseDatastore;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.TableField;
import io.squashql.type.TypedField;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static io.squashql.ClickHouseUtil.classToClickHouseType;

public class ClickHouseDataLoader implements DataLoader {

  protected final ClickHouseDataSource clickHouseDataSource;

  public ClickHouseDataLoader(ClickHouseDataSource clickHouseDataSource) {
    this.clickHouseDataSource = clickHouseDataSource;
  }

  public void dropAndCreateInMemoryTable(String table, List<TypedField> fields) {
    dropAndCreateInMemoryTable(this.clickHouseDataSource, table, fields, true);
  }

  public void dropAndCreateInMemoryTableWithoutScenarioColumn(String table, List<TypedField> fields) {
    dropAndCreateInMemoryTable(this.clickHouseDataSource, table, fields, false);
  }

  public static void dropAndCreateInMemoryTable(ClickHouseDataSource clickHouseDataSource,
                                                String table,
                                                List<TypedField> fields,
                                                boolean cjMode) {
    List<TypedField> list = cjMode ? ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new TableField(table, SCENARIO_FIELD_NAME, String.class))
            .castToList() : fields;

    try (ClickHouseConnection conn = clickHouseDataSource.getConnection();
         ClickHouseStatement stmt = conn.createStatement()) {
      stmt.execute("drop table if exists " + table);
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      int size = list.size();
      for (int i = 0; i < size; i++) {
        TypedField field = list.get(i);
        sb.append(SqlUtils.backtickEscape(field.fieldName())).append(" Nullable(").append(classToClickHouseType(field.type())).append(')');
        if (i < size - 1) {
          sb.append(", ");
        }
      }
      sb.append(")");
      stmt.execute("create table " + table + sb + "engine=Memory");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String scenario, String table, List<Object[]> tuples) {
    // Check the table contains a column scenario.
    ensureScenarioColumnIsPresent(table);
    String join = String.join(",", IntStream.range(0, tuples.get(0).length + 1).mapToObj(i -> "?").toList());
    String pattern = "insert into " + table + " values(" + join + ")";
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

  private void ensureScenarioColumnIsPresent(String store) {
    List<TypedField> fields = ClickHouseDatastore.getFields(this.clickHouseDataSource, store);
    boolean found = fields.stream().anyMatch(f -> f.fieldName().equals(SCENARIO_FIELD_NAME));
    if (!found) {
      throw new RuntimeException(String.format("%s field not found", SCENARIO_FIELD_NAME));
    }
  }

  @Override
  public void loadCsv(String scenario, String table, String path, String delimiter, boolean header) {
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
